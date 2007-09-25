/**
 * Copyright (c) 2000-2006, Identyx Corporation.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.safehaus.penrose.directory;

import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.mapping.SourceMapping;
import org.safehaus.penrose.mapping.Link;
import org.safehaus.penrose.source.Source;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.Partitions;
import org.safehaus.penrose.partition.PartitionContext;
import org.safehaus.penrose.acl.ACI;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.handler.Handler;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.engine.Engine;
import org.safehaus.penrose.engine.EngineTool;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.interpreter.InterpreterManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class Entry implements Cloneable {

    public Logger log = LoggerFactory.getLogger(getClass());
    public boolean debug = log.isDebugEnabled();

    public final static Collection<Entry> EMPTY_ENTRIES        = new ArrayList<Entry>();

    public final static String  FETCH         = "fetch";
    public final static boolean DEFAULT_FETCH = false;

    protected EntryMapping entryMapping;
    protected EntryContext entryContext;

    protected Map<String,SourceRef> localSourceRefs = new LinkedHashMap<String,SourceRef>();
    protected Map<String,SourceRef> localPrimarySourceRefs = new LinkedHashMap<String,SourceRef>();

    protected Map<String,SourceRef> sourceRefs = new LinkedHashMap<String,SourceRef>();
    protected Map<String,SourceRef> primarySourceRefs = new LinkedHashMap<String,SourceRef>();

    protected Entry parent;

    protected Map<String,Entry> children = new LinkedHashMap<String,Entry>();
    protected Map<String,Collection<Entry>> childrenByRdn = new LinkedHashMap<String,Collection<Entry>>();

    Partition partition;

    protected boolean fetch = DEFAULT_FETCH;

    public Entry() {
    }

    public void init(EntryMapping entryMapping, EntryContext entryContext) throws Exception {
        this.entryMapping = entryMapping;
        this.entryContext = entryContext;

        Directory directory = entryContext.getDirectory();
        DirectoryContext directoryContext = directory.getDirectoryContext();
        partition = directoryContext.getPartition();

        // create source references
        
        String primarySourceName = entryMapping.getPrimarySourceName();

        for (SourceMapping sourceMapping : entryMapping.getSourceMappings()) {

            SourceRef sourceRef = createSourceRef(sourceMapping);
            String alias = sourceRef.getAlias();

            localSourceRefs.put(alias, sourceRef);
            sourceRefs.put(alias, sourceRef);

            if (alias.equals(primarySourceName)) {
                localPrimarySourceRefs.put(alias, sourceRef);
                primarySourceRefs.put(alias, sourceRef);
            }
        }

        // inherit source referencess from the parent entries

        Entry parent = directory.getEntry(entryMapping.getParentId());

        while (parent != null) {

            String psn = parent.getPrimarySourceName();

            for (SourceRef sourceRef : parent.getLocalSourceRefs()) {
                String alias = sourceRef.getAlias();

                sourceRefs.put(alias, sourceRef);

                if (alias.equals(psn)) {
                    primarySourceRefs.put(alias, sourceRef);
                }
            }

            parent = parent.getParent();
        }

        init();
    }

    public void init() throws Exception {
    }

    public SourceRef createSourceRef(SourceMapping sourceMapping) throws Exception {

        log.debug("Initializing source reference "+sourceMapping.getName()+".");

        Partition partition = getPartition();

        Source source = partition.getSource(sourceMapping.getSourceName());
        if (source == null) throw new Exception("Unknown source "+sourceMapping.getSourceName()+".");

        return new SourceRef(this, source, sourceMapping);
    }

    public String getId() {
        return entryMapping.getId();
    }

    public String getParentId() {
        return entryMapping.getParentId();
    }
    
    public DN getDn() {
        return entryMapping.getDn();
    }

    public DN getParentDn() {
        return entryMapping.getParentDn();
    }

    public EntryMapping getEntryMapping() {
        return entryMapping;
    }

    public String getHandlerName() {
        return entryMapping.getHandlerName();
    }

    public EntryContext getEntryContext() {
        return entryContext;
    }

    public void setEntryContext(EntryContext entryContext) {
        this.entryContext = entryContext;
    }

    public Directory getDirectory() {
        return entryContext.getDirectory();
    }

    public Partition getPartition() {
        return getDirectory().getPartition();
    }

    public Collection<SourceRef> getLocalSourceRefs() {
        return localSourceRefs.values();
    }

    public Collection<SourceRef> getLocalPrimarySourceRefs() {
        return localPrimarySourceRefs.values();
    }

    public Collection<SourceRef> getSourceRefs() {
        return sourceRefs.values();
    }

    public SourceRef getSourceRef(String name) {
        return sourceRefs.get(name);
    }

    public void setSourceRefs(Map<String,SourceRef> sourceRefs) {
        this.sourceRefs = sourceRefs;
    }

    public Collection<SourceRef> getPrimarySourceRefs() {
        return primarySourceRefs.values();
    }

    public void setPrimarySourceRefs(Map<String, SourceRef> primarySourceRefs) {
        this.primarySourceRefs = primarySourceRefs;
    }

    public Collection<Entry> getChildren() {
        return children.values();
    }

    public Collection<Entry> getChildren(RDN rdn) {
        if (rdn == null) return EMPTY_ENTRIES;

        Collection<Entry> list = childrenByRdn.get(rdn.getNormalized());
        if (list == null) return EMPTY_ENTRIES;

        return new ArrayList<Entry>(list);
    }

    public void addChild(Entry child) {

        String rdn = child.getDn().getRdn().getNormalized();

        children.put(child.getId(), child);
        child.setParent(this);

        Collection<Entry> c = childrenByRdn.get(rdn);
        if (c == null) {
            c = new ArrayList<Entry>();
            childrenByRdn.put(rdn, c);
        }
        c.add(child);
    }

    public void setChildren(Collection<Entry> children) {
        if (this.children == children) return;
        this.children.clear();

        for (Entry child : children) {
            addChild(child);
        }
    }

    public void clearChildren() {
        children.clear();
        childrenByRdn.clear();
    }
    
    public Entry getParent() {
        return parent;
    }

    public void setParent(Entry parent) {
        this.parent = parent;
    }

    public String getPrimarySourceName() {
        return entryMapping.getPrimarySourceName();
    }

    public Collection<String> getObjectClasses() {
        return entryMapping.getObjectClasses();
    }

    public String getParameter(String name) {
        return entryMapping.getParameter(name);
    }

    public Collection<String> getParameterNames() {
        return entryMapping.getParameterNames();
    }

    public Link getLink() {
        return entryMapping.getLink();
    }

    public List<Entry> getPath() {
        List<Entry> path = new ArrayList<Entry>();

        Entry entry = this;
        do {
            path.add(0, entry);
            entry = entry.getParent();
        } while (entry != null);

        return path;
    }

    public List<Entry> getRelativePath(Entry base) {
        List<Entry> path = new ArrayList<Entry>();

        Entry entry = this;
        do {
            path.add(0, entry);
            if (entry == base) break;
            entry = entry.getParent();
        } while (entry != null);

        return path;
    }

    public boolean containsObjectClass(String objectClass) {
        return entryMapping.containsObjectClass(objectClass);
    }

    public Collection<AttributeMapping> getAttributeMappings() {
        return entryMapping.getAttributeMappings();
    }

    public AttributeMapping getAttributeMapping(String attributeName) {
        return entryMapping.getAttributeMapping(attributeName);
    }

    public Collection<AttributeMapping> getRdnAttributeMappings() {
        return entryMapping.getRdnAttributeMappings();
    }
    
    public Collection<ACI> getACL() {
        return entryMapping.getACL();
    }

    public Collection<SourceMapping> getSourceMappings() {
        return entryMapping.getSourceMappings();
    }

    public SourceMapping getSourceMapping(int index) {
        return entryMapping.getSourceMapping(index);
    }
    
    public SourceMapping getSourceMapping(String alias) {
        return entryMapping.getSourceMapping(alias);
    }

    public Collection<SourceMapping> getEffectiveSourceMappings() {
         Collection<SourceMapping> list = new ArrayList<SourceMapping>();
         list.addAll(entryMapping.getSourceMappings());

         if (parent != null) list.addAll(parent.getEffectiveSourceMappings());

         return list;
     }

    public boolean isDynamic() {

        boolean dynamic = entryMapping.isDynamic();

        //log.debug("Mapping "+entryMapping.getDn()+" is "+(dynamic ? "dynamic" : "not dynamic"));
        return dynamic || parent != null && parent.isDynamic();

    }

    public String getEngineName() {
        return entryMapping.getEngineName();
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Add
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void add(
            Session session,
            AddRequest request,
            AddResponse response
    ) throws Exception {

        PartitionContext partitionContext = partition.getPartitionContext();
        PenroseContext penroseContext = partitionContext.getPenroseContext();

        Partitions partitions = penroseContext.getPartitions();
        Handler handler = partitions.getHandler(partition, this);

        DN dn = request.getDn();

        SourceValues sourceValues = new SourceValues();

        boolean fetchEntry = fetch;

        String s = getParameter(FETCH);
        if (s != null) fetchEntry = Boolean.valueOf(s);

        if (fetchEntry) {
            DN parentDn = parent.getDn();

            SearchResult sr = parent.find(session, parentDn);
            sourceValues.add(sr.getSourceValues());

        } else {
            EngineTool.extractSourceValues(this, dn, sourceValues);
        }

        EngineTool.propagateDown(this, sourceValues);

        if (debug) {
            log.debug("Source values:");
            sourceValues.print();
        }

        Engine engine = partitions.getEngine(partition, handler, this);
        engine.add(
                session,
                this,
                sourceValues,
                request,
                response
        );
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Bind
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void bind(
            Session session,
            BindRequest request,
            BindResponse response
    ) throws Exception {

        PartitionContext partitionContext = partition.getPartitionContext();
        PenroseContext penroseContext = partitionContext.getPenroseContext();

        Partitions partitions = penroseContext.getPartitions();
        Handler handler = partitions.getHandler(partition, this);

        DN dn = request.getDn();

        SourceValues sourceValues = new SourceValues();

        boolean fetchEntry = fetch;

        String s = getParameter(FETCH);
        if (s != null) fetchEntry = Boolean.valueOf(s);

        if (fetchEntry) {
            SearchResult sr = find(null, dn);
            sourceValues.add(sr.getSourceValues());
        } else {
            EngineTool.extractSourceValues(this, dn, sourceValues);
        }

        EngineTool.propagateDown(this, sourceValues);

        if (debug) {
            log.debug("Source values:");
            sourceValues.print();
        }

        Engine engine = partitions.getEngine(partition, handler, this);
        engine.bind(
                session,
                this,
                sourceValues,
                request,
                response
        );
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Compare
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void compare(
            Session session,
            CompareRequest request,
            CompareResponse response
    ) throws Exception {

        PartitionContext partitionContext = partition.getPartitionContext();
        PenroseContext penroseContext = partitionContext.getPenroseContext();

        Partitions partitions = penroseContext.getPartitions();
        Handler handler = partitions.getHandler(partition, this);

        DN dn = request.getDn();

        SourceValues sourceValues = new SourceValues();

        boolean fetchEntry = fetch;

        String s = getParameter(FETCH);
        if (s != null) fetchEntry = Boolean.valueOf(s);

        if (fetchEntry) {
            SearchResult sr = find(null, dn);
            sourceValues.add(sr.getSourceValues());
        } else {
            EngineTool.extractSourceValues(this, dn, sourceValues);
        }

        EngineTool.propagateDown(this, sourceValues);

        if (debug) {
            log.debug("Source values:");
            sourceValues.print();
        }

        Engine engine = partitions.getEngine(partition, handler, this);
        engine.compare(
                session,
                this,
                sourceValues,
                request,
                response
        );
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Delete
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void delete(
            Session session,
            DeleteRequest request,
            DeleteResponse response
    ) throws Exception {

        PartitionContext partitionContext = partition.getPartitionContext();
        PenroseContext penroseContext = partitionContext.getPenroseContext();

        Partitions partitions = penroseContext.getPartitions();
        Handler handler = partitions.getHandler(partition, this);

        DN dn = request.getDn();

        SourceValues sourceValues = new SourceValues();

        boolean fetchEntry = fetch;

        String s = getParameter(FETCH);
        if (s != null) fetchEntry = Boolean.valueOf(s);

        if (fetchEntry) {
            SearchResult sr = find(session, dn);
            sourceValues.add(sr.getSourceValues());
        } else {
            EngineTool.extractSourceValues(this, dn, sourceValues);
        }

        EngineTool.propagateDown(this, sourceValues);

        if (debug) {
            log.debug("Source values:");
            sourceValues.print();
        }

        Engine engine = partitions.getEngine(partition, handler, this);
        engine.delete(
                session,
                this,
                sourceValues,
                request,
                response
        );
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Find
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public Collection<Entry> findEntries(DN dn) throws Exception {

        Collection<Entry> results = new ArrayList<Entry>();
        if (dn == null) return results;

        if (getDn().matches(dn)) {
            results.add(this);
            return results;
        }

        for (Entry child : children.values()) {
            Collection<Entry> list = child.findEntries(dn);
            results.addAll(list);
        }

        return results;
    }

    public SearchResult find(
            Session session,
            DN dn
    ) throws Exception {

        PartitionContext partitionContext = partition.getPartitionContext();
        PenroseContext penroseContext = partitionContext.getPenroseContext();

        Partitions partitions = penroseContext.getPartitions();
        Handler handler = partitions.getHandler(partition, this);

        Engine engine = partitions.getEngine(partition, handler, this);
        return engine.find(session, this, dn);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Modify
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modify(
            Session session,
            ModifyRequest request,
            ModifyResponse response
    ) throws Exception {

        PartitionContext partitionContext = partition.getPartitionContext();
        PenroseContext penroseContext = partitionContext.getPenroseContext();

        Partitions partitions = penroseContext.getPartitions();
        Handler handler = partitions.getHandler(partition, this);

        DN dn = request.getDn();

        SourceValues sourceValues = new SourceValues();

        boolean fetchEntry = fetch;

        String s = getParameter(FETCH);
        if (s != null) fetchEntry = Boolean.valueOf(s);

        if (fetchEntry) {
            SearchResult sr = find(session, dn);
            sourceValues.add(sr.getSourceValues());
        } else {
            EngineTool.extractSourceValues(this, dn, sourceValues);
        }

        EngineTool.propagateDown(this, sourceValues);

        if (debug) {
            log.debug("Source values:");
            sourceValues.print();
        }

        Engine engine = partitions.getEngine(partition, handler, this);
        engine.modify(session, this, sourceValues, request, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // ModRDN
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modrdn(
            Session session,
            ModRdnRequest request,
            ModRdnResponse response
    ) throws Exception {

        PartitionContext partitionContext = partition.getPartitionContext();
        PenroseContext penroseContext = partitionContext.getPenroseContext();

        Partitions partitions = penroseContext.getPartitions();
        Handler handler = partitions.getHandler(partition, this);

        DN dn = request.getDn();

        SourceValues sourceValues = new SourceValues();

        boolean fetchEntry = fetch;

        String s = getParameter(FETCH);
        if (s != null) fetchEntry = Boolean.valueOf(s);

        if (fetchEntry) {
            SearchResult sr = find(session, dn);
            sourceValues.add(sr.getSourceValues());

        } else {
            EngineTool.extractSourceValues(this, dn, sourceValues);
        }

        EngineTool.propagateDown(this, sourceValues);

        if (debug) {
            log.debug("Source values:");
            sourceValues.print();
        }

        Engine engine = partitions.getEngine(partition, handler, this);
        engine.modrdn(
                session,
                this,
                sourceValues,
                request,
                response
        );
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Search
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void search(
            Session session,
            SearchRequest request,
            SearchResponse response
    ) throws Exception {

        if (debug) log.debug("Searching "+LDAP.getScope(request.getScope())+" with base "+entryMapping.getDn());

        DN dn = request.getDn();

        SourceValues sourceValues = new SourceValues();

        boolean fetchEntry = fetch;

        String s = getParameter(FETCH);
        if (s != null) fetchEntry = Boolean.valueOf(s);

        if (fetchEntry) {
            SearchResult sr = find(session, dn);
            sourceValues.add(sr.getSourceValues());

            response.add(sr);
            return;

        } else {
           EngineTool.extractSourceValues(this, dn, sourceValues);
        }

        //EngineTool.propagateDown(partition, entry, sourceValues);

        if (debug) {
            log.debug("Source values:");
            sourceValues.print();
        }

        search(session, this, sourceValues, request, response);

        response.close();
    }

    public void search(
            Session session,
            Entry base,
            SourceValues sourceValues,
            SearchRequest request,
            SearchResponse response
    ) throws Exception {

        int scope = request.getScope();

        if (scope == SearchRequest.SCOPE_BASE) {

            searchEntry(session, base, sourceValues, request, response);

        } else if (scope == SearchRequest.SCOPE_ONE) {

            if (base == this) {

                if (debug) log.debug("Searching children of "+entryMapping.getDn()+" ("+children.size()+")");

                for (Entry child : children.values()) {
                    child.search(session, base, sourceValues, request, response);
                }

            } else {
                searchEntry(session, base, sourceValues, request, response);
            }

        } else if (scope == SearchRequest.SCOPE_SUB) {

            searchEntry(session, base, sourceValues, request, response);

            if (debug) log.debug("Searching children of "+entryMapping.getDn()+" ("+children.size()+")");

            for (Entry child : children.values()) {
                child.search(session, base, sourceValues, request, response);
            }
        }
    }

    public void searchEntry(
            Session session,
            Entry base,
            SourceValues sourceValues,
            SearchRequest request,
            SearchResponse response
    ) throws Exception {

        if (debug) log.debug("Searching entry "+entryMapping.getDn());

        Directory directory = getDirectory();
        Partition partition = directory.getPartition();
        PartitionContext partitionContext = partition.getPartitionContext();
        PenroseContext penroseContext = partitionContext.getPenroseContext();

        Partitions partitions = penroseContext.getPartitions();
        Handler handler = partitions.getHandler(partition, this);

        Engine engine = partitions.getEngine(partition, handler, this);

        engine.search(
                session,
                base,
                this,
                sourceValues,
                request,
                response
        );
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Unbind
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void unbind(
            Session session,
            UnbindRequest request,
            UnbindResponse response
    ) throws Exception {

        PartitionContext partitionContext = partition.getPartitionContext();
        PenroseContext penroseContext = partitionContext.getPenroseContext();

        Partitions partitions = penroseContext.getPartitions();
        Handler handler = partitions.getHandler(partition, this);

        Engine engine = partitions.getEngine(partition, handler, this);
        engine.unbind(
                session,
                this,
                request,
                response
        );
    }

    public Object clone() throws CloneNotSupportedException {

        Entry entry = (Entry)super.clone();

        entry.entryMapping = (EntryMapping)entryMapping.clone();
        entry.entryContext = entryContext;

        entry.localSourceRefs = new LinkedHashMap<String,SourceRef>();
        entry.localPrimarySourceRefs = new LinkedHashMap<String,SourceRef>();

        entry.sourceRefs = new LinkedHashMap<String,SourceRef>();
        entry.primarySourceRefs = new LinkedHashMap<String,SourceRef>();

        for (String alias : sourceRefs.keySet()) {
            SourceRef sourceRef = (SourceRef)sourceRefs.get(alias).clone();
            entry.sourceRefs.put(alias, sourceRef);

            if (primarySourceRefs.containsKey(alias)) {
                entry.primarySourceRefs.put(alias, sourceRef);
            }

            if (localSourceRefs.containsKey(alias)) {
                entry.localSourceRefs.put(alias, sourceRef);
            }

            if (localPrimarySourceRefs.containsKey(alias)) {
                entry.localPrimarySourceRefs.put(alias, sourceRef);
            }
        }

        entry.parent = parent;

        entry.children = new LinkedHashMap<String,Entry>();
        entry.childrenByRdn = new LinkedHashMap<String,Collection<Entry>>();

        for (Entry origChild : children.values()) {
            Entry child = (Entry)origChild.clone();
            child.setParent(entry);
            
            entry.children.put(child.getId(), child);

            String rdn = child.getDn().getRdn().getNormalized();
            Collection<Entry> c = entry.childrenByRdn.get(rdn);
            if (c == null) {
                c = new ArrayList<Entry>();
                entry.childrenByRdn.put(rdn, c);
            }
            c.add(child);
        }

        entry.partition = partition;

        entry.fetch = fetch;

        return entry;
    }

    public Attributes getAttributes() throws Exception {
        PartitionContext partitionContext = partition.getPartitionContext();
        PenroseContext penroseContext = partitionContext.getPenroseContext();

        InterpreterManager interpreterManager = penroseContext.getInterpreterManager();
        Interpreter interpreter = interpreterManager.newInstance();

        return getAttributes(interpreter);
    }

    public Attributes getAttributes(
            Interpreter interpreter
    ) throws Exception {

        Attributes attributes = new Attributes();

        for (AttributeMapping attributeMapping : entryMapping.getAttributeMappings()) {

            Object value = interpreter.eval(attributeMapping);
            if (value == null) continue;

            if (value instanceof Collection) {
                attributes.addValues(attributeMapping.getName(), (Collection) value);
            } else {
                attributes.addValue(attributeMapping.getName(), value);
            }
        }

        Collection<String> objectClasses = entryMapping.getObjectClasses();
        for (String objectClass : objectClasses) {
            attributes.addValue("objectClass", objectClass);
        }

        return attributes;
    }

}
