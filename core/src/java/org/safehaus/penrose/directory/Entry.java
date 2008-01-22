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
import org.safehaus.penrose.source.Source;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionContext;
import org.safehaus.penrose.acl.ACI;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.engine.Engine;
import org.safehaus.penrose.engine.EngineTool;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.schema.SchemaManager;
import org.safehaus.penrose.schema.ObjectClass;
import org.safehaus.penrose.cache.CacheManager;
import org.safehaus.penrose.cache.CacheKey;
import org.safehaus.penrose.cache.Cache;
import org.safehaus.penrose.filter.FilterEvaluator;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.Formatter;

/**
 * @author Endi S. Dewata
 */
public class Entry implements Cloneable {

    public Logger log = LoggerFactory.getLogger(getClass());
    public boolean debug = log.isDebugEnabled();

    public final static Collection<Entry> EMPTY_ENTRIES = new ArrayList<Entry>();

    public final static String  FETCH                   = "fetch";
    public final static boolean DEFAULT_FETCH           = false; // disabled

    public final static String  SCHEMA_CHECKING         = "schemaChecking";
    public final static boolean DEFAULT_SCHEMA_CHECKING = false; // disabled

    public final static String CACHE                    = "cache";
    public final static boolean DEFAULT_CACHE           = false; // disabled

    public final static String CACHE_SIZE               = "cacheSize";
    public final static int DEFAULT_CACHE_SIZE          = 10; // entries

    public final static String CACHE_EXPIRATION         = "cacheExpiration";
    public final static int DEFAULT_CACHE_EXPIRATION    = 10; // minutes

    protected EntryMapping entryMapping;
    protected EntryContext entryContext;

    protected Map<String,SourceRef> localSourceRefs = new LinkedHashMap<String,SourceRef>();
    protected Map<String,SourceRef> localPrimarySourceRefs = new LinkedHashMap<String,SourceRef>();

    protected List<SourceRef> sourceRefs              = new ArrayList<SourceRef>();
    protected Map<String,SourceRef> sourceRefsByName  = new LinkedHashMap<String,SourceRef>();
    protected Map<String,SourceRef> primarySourceRefs = new LinkedHashMap<String,SourceRef>();

    protected Entry parent;

    protected Collection<Entry> children = new LinkedHashSet<Entry>();
    protected Map<String,Collection<Entry>> childrenByRdn = new LinkedHashMap<String,Collection<Entry>>();

    protected Partition partition;

    protected boolean fetch;
    protected boolean schemaChecking;

    protected CacheManager cacheManager;

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

            sourceRefs.add(sourceRef);
            localSourceRefs.put(alias, sourceRef);
            sourceRefsByName.put(alias, sourceRef);

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

                sourceRefs.add(sourceRef);
                sourceRefsByName.put(alias, sourceRef);

                if (alias.equals(psn)) {
                    primarySourceRefs.put(alias, sourceRef);
                }
            }

            parent = parent.getParent();
        }

        String s = getParameter(FETCH);
        fetch = s == null ? DEFAULT_FETCH : Boolean.valueOf(s);

        s = getParameter(SCHEMA_CHECKING);
        schemaChecking = s == null ? DEFAULT_SCHEMA_CHECKING : Boolean.valueOf(s);

        s = getParameter(CACHE);
        boolean cacheEnabled = s == null ? DEFAULT_CACHE : Boolean.parseBoolean(s);

        if (cacheEnabled) {
            s = getParameter(CACHE_SIZE);
            int cacheSize = s == null ? DEFAULT_CACHE_SIZE : Integer.parseInt(s);
    
            s = getParameter(CACHE_EXPIRATION);
            int cacheExpiration = s == null ? DEFAULT_CACHE_EXPIRATION : Integer.parseInt(s);

            cacheManager = new CacheManager(cacheSize);
            cacheManager.setExpiration(cacheExpiration);
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

    public RDN getRdn() {
        return entryMapping.getRdn();
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
        return sourceRefs;
    }

    public int getSourceRefsCount() {
        return sourceRefs.size();
    }

    public SourceRef getSourceRef(String name) {
        return sourceRefsByName.get(name);
    }

    public SourceRef getSourceRef(int index) {
        return sourceRefs.get(index);
    }

    public Collection<SourceRef> getPrimarySourceRefs() {
        return primarySourceRefs.values();
    }

    public void setPrimarySourceRefs(Map<String, SourceRef> primarySourceRefs) {
        this.primarySourceRefs = primarySourceRefs;
    }

    public Collection<Entry> getChildren() {
        return children;
    }

    public Collection<Entry> getChildren(RDN rdn) {
        if (rdn == null) return EMPTY_ENTRIES;

        Collection<Entry> list = childrenByRdn.get(rdn.getNormalized());
        if (list == null) return EMPTY_ENTRIES;

        return new ArrayList<Entry>(list);
    }

    public void addChild(Entry child) {

        String rdn = child.getDn().getRdn().getNormalized();

        children.add(child);
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

        DN dn = request.getDn();
        Attributes attributes = request.getAttributes();

        PartitionContext partitionContext = partition.getPartitionContext();
        PenroseContext penroseContext = partitionContext.getPenroseContext();

        if (schemaChecking) {
            SchemaManager schemaManager = penroseContext.getSchemaManager();
            Collection<ObjectClass> objectClasses = schemaManager.getObjectClasses(this);

            for (Attribute attribute : attributes.getAll()) {
                String attributeName = attribute.getName();
                boolean found = false;

                for (ObjectClass oc : objectClasses) {
                    if (oc.getName().equalsIgnoreCase("extensibleObject")) {
                        found = true;
                        break;
                    }

                    if (oc.containsRequiredAttribute(attributeName)) {
                        found = true;
                        break;
                    }

                    if (oc.containsOptionalAttribute(attributeName)) {
                        found = true;
                        break;
                    }
                }

                if (!found) {
                    throw LDAP.createException(LDAP.OBJECT_CLASS_VIOLATION);
                }
            }

            for (ObjectClass oc : objectClasses) {
                for (String attributeName : oc.getRequiredAttributes()) {
                    if (attributes.get(attributeName) == null) {
                        throw LDAP.createException(LDAP.OBJECT_CLASS_VIOLATION);
                    }
                }
            }
        }

        SourceValues sourceValues;

        if (fetch) {
            DN parentDn = parent.getDn();

            SearchResult sr = parent.find(session, parentDn);

            sourceValues = new SourceValues();
            sourceValues.add(sr.getSourceValues());

        } else {
            sourceValues = extractSourceValues(dn, attributes);
        }

        EngineTool.propagateDown(this, sourceValues);

        if (debug) {
            log.debug("Source values:");
            sourceValues.print();
        }

        Engine engine = partition.getEngine();
        engine.add(
                session,
                this,
                sourceValues,
                request,
                response
        );

        if (cacheManager != null) cacheManager.clear();
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Bind
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void bind(
            Session session,
            BindRequest request,
            BindResponse response
    ) throws Exception {

        DN dn = request.getDn();

        if (debug) {
            log.debug(org.safehaus.penrose.util.Formatter.displaySeparator(80));
            log.debug(org.safehaus.penrose.util.Formatter.displayLine("BIND", 80));
            log.debug(org.safehaus.penrose.util.Formatter.displayLine("Entry : "+getDn(), 80));
            log.debug(org.safehaus.penrose.util.Formatter.displayLine("DN    : "+dn, 80));
            log.debug(org.safehaus.penrose.util.Formatter.displaySeparator(80));
        }

        SourceValues sourceValues = new SourceValues();

        if (fetch) {
            SearchResult sr = find(dn);
            sourceValues.add(sr.getSourceValues());
        } else {
            EngineTool.extractSourceValues(this, dn, sourceValues);
        }

        EngineTool.propagateDown(this, sourceValues);

        if (debug) {
            log.debug("Source values:");
            sourceValues.print();
        }

        Engine engine = partition.getEngine();
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

        DN dn = request.getDn();

        SourceValues sourceValues = new SourceValues();

        if (fetch) {
            SearchResult sr = find(dn);
            sourceValues.add(sr.getSourceValues());
        } else {
            EngineTool.extractSourceValues(this, dn, sourceValues);
        }

        EngineTool.propagateDown(this, sourceValues);

        if (debug) {
            log.debug("Source values:");
            sourceValues.print();
        }

        Engine engine = partition.getEngine();
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

        DN dn = request.getDn();

        SourceValues sourceValues = new SourceValues();

        if (fetch) {
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

        Engine engine = partition.getEngine();
        engine.delete(
                session,
                this,
                sourceValues,
                request,
                response
        );

        if (cacheManager != null) cacheManager.clear();
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Find
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public Collection<Entry> findEntries(DN dn) throws Exception {

        if (debug) log.debug("Finding matching entries for "+dn+":");

        if (dn == null) return EMPTY_ENTRIES;

        DN thisDn = getDn();
        int level = thisDn.getSize() - 1;
        int length = dn.getSize();

        if (!dn.endsWith(thisDn)) {
            //if (debug) log.debug("Doesn't match "+thisDn);
            return EMPTY_ENTRIES;
        }

        if (level < length - 1) { // children has priority
            Collection<Entry> results = new ArrayList<Entry>();
            for (Entry child : children) {
                Collection<Entry> list = child.findEntries(dn, level + 1);
                results.addAll(list);
            }
            return results;
        }

        Collection<Entry> results = new ArrayList<Entry>();
        results.add(this);
        
        if (debug) log.debug("Found entry "+getDn());

        return results;
    }

    public Collection<Entry> findEntries(DN dn, int level) throws Exception {

        RDN thisRdn = getRdn();
        int length = dn.getSize();
        RDN rdn = dn.get(length - level - 1);

        if (!thisRdn.matches(rdn)) {
            //if (debug) log.debug("Doesn't match with "+getDn());
            return EMPTY_ENTRIES;
        }

        if (level < length - 1) { // children has priority
            Collection<Entry> results = new ArrayList<Entry>();
            for (Entry child : children) {
                Collection<Entry> list = child.findEntries(dn, level + 1);
                results.addAll(list);
            }
            return results;
        }

        Collection<Entry> results = new ArrayList<Entry>();
        results.add(this);
        
        if (debug) log.debug("Found entry "+getDn());

        return results;
    }

    public SearchResult find(DN dn) throws Exception {
        return find(null, dn);
    }

    public SearchResult find(
            Session session,
            DN dn
    ) throws Exception {

        SearchRequest request = new SearchRequest();
        request.setDn(dn);
        request.setScope(SearchRequest.SCOPE_BASE);

        SearchResponse response = new SearchResponse();

        SourceValues sourceValues = new SourceValues();

        searchEntry(
                session,
                this,
                sourceValues,
                request,
                response
        );

        if (!response.hasNext()) {
            if (debug) log.debug("Entry "+dn+" not found");
            throw LDAP.createException(LDAP.NO_SUCH_OBJECT);
        }

        return response.next();
/*
        Engine engine = partition.getEngine();
        return engine.find(session, this, dn);
*/
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Modify
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modify(
            Session session,
            ModifyRequest request,
            ModifyResponse response
    ) throws Exception {

        DN dn = request.getDn();

        PartitionContext partitionContext = partition.getPartitionContext();
        PenroseContext penroseContext = partitionContext.getPenroseContext();

        if (schemaChecking) {
            SchemaManager schemaManager = penroseContext.getSchemaManager();
            Collection<ObjectClass> objectClasses = schemaManager.getObjectClasses(this);

            Collection<Modification> modifications = request.getModifications();
            for (Modification modification : modifications) {
                int type = modification.getType();
                Attribute attribute = modification.getAttribute();
                String attributeName = attribute.getName();

                if (type == Modification.ADD) {

                    boolean found = false;

                    for (ObjectClass oc : objectClasses) {
                        if (oc.getName().equalsIgnoreCase("extensibleObject")) {
                            found = true;
                            break;
                        }

                        if (oc.containsRequiredAttribute(attributeName)) {
                            found = true;
                            break;
                        }

                        if (oc.containsOptionalAttribute(attributeName)) {
                            found = true;
                            break;
                        }
                    }

                    if (!found) {
                        throw LDAP.createException(LDAP.OBJECT_CLASS_VIOLATION);
                    }

                } else if (type == Modification.DELETE && attribute.isEmpty()) {

                    boolean found = false;

                    for (ObjectClass oc : objectClasses) {
                        if (oc.containsRequiredAttribute(attributeName)) {
                            found = true;
                            break;
                        }
                    }

                    if (found) {
                        throw LDAP.createException(LDAP.OBJECT_CLASS_VIOLATION);
                    }
                }
            }
        }

        SourceValues sourceValues = new SourceValues();

        if (fetch) {
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

        Engine engine = partition.getEngine();
        engine.modify(session, this, sourceValues, request, response);

        if (cacheManager != null) cacheManager.clear();
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // ModRDN
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modrdn(
            Session session,
            ModRdnRequest request,
            ModRdnResponse response
    ) throws Exception {

        DN dn = request.getDn();

        PartitionContext partitionContext = partition.getPartitionContext();
        PenroseContext penroseContext = partitionContext.getPenroseContext();

        if (schemaChecking) {
            SchemaManager schemaManager = penroseContext.getSchemaManager();
            Collection<ObjectClass> objectClasses = schemaManager.getObjectClasses(this);

            RDN newRdn = request.getNewRdn();
            for (String attributeName : newRdn.getNames()) {

                boolean found = false;

                for (ObjectClass oc : objectClasses) {
                    if (oc.getName().equalsIgnoreCase("extensibleObject")) {
                        found = true;
                        break;
                    }

                    if (oc.containsRequiredAttribute(attributeName)) {
                        found = true;
                        break;
                    }

                    if (oc.containsOptionalAttribute(attributeName)) {
                        found = true;
                        break;
                    }
                }

                if (!found) {
                    throw LDAP.createException(LDAP.OBJECT_CLASS_VIOLATION);
                }
            }

            for (String attributeName : newRdn.getNames()) {
                for (ObjectClass oc : objectClasses) {
                    if (oc.containsRequiredAttribute(attributeName)) {
                        throw LDAP.createException(LDAP.OBJECT_CLASS_VIOLATION);
                    }
                }
            }
        }

        SourceValues sourceValues = new SourceValues();

        if (fetch) {
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

        Engine engine = partition.getEngine();
        engine.modrdn(
                session,
                this,
                sourceValues,
                request,
                response
        );

        if (cacheManager != null) cacheManager.clear();
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Search
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void search(
            Session session,
            SearchRequest request,
            SearchResponse response
    ) throws Exception {

        DN dn = request.getDn();

        SourceValues sourceValues = new SourceValues();

        if (fetch) {
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

                for (Entry child : children) {
                    child.search(session, base, sourceValues, request, response);
                }

            } else {
                try {
                    searchEntry(session, base, sourceValues, request, response);
                } catch (Exception e) {
                    // ignore
                }
            }

        } else if (scope == SearchRequest.SCOPE_SUB) {

            try {
                searchEntry(session, base, sourceValues, request, response);
            } catch (Exception e) {
                // ignore
            }

            if (debug) log.debug("Searching children of "+entryMapping.getDn()+" ("+children.size()+")");

            for (Entry child : children) {
                child.search(session, base, sourceValues, request, response);
            }
        }
    }

    public void searchEntry(
            final Session session,
            final Entry base,
            final SourceValues sourceValues,
            final SearchRequest request,
            final SearchResponse response
    ) throws Exception {

        final DN baseDn     = request.getDn();
        final Filter filter = request.getFilter();
        final int scope     = request.getScope();

        if (debug) {
            log.debug(org.safehaus.penrose.util.Formatter.displaySeparator(80));
            log.debug(org.safehaus.penrose.util.Formatter.displayLine("SEARCH", 80));
            log.debug(org.safehaus.penrose.util.Formatter.displayLine("Entry  : "+getDn(), 80));
            log.debug(org.safehaus.penrose.util.Formatter.displayLine("Base   : "+baseDn, 80));
            log.debug(org.safehaus.penrose.util.Formatter.displayLine("Filter : "+filter, 80));
            log.debug(org.safehaus.penrose.util.Formatter.displayLine("Scope  : "+ LDAP.getScope(scope), 80));
            log.debug(org.safehaus.penrose.util.Formatter.displaySeparator(80));
        }

        PartitionContext partitionContext = partition.getPartitionContext();
        PenroseContext penroseContext = partitionContext.getPenroseContext();

        final FilterEvaluator filterEvaluator = penroseContext.getFilterEvaluator();

        if (!filterEvaluator.eval(this, filter)) {
            if (debug) log.debug("Entry \""+getDn()+"\" doesn't match search filter.");
            return;
        }

        if (debug) log.debug("Searching entry "+entryMapping.getDn());

        final Cache cache;

        if (cacheManager == null) {
            cache = null;

        } else {
            CacheKey cacheKey = new CacheKey();
            cacheKey.setSearchRequest(request);
            cacheKey.setEntry(this);

            Cache c = cacheManager.get(cacheKey);

            if (c != null) {
                log.debug("Returning results from cache.");
                for (SearchResult searchResult : c.getSearchResults()) {
                    response.add(searchResult);
                }
                return;
            }

            cache = cacheManager.create();

            int cacheSize = cacheManager.getSize();
            cache.setSize(cacheSize);

            int cacheExpiration = cacheManager.getExpiration();
            cache.setExpiration(cacheExpiration);

            cacheManager.put(cacheKey, cache);
        }

        if (getSourceRefsCount() == 0) {
            Interpreter interpreter = partition.newInterpreter();
            interpreter.set(sourceValues);
            Attributes attributes = computeAttributes(interpreter);
            interpreter.clear();

            if (filterEvaluator.eval(attributes, filter)) {
                SearchResult searchResult = new SearchResult(getDn(), attributes);
                searchResult.setEntry(this);
                searchResult.setSourceValues(sourceValues);
                response.add(searchResult);
            }

            return;
        }

        SearchResponse sr = new SearchResponse() {

            public void add(SearchResult searchResult) throws Exception {

                if (debug) log.debug("Checking filter "+filter);

                if (!filterEvaluator.eval(searchResult.getAttributes(), filter)) {
                    if (debug) log.debug("Entry \""+searchResult.getDn()+"\" doesn't match search filter.");
                    return;
                }

                response.add(searchResult);

                if (cache != null) cache.add(searchResult);
            }
        };

        Engine engine = partition.getEngine();
        engine.search(
                session,
                base,
                this,
                sourceValues,
                request,
                sr
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

        Engine engine = partition.getEngine();
        engine.unbind(
                session,
                this,
                request,
                response
        );
    }

    public DN computeDn(
            Interpreter interpreter
    ) throws Exception {

        DNBuilder db = new DNBuilder();

        RDN rdn = computeRdn(interpreter);

        if (rdn.isEmpty()) {
            log.error("RDN is empty: "+rdn);
            throw LDAP.createException(LDAP.OPERATIONS_ERROR);
        }

        db.set(rdn);

        if (parent == null) {
            db.append(entryMapping.getParentDn());

        } else {
            db.append(parent.computeDn(interpreter));
        }

        return db.toDn();
    }

    public RDN computeRdn(
            Interpreter interpreter
    ) throws Exception {

        RDNBuilder rb = new RDNBuilder();

        for (AttributeMapping attributeMapping : entryMapping.getRdnAttributeMappings()) {
            String name = attributeMapping.getName();

            Object value = interpreter.eval(attributeMapping);
            if (value == null) continue;

            rb.set(name, value);
        }

        return rb.toRdn();
    }

    public Attributes computeAttributes(
            Interpreter interpreter
    ) throws Exception {

        Attributes attributes = new Attributes();

        for (AttributeMapping attributeMapping : entryMapping.getAttributeMappings()) {

            Object value = interpreter.eval(attributeMapping);
            //log.debug("Attribute "+attributeMapping.getName()+": "+value);
            if (value == null) continue;

            if (value instanceof Collection) {
                attributes.addValues(attributeMapping.getName(), (Collection) value);
            } else {
                attributes.addValue(attributeMapping.getName(), value);
            }
        }

        for (String objectClass : entryMapping.getObjectClasses()) {
            attributes.addValue("objectClass", objectClass);
        }

        return attributes;
    }

    public SourceValues extractSourceValues(DN dn) throws Exception {

        if (debug) log.debug("Extracting dn "+dn+":");

        Interpreter interpreter = partition.newInterpreter();
        SourceValues sourceValues = new SourceValues();

        extractSourceValues(
                dn,
                this,
                interpreter,
                sourceValues
        );

        return sourceValues;
    }

    public void extractSourceValues(
            DN dn,
            Entry entry,
            Interpreter interpreter,
            SourceValues sourceValues
    ) throws Exception {

        DN parentDn = dn.getParentDn();
        Entry parent = entry.getParent();

        if (parentDn != null && parent != null) {
            extractSourceValues(parentDn, parent, interpreter, sourceValues);
        }

        RDN rdn = dn.getRdn();
        interpreter.set(rdn);

        entry.computeSources(interpreter, sourceValues);

        interpreter.clear();
    }

    public SourceValues extractSourceValues(
            DN dn,
            Attributes attributes
    ) throws Exception {

        if (debug) log.debug("Extracting entry "+dn+":");

        Interpreter interpreter = partition.newInterpreter();
        SourceValues sourceValues = new SourceValues();

        extractSourceValues(
                dn,
                attributes,
                interpreter,
                sourceValues
        );

        return sourceValues;
    }

    public void extractSourceValues(
            DN dn,
            Attributes attributes,
            Interpreter interpreter,
            SourceValues sourceValues
    ) throws Exception {

        DN parentDn = dn.getParentDn();

        if (parentDn != null && parent != null) {
            extractSourceValues(parentDn, parent, interpreter, sourceValues);
        }

        RDN rdn = dn.getRdn();
        interpreter.set(rdn);
        interpreter.set("rdn", rdn);
        interpreter.set(attributes);

        computeSources(interpreter, sourceValues);

        interpreter.clear();
    }

    public void computeSources(
            Interpreter interpreter,
            SourceValues sourceValues
    ) throws Exception {

        for (SourceRef sourceRef : localSourceRefs.values()) {

            if (debug) log.debug("Extracting source "+sourceRef.getAlias()+":");

            Attributes attributes = sourceValues.get(sourceRef.getAlias());

            for (FieldRef fieldRef : sourceRef.getFieldRefs()) {

                Object value = interpreter.eval(fieldRef);
                if (value == null) continue;

                if ("INTEGER".equals(fieldRef.getType()) && value instanceof String) {
                    value = Integer.parseInt((String)value);
                }

                attributes.addValue(fieldRef.getName(), value);

                String fieldName = sourceRef.getAlias() + "." + fieldRef.getName();
                if (debug) log.debug(" - " + fieldName + ": " + value);
            }
        }
    }

    public int hashCode() {
        return entryMapping.hashCode();
    }

    boolean equals(Object o1, Object o2) {
        if (o1 == null && o2 == null) return true;
        if (o1 != null) return o1.equals(o2);
        return o2.equals(o1);
    }

    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null) return false;
        if (object.getClass() != this.getClass()) return false;

        Entry entry = (Entry)object;
        if (!equals(entryMapping, entry.entryMapping)) return false;

        return true;
    }

    public Object clone() throws CloneNotSupportedException {

        Entry entry = (Entry)super.clone();

        entry.entryMapping = (EntryMapping)entryMapping.clone();
        entry.entryContext = entryContext;

        entry.localSourceRefs = new LinkedHashMap<String,SourceRef>();
        entry.localPrimarySourceRefs = new LinkedHashMap<String,SourceRef>();

        entry.sourceRefs        = new ArrayList<SourceRef>();
        entry.sourceRefsByName  = new LinkedHashMap<String,SourceRef>();
        entry.primarySourceRefs = new LinkedHashMap<String,SourceRef>();

        for (SourceRef origSourceRef : sourceRefs) {
            SourceRef sourceRef = (SourceRef)origSourceRef.clone();

            String alias = sourceRef.getAlias();

            entry.sourceRefs.add(sourceRef);
            entry.sourceRefsByName.put(alias, sourceRef);

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

        entry.children = new LinkedHashSet<Entry>();
        entry.childrenByRdn = new LinkedHashMap<String,Collection<Entry>>();

        for (Entry origChild : children) {
            Entry child = (Entry)origChild.clone();
            child.setParent(entry);

            entry.children.add(child);

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
}