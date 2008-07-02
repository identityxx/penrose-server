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

import org.safehaus.penrose.connection.Connection;
import org.safehaus.penrose.util.TransformationUtil;
import org.safehaus.penrose.util.TextUtil;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.source.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * @author Endi S. Dewata
 */
public class DynamicEntry extends Entry implements Cloneable {

    public final static String  FETCH         = "fetch";
    public final static boolean DEFAULT_FETCH = false; // disabled

    protected boolean fetch;

    public void init() throws Exception {
        String s = getParameter(FETCH);
        fetch = s == null ? DEFAULT_FETCH : Boolean.valueOf(s);
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

        if (debug) {
            log.debug(TextUtil.displaySeparator(80));
            log.debug(TextUtil.displayLine("ADD", 80));
            log.debug(TextUtil.displayLine("Entry : "+getDn(), 80));
            log.debug(TextUtil.displayLine("DN    : "+dn, 80));
            log.debug(TextUtil.displaySeparator(80));
        }

        validatePermission(session, request);
        validateSchema(request);

        Attributes attributes = request.getAttributes();

        SourceValues sourceValues;

        if (fetch) {
            Entry parent = getParent();
            DN parentDn = parent.getDn();

            SearchResult sr = parent.find(session, parentDn);

            sourceValues = new SourceValues();
            sourceValues.add(sr.getSourceValues());

        } else {
            sourceValues = extractSourceValues(dn, attributes);
        }

        propagate(sourceValues);

        if (debug) {
            log.debug("Source values:");
            sourceValues.print();
        }

        Collection<Collection<SourceRef>> groupsOfSources = getGroupsOfSources();
        Iterator<Collection<SourceRef>> iterator = groupsOfSources.iterator();
        Collection<SourceRef> sourceRefs = iterator.next();

        Collection<SourceRef> localSourceRefs = new ArrayList<SourceRef>();

        for (SourceRef sourceRef : sourceRefs) {
            if (SourceMapping.IGNORE.equals(sourceRef.getAdd())) continue;
            if (entryConfig.getSourceMapping(sourceRef.getAlias()) == null) continue;

            localSourceRefs.add(sourceRef);
        }

        SourceRef sourceRef = sourceRefs.iterator().next();
        Source source = sourceRef.getSource();

        source.add(
                session,
                localSourceRefs,
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

        DN dn = request.getDn();

        if (debug) {
            log.debug(TextUtil.displaySeparator(80));
            log.debug(TextUtil.displayLine("BIND", 80));
            log.debug(TextUtil.displayLine("Entry : "+getDn(), 80));
            log.debug(TextUtil.displayLine("DN    : "+dn, 80));
            log.debug(TextUtil.displaySeparator(80));
        }

        SourceValues sourceValues = new SourceValues();

        if (fetch) {
            SearchResult sr = find(dn);
            sourceValues.add(sr.getSourceValues());
        } else {
            extractSourceValues(dn, sourceValues);
        }

        propagate(sourceValues);

        if (debug) {
            log.debug("Source values:");
            sourceValues.print();
        }

        Collection<Collection<SourceRef>> groupsOfSources = getGroupsOfSources();

        boolean success = true;
        boolean found = false;

        for (Collection<SourceRef> sourceRefs : groupsOfSources) {

            SourceRef sourceRef = sourceRefs.iterator().next();
            Source source = sourceRef.getSource();

            String flag = sourceRef.getBind();
            if (debug) log.debug("Flag: "+flag);

            if (SourceMapping.IGNORE.equals(flag)) {
                continue;
            }

            found |= flag != null;

            try {
                source.bind(
                        session,
                        sourceRefs,
                        sourceValues,
                        request,
                        response
                );

                if (flag == null || SourceMapping.SUFFICIENT.equals(flag)) {
                    if (debug) log.debug("Bind is sufficient.");
                    return;
                }

            } catch (Exception e) {

                log.error(e.getMessage());

                if (SourceMapping.REQUISITE.equals(flag)) {
                    if (debug) log.debug("Bind is requisite.");
                    throw e;

                } else {
                    success = false;
                }
            }
        }

        if (!found || !success) {
            log.debug("Calling default bind operation.");
            super.bind(session, request, response);
        }
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

        if (debug) {
            log.debug(TextUtil.displaySeparator(80));
            log.debug(TextUtil.displayLine("COMPARE", 80));
            log.debug(TextUtil.displayLine("Entry : "+getDn(), 80));
            log.debug(TextUtil.displayLine("DN    : "+dn, 80));
            log.debug(TextUtil.displaySeparator(80));
        }

        validatePermission(session, request);

        SourceValues sourceValues = new SourceValues();

        if (fetch) {
            SearchResult sr = find(dn);
            sourceValues.add(sr.getSourceValues());

        } else {
            extractSourceValues(dn, sourceValues);
        }

        propagate(sourceValues);

        if (debug) {
            log.debug("Source values:");
            sourceValues.print();
        }

        Collection<Collection<SourceRef>> groupsOfSources = getGroupsOfSources();
        Iterator<Collection<SourceRef>> iterator = groupsOfSources.iterator();
        Collection<SourceRef> sourceRefs = iterator.next();

        SourceRef sourceRef = sourceRefs.iterator().next();
        Source source = sourceRef.getSource();

        source.compare(
                session,
                sourceRefs,
                sourceValues,
                request,
                response
        );

        //log.debug("Calling default compare operation.");
        //super.compare(session, request, response);
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

        if (debug) {
            log.debug(TextUtil.displaySeparator(80));
            log.debug(TextUtil.displayLine("DELETE", 80));
            log.debug(TextUtil.displayLine("Entry : "+getDn(), 80));
            log.debug(TextUtil.displayLine("DN    : "+dn, 80));
            log.debug(TextUtil.displaySeparator(80));
        }

        validatePermission(session, request);

        SourceValues sourceValues = new SourceValues();

        if (fetch) {
            SearchResult sr = find(session, dn);
            sourceValues.add(sr.getSourceValues());
        } else {
            extractSourceValues(dn, sourceValues);
        }

        propagate(sourceValues);

        if (debug) {
            log.debug("Source values:");
            sourceValues.print();
        }

        Collection<Collection<SourceRef>> groupsOfSources = getGroupsOfSources();
        Iterator<Collection<SourceRef>> iterator = groupsOfSources.iterator();
        Collection<SourceRef> sourceRefs = iterator.next();

        Collection<SourceRef> localSourceRefs = new ArrayList<SourceRef>();

        for (SourceRef sourceRef : sourceRefs) {
            if (entryConfig.getSourceMapping(sourceRef.getAlias()) != null) {
                localSourceRefs.add(sourceRef);
            }
        }

        SourceRef sourceRef = sourceRefs.iterator().next();
        Source source = sourceRef.getSource();

        source.delete(
                session,
                localSourceRefs,
                sourceValues,
                request,
                response
        );
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

        if (debug) {
            log.debug(TextUtil.displaySeparator(80));
            log.debug(TextUtil.displayLine("MODIFY", 80));
            log.debug(TextUtil.displayLine("Entry : "+getDn(), 80));
            log.debug(TextUtil.displayLine("DN    : "+dn, 80));
            log.debug(TextUtil.displaySeparator(80));
        }

        validatePermission(session, request);
        validateSchema(request);

        SourceValues sourceValues = new SourceValues();

        if (fetch) {
            SearchResult sr = find(session, dn);
            sourceValues.add(sr.getSourceValues());
        } else {
            extractSourceValues(dn, sourceValues);
        }

        propagate(sourceValues);

        if (debug) {
            log.debug("Source values:");
            sourceValues.print();
        }

        Collection<Collection<SourceRef>> groupsOfSources = getGroupsOfSources();
        Iterator<Collection<SourceRef>> iterator = groupsOfSources.iterator();
        Collection<SourceRef> sourceRefs = iterator.next();

        Collection<SourceRef> localSourceRefs = new ArrayList<SourceRef>();

        for (SourceRef sourceRef : sourceRefs) {
            if (entryConfig.getSourceMapping(sourceRef.getAlias()) != null) {
                localSourceRefs.add(sourceRef);
            }
        }

        SourceRef sourceRef = sourceRefs.iterator().next();
        Source source = sourceRef.getSource();

        source.modify(
                session,
                localSourceRefs,
                sourceValues,
                request,
                response
        );
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

        if (debug) {
            log.debug(TextUtil.displaySeparator(80));
            log.debug(TextUtil.displayLine("MODRDN", 80));
            log.debug(TextUtil.displayLine("Entry : "+getDn(), 80));
            log.debug(TextUtil.displayLine("DN    : "+dn, 80));
            log.debug(TextUtil.displaySeparator(80));
        }

        validatePermission(session, request);
        validateSchema(request);

        SourceValues sourceValues = new SourceValues();

        if (fetch) {
            SearchResult sr = find(session, dn);
            sourceValues.add(sr.getSourceValues());

        } else {
            extractSourceValues(dn, sourceValues);
        }

        propagate(sourceValues);

        if (debug) {
            log.debug("Source values:");
            sourceValues.print();
        }

        Collection<Collection<SourceRef>> groupsOfSources = getGroupsOfSources();
        Iterator<Collection<SourceRef>> iterator = groupsOfSources.iterator();
        Collection<SourceRef> sourceRefs = iterator.next();

        Collection<SourceRef> localSourceRefs = new ArrayList<SourceRef>();

        for (SourceRef sourceRef : sourceRefs) {
            if (entryConfig.getSourceMapping(sourceRef.getAlias()) != null) {
                localSourceRefs.add(sourceRef);
            }
        }

        SourceRef sourceRef = sourceRefs.iterator().next();
        Source source = sourceRef.getSource();

        source.modrdn(
                session,
                localSourceRefs,
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

        final DN baseDn     = request.getDn();
        final Filter filter = request.getFilter();
        final int scope     = request.getScope();

        if (debug) {
            log.debug(TextUtil.displaySeparator(80));
            log.debug(TextUtil.displayLine("SEARCH", 80));
            log.debug(TextUtil.displayLine("Entry  : "+getDn(), 80));
            log.debug(TextUtil.displayLine("Base   : "+baseDn, 80));
            log.debug(TextUtil.displayLine("Filter : "+filter, 80));
            log.debug(TextUtil.displayLine("Scope  : "+ LDAP.getScope(scope), 80));
            log.debug(TextUtil.displaySeparator(80));
        }

        response = createSearchResponse(session, request, response);

        try {
            validateScope(request);
            validatePermission(session, request);
            validateFilter(filter);

        } catch (Exception e) {
            response.close();
            return;
        }

        try {
            generateSearchResults(session, request, response);

        } finally {
            response.close();
        }
    }

    public void generateSearchResults(
            Session session,
            SearchRequest request,
            SearchResponse response
    ) throws Exception {

        DN baseDn = request.getDn();
        
        SourceValues sourceValues = new SourceValues();

        if (fetch) {
            SearchResult result = find(session, baseDn);
            sourceValues.add(result.getSourceValues());

            response.add(result);
            return;

        } else {
            extractSourceValues(baseDn, sourceValues);
        }

        if (debug) {
            log.debug("Source values:");
            sourceValues.print();
        }

        Interpreter interpreter = partition.newInterpreter();

        List<Collection<SourceRef>> groupsOfSources = getGroupsOfSources(
                request
        );

        Collection<SourceRef> sourceRefs = groupsOfSources.get(0);

        SearchRequest sourceRequest = (SearchRequest)request.clone();
        if (!getDn().matches(baseDn)) sourceRequest.setDn((DN)null);

        DynamicSearchResponse response2 = new DynamicSearchResponse(
                session,
                this,
                groupsOfSources,
                sourceValues,
                interpreter,
                sourceRequest,
                response
        );

        Collection<SourceRef> localSourceRefs = getLocalSourceRefs();

        SourceRef sourceRef = sourceRefs.iterator().next();
        Source source = sourceRef.getSource();

        source.search(
                session,
                //primarySourceRefs,
                localSourceRefs,
                sourceRefs,
                sourceValues,
                sourceRequest,
                response2
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

        DN dn = session.getBindDn();

        if (debug) {
            log.debug(TextUtil.displaySeparator(80));
            log.debug(TextUtil.displayLine("UNBIND", 80));
            log.debug(TextUtil.displayLine("Entry : "+getDn(), 80));
            log.debug(TextUtil.displayLine("DN    : "+dn, 80));
            log.debug(TextUtil.displaySeparator(80));
        }

    }

    public Collection<SourceMapping> getEffectiveSourceMappings() {
        Collection<SourceMapping> list = new ArrayList<SourceMapping>();
        list.addAll(entryConfig.getSourceMappings());

        Entry parent = getParent();
        if (parent == null) {
            return list;
        }

        if (!(parent instanceof DynamicEntry)) {
            return list;
        }

        DynamicEntry dynamicParent = (DynamicEntry)parent;
        list.addAll(dynamicParent.getEffectiveSourceMappings());

        return list;
    }

    public List<Collection<SourceRef>> getGroupsOfSources() throws Exception {

        List<Collection<SourceRef>> results = new ArrayList<Collection<SourceRef>>();

        Collection<SourceRef> list = new ArrayList<SourceRef>();
        Connection lastConnection = null;

        for (Entry e : getPath()) {

            for (SourceRef sourceRef : e.getLocalSourceRefs()) {

                Source source = sourceRef.getSource();
                Connection connection = source.getConnection();

                if (lastConnection == null) {
                    lastConnection = connection;

                } else if (lastConnection != connection || !connection.isJoinSupported()) {
                    results.add(list);
                    list = new ArrayList<SourceRef>();
                    lastConnection = connection;
                }

                list.add(sourceRef);
            }
        }

        if (!list.isEmpty()) results.add(list);

        return results;
    }

    public List<Collection<SourceRef>> getGroupsOfSources(
            SearchRequest request
    ) throws Exception {

        DN baseDn = request.getDn();

        if (getDn().matches(baseDn)) { // if (entry == base) {
            return getGroupsOfSources();
        }

        List<Collection<SourceRef>> results = new ArrayList<Collection<SourceRef>>();

        Collection<SourceRef> list = new ArrayList<SourceRef>();
        Connection lastConnection = null;

        for (Entry e : getRelativePath(baseDn)) {

            for (SourceRef sourceRef : e.getLocalSourceRefs()) {

                Source source = sourceRef.getSource();
                Connection connection = source.getConnection();

                if (lastConnection == null) {
                    lastConnection = connection;

                } else if (lastConnection != connection || !connection.isJoinSupported()) {
                    results.add(list);
                    list = new ArrayList<SourceRef>();
                    lastConnection = connection;
                }

                list.add(sourceRef);
            }
        }

        if (!list.isEmpty()) results.add(list);

        return results;
    }

    public Collection<DN> computeDns(Interpreter interpreter) throws Exception {

        Collection<DN> dns = new ArrayList<DN>();
        Collection<DN> parentDns = new ArrayList<DN>();

        Entry parent = getParent();
        if (parent != null) {
            if (parent instanceof DynamicEntry) {
                DynamicEntry dynamicParent = (DynamicEntry)parent;
                parentDns.addAll(dynamicParent.computeDns(interpreter));

            } else {
                parentDns.add(parent.getDn());
            }

        } else if (!getParentDn().isEmpty()) {
            parentDns.add(getParentDn());
        }

        if (parentDns.isEmpty()) {
            DN dn = getDn();
            if (debug) log.debug("DN: "+dn);
            dns.add(dn);

        } else {
            Collection<RDN> rdns = computeRdns(interpreter);

            DNBuilder db = new DNBuilder();

            for (RDN rdn : rdns) {
                //log.info("Processing RDN: "+rdn);

                for (DN parentDn : parentDns) {
                    //log.debug("Appending parent DN: "+parentDn);

                    db.set(rdn);
                    db.append(parentDn);
                    DN dn = db.toDn();

                    if (debug) log.debug("DN: " + dn);
                    dns.add(dn);
                }
            }
        }

        return dns;
    }

    public Collection<RDN> computeRdns(
            Interpreter interpreter
    ) throws Exception {

        //log.debug("Computing RDNs:");
        Attributes attributes = new Attributes();

        Collection<AttributeMapping> rdnAttributes = getRdnAttributeMappings();
        for (AttributeMapping attributeMapping : rdnAttributes) {
            String name = attributeMapping.getName();

            Object value = interpreter.eval(attributeMapping);
            if (value == null) continue;

            attributes.addValue(name, value);
        }

        return TransformationUtil.convert(attributes);
    }

    public void extractSourceValues(
            DN baseDn,
            SourceValues sourceValues
    ) throws Exception {

        if (debug) log.debug("Extracting DN: "+baseDn);

        Interpreter interpreter = partition.newInterpreter();

        Entry base = this;
        while (base.getDn().getSize() > baseDn.getSize()) base = base.getParent();

        for (Entry entry : base.getPath()) {

            int i = baseDn.getSize() - entry.getDn().getSize();
            RDN rdn = baseDn.get(i);

            if (debug) {
                log.debug(" - RDN: "+rdn);
                //sourceValues.print();
            }

            if (!(entry instanceof DynamicEntry)) continue;

            DynamicEntry de = (DynamicEntry)entry;
            de.extractSourceValues(
                    rdn,
                    interpreter,
                    sourceValues
            );
        }
    }

    public void extractSourceValues(
            RDN rdn,
            Interpreter interpreter,
            SourceValues sourceValues
    ) throws Exception {

        interpreter.set(sourceValues);
        interpreter.set(rdn);
        interpreter.set("rdn", rdn);

        for (SourceRef sourceRef : getLocalSourceRefs()) {

            if (debug) log.debug("   - Source: "+sourceRef.getAlias()+" ("+sourceRef.getSource().getName()+")");

            extractSourceValues(
                    sourceRef,
                    interpreter,
                    sourceValues
            );
        }

        interpreter.clear();
    }

    public void extractSourceValues(
            SourceRef sourceRef,
            Interpreter interpreter,
            SourceValues sourceValues
    ) throws Exception {

        SourceMapping sourceMapping = getSourceMapping(sourceRef.getAlias());

        Attributes attributes = sourceValues.get(sourceMapping.getName());

        for (FieldRef fieldRef : sourceRef.getFieldRefs()) {
            String name = fieldRef.getName();

            for (FieldMapping fieldMapping : sourceMapping.getFieldMappings(name)) {
                Object value = interpreter.eval(fieldMapping);
                if (value == null) continue;

                if ("INTEGER".equals(fieldRef.getType()) && value instanceof String) {
                    value = Integer.parseInt((String)value);
                }

                if (fieldMapping.isPrimaryKey()) {
                    String n = "primaryKey."+name;
                    attributes.addValue(n, value);
                    if (debug) log.debug("     - " + n + ": " + value);

                } else {
                    attributes.addValue(name, value);
                    if (debug) log.debug("     - " + name + ": " + value);
                }
            }
        }
    }

    public void propagate(
            SourceValues sourceValues
    ) throws Exception {

        Interpreter interpreter = partition.newInterpreter();

        propagate(sourceValues, interpreter);
    }

    public void propagate(
            SourceValues sourceValues,
            Interpreter interpreter
    ) throws Exception {

        List<Entry> path = getPath();

        for (Entry entry : path) {

            if (!(entry instanceof DynamicEntry)) continue;

            DynamicEntry dynamicEntry = (DynamicEntry)entry;
            
            Collection<SourceMapping> sourceMappings = dynamicEntry.getSourceMappings();
            for (SourceMapping sourceMapping : sourceMappings) {
                dynamicEntry.propagateSource(sourceMapping, sourceValues, interpreter);
            }
        }
    }

    public void propagateSource(
            SourceMapping sourceMapping,
            SourceValues sourceValues,
            Interpreter interpreter
    ) throws Exception {

        if (debug) log.debug("Propagating source "+sourceMapping.getName()+" in "+getDn()+":");

        interpreter.set(sourceValues);

        Collection<FieldMapping> fieldMappings = sourceMapping.getFieldMappings();
        for (FieldMapping fieldMapping : fieldMappings) {

            propagateField(
                    sourceMapping,
                    fieldMapping,
                    sourceValues,
                    interpreter
            );
        }

        interpreter.clear();
    }

    public void propagateField(
            SourceMapping sourceMapping,
            FieldMapping fieldMapping,
            SourceValues sourceValues,
            Interpreter interpreter
    ) throws Exception {

        String lsourceName = sourceMapping.getName();

        String lfieldName;
        if (fieldMapping.isPrimaryKey()) {
            lfieldName = "primaryKey."+fieldMapping.getName();
        } else {
            lfieldName = fieldMapping.getName();
        }

        String lhs = lsourceName + "." + lfieldName;

        Attributes lattributes = sourceValues.get(lsourceName);
        Attribute lattribute = lattributes.get(lfieldName);

        if (lattribute != null && !lattribute.isEmpty()) {
            if (debug) {
                for (Object value : lattribute.getValues()) {
                    log.debug(" - "+lhs+" has been set to ["+value+"].");
                }
            }
            return;
        }

        Object value = interpreter.eval(fieldMapping);
        if (value == null) {
            if (debug) log.debug(" - "+lhs+" is null.");
            return;
        }

        if (debug) log.debug(" - "+lhs+": "+value);

        if (value instanceof Collection) {
            lattributes.addValues(lfieldName, (Collection)value);
        } else {
            lattributes.addValue(lfieldName, value);
        }
    }
}