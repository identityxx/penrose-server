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
package org.safehaus.penrose.adapter.jdbc;

import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.SubstringFilter;
import org.safehaus.penrose.filter.SimpleFilter;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.partition.FieldConfig;
import org.safehaus.penrose.partition.SourceConfig;
import org.safehaus.penrose.entry.*;
import org.safehaus.penrose.adapter.Adapter;
import org.safehaus.penrose.jdbc.*;
import org.safehaus.penrose.jdbc.Request;
import org.safehaus.penrose.source.*;
import org.safehaus.penrose.source.jdbc.JDBCSourceSync;
import org.safehaus.penrose.ldap.*;

import java.sql.ResultSet;
import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class JDBCAdapter extends Adapter {

    private JDBCClient client;

    public void init() throws Exception {
        client = new JDBCClient(getParameters());
    }

    public void start() throws Exception {
        client.connect();
    }

    public void stop() throws Exception {
        client.close();
    }

    public Object openConnection() throws Exception {
        return client.getConnection();
    }

    public String getFieldNames(SourceConfig sourceConfig) throws Exception {
        StringBuilder sb = new StringBuilder();

        Collection<FieldConfig> fields = sourceConfig.getFieldConfigs();
        for (FieldConfig fieldConfig : fields) {
            if (sb.length() > 0) sb.append(", ");
            sb.append(fieldConfig.getOriginalName());
        }

        return sb.toString();
    }

    public String getOringialPrimaryKeyFieldNamesAsString(SourceConfig sourceConfig) throws Exception {
        StringBuilder sb = new StringBuilder();

        Collection<String> fields = sourceConfig.getOriginalPrimaryKeyNames();
        for (String name : fields) {
            if (sb.length() > 0) sb.append(", ");
            sb.append(name);
        }

        return sb.toString();
    }

    public String getTableName(SourceConfig sourceConfig) {
        String catalog = sourceConfig.getParameter(JDBCClient.CATALOG);
        String schema = sourceConfig.getParameter(JDBCClient.SCHEMA);
        String table = sourceConfig.getParameter(JDBCClient.TABLE);

        if (table == null) table = sourceConfig.getParameter(JDBCClient.TABLE_NAME);
        if (catalog != null) table = catalog +"."+table;
        if (schema != null) table = schema +"."+table;

        return table;
    }

    public boolean isJoinSupported() {
        return true;
    }

    public String getSyncClassName() {
        return JDBCSourceSync.class.getName();
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Storage
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void create(Source source) throws Exception {

        boolean debug = log.isDebugEnabled();

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Create "+source.getName(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        client.createTable(source);
    }

    public void rename(Source oldSource, Source newSource) throws Exception {

        boolean debug = log.isDebugEnabled();

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Rename "+oldSource.getName()+" to "+newSource.getName(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        client.renameTable(oldSource, newSource);
    }

    public void drop(Source source) throws Exception {

        boolean debug = log.isDebugEnabled();

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Drop "+source.getName(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        client.dropTable(source);
    }

    public void clean(Source source) throws Exception {

        boolean debug = log.isDebugEnabled();

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Clean "+source.getName(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        client.cleanTable(source);
    }

    public void status(Source source) throws Exception {

        boolean debug = log.isDebugEnabled();

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Clean "+source.getName(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        client.showStatus(source);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Add
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void add(
            final Source source,
            final AddRequest request,
            final AddResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Add "+source.getName(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        InsertStatement statement = new InsertStatement();

        statement.setSource(source);

        RDN rdn = request.getDn().getRdn();

        if (rdn != null) {
            for (String name : rdn.getNames()) {

                Object value = rdn.get(name);

                Field field = source.getField(name);
                if (field == null) throw new Exception("Unknown field: " + name);

                statement.addAssignment(new Assignment(field, value));
            }
        }

        Attributes attributes = request.getAttributes();

        for (String name : attributes.getNames()) {
            if (rdn != null && rdn.contains(name)) continue;

            Object value = attributes.getValue(name); // get first value

            Field field = source.getField(name);
            if (field == null) throw new Exception("Unknown field: " + name);

            statement.addAssignment(new Assignment(field, value));
        }

        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.setStatement(statement);

        UpdateResponse updateResponse = new UpdateResponse();

        client.executeUpdate(updateRequest, updateResponse);

        log.debug("Add operation completed.");
    }

    public void add(
            EntryMapping entryMapping,
            Collection<SourceRef> sourceRefs,
            SourceValues sourceValues,
            AddRequest request,
            AddResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Add "+ sourceRefs, 80));
            log.debug(Formatter.displaySeparator(80));

            log.debug("Source values:");
            sourceValues.print();
        }

        AddRequestBuilder builder = new AddRequestBuilder(
                sourceRefs,
                sourceValues,
                penroseContext.getInterpreterManager().newInstance(),
                request,
                response
        );

        Collection<Request> requests = builder.generate();
        for (Request req : requests) {
            UpdateRequest updateRequest = (UpdateRequest) req;
            UpdateResponse updateResponse = new UpdateResponse();

            client.executeUpdate(updateRequest, updateResponse);
        }

        log.debug("Add operation completed.");
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Delete
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void delete(
            final Source source,
            final DeleteRequest request,
            final DeleteResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Delete "+source.getName(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        DeleteStatement statement = new DeleteStatement();

        statement.setSource(source);

        RDN rdn = request.getDn().getRdn();
        Filter filter = null;
        for (String name : rdn.getNames()) {
            Object value = rdn.get(name);

            SimpleFilter sf = new SimpleFilter(name, "=", value);
            filter = FilterTool.appendAndFilter(filter, sf);
        }

        statement.setFilter(filter);

        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.setStatement(statement);

        UpdateResponse updateResponse = new UpdateResponse();

        client.executeUpdate(updateRequest, updateResponse);

        log.debug("Delete operation completed.");
    }

    public void delete(
            EntryMapping entryMapping,
            Collection<SourceRef> sourceRefs,
            SourceValues sourceValues,
            DeleteRequest request,
            DeleteResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Delete "+ sourceRefs, 80));
            log.debug(Formatter.displaySeparator(80));

            log.debug("Source values:");
            sourceValues.print();
        }

        DeleteRequestBuilder builder = new DeleteRequestBuilder(
                sourceRefs,
                sourceValues,
                penroseContext.getInterpreterManager().newInstance(),
                request,
                response
        );

        Collection<Request> requests = builder.generate();
        for (Iterator i=requests.iterator(); i.hasNext(); ) {
            UpdateRequest updateRequest = (UpdateRequest)i.next();
            UpdateResponse updateResponse = new UpdateResponse();

            client.executeUpdate(updateRequest, updateResponse);
        }

        log.debug("Delete operation completed.");
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Modify
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modify(
            final Source source,
            final ModifyRequest request,
            final ModifyResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Modify "+source.getName(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        UpdateStatement statement = new UpdateStatement();

        statement.setSource(source);

        RDN rdn = request.getDn().getRdn();

        Collection<Modification> modifications = request.getModifications();
        for (Iterator i=modifications.iterator(); i.hasNext(); ) {
            Modification modification = (Modification)i.next();

            int type = modification.getType();
            Attribute attribute = modification.getAttribute();
            String name = attribute.getName();

            Field field = source.getField(name);
            if (field == null) continue;

            switch (type) {
                case Modification.ADD:
                case Modification.REPLACE:
                    Object value = rdn.get(name);
                    if (value == null) value = attribute.getValue();
                    statement.addAssignment(new Assignment(field, value));
                    break;

                case Modification.DELETE:
                    statement.addAssignment(new Assignment(field, null));
                    break;
            }
        }

        Filter filter = null;
        for (Iterator i=rdn.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Object value = rdn.get(name);

            SimpleFilter sf = new SimpleFilter(name, "=", value);
            filter = FilterTool.appendAndFilter(filter, sf);
        }

        statement.setFilter(filter);

        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.setStatement(statement);

        UpdateResponse updateResponse = new UpdateResponse();

        client.executeUpdate(updateRequest, updateResponse);

        log.debug("Modify operation completed.");
    }

    public void modify(
            EntryMapping entryMapping,
            Collection<SourceRef> sourceRefs,
            SourceValues sourceValues,
            ModifyRequest request,
            ModifyResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Modify "+ sourceRefs, 80));
            log.debug(Formatter.displaySeparator(80));

            log.debug("Source values:");
            sourceValues.print();
        }

        ModifyRequestBuilder builder = new ModifyRequestBuilder(
                sourceRefs,
                sourceValues,
                penroseContext.getInterpreterManager().newInstance(),
                request,
                response
        );

        Collection requests = builder.generate();
        for (Iterator i=requests.iterator(); i.hasNext(); ) {
            UpdateRequest updateRequest = (UpdateRequest)i.next();
            UpdateResponse updateResponse = new UpdateResponse();

            client.executeUpdate(updateRequest, updateResponse);
        }

        log.debug("Modify operation completed.");
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // ModRDN
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modrdn(
            final Source source,
            final ModRdnRequest request,
            final ModRdnResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("ModRdn "+source.getName(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        UpdateStatement statement = new UpdateStatement();

        statement.setSource(source);

        RDN newRdn = request.getNewRdn();
        for (Iterator i=newRdn.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Object value = newRdn.get(name);

            Field field = source.getField(name);
            if (field == null) continue;

            statement.addAssignment(new Assignment(field, value));
        }

        RDN rdn = request.getDn().getRdn();
        Filter filter = null;
        for (Iterator i=rdn.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Object value = rdn.get(name);

            SimpleFilter sf = new SimpleFilter(name, "=", value);
            filter = FilterTool.appendAndFilter(filter, sf);
        }

        statement.setFilter(filter);

        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.setStatement(statement);

        UpdateResponse updateResponse = new UpdateResponse();

        client.executeUpdate(updateRequest, updateResponse);

        log.debug("ModRdn operation completed.");
    }

    public void modrdn(
            EntryMapping entryMapping,
            Collection<SourceRef> sourceRefs,
            SourceValues sourceValues,
            ModRdnRequest request,
            ModRdnResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("ModRdn "+ sourceRefs, 80));
            log.debug(Formatter.displaySeparator(80));

            log.debug("Source values:");
            sourceValues.print();
        }

        ModRdnRequestBuilder builder = new ModRdnRequestBuilder(
                sourceRefs,
                sourceValues,
                penroseContext.getInterpreterManager().newInstance(),
                request,
                response
        );

        Collection requests = builder.generate();
        for (Iterator i=requests.iterator(); i.hasNext(); ) {
            UpdateRequest updateRequest = (UpdateRequest)i.next();
            UpdateResponse updateResponse = new UpdateResponse();

            client.executeUpdate(updateRequest, updateResponse);
        }

        log.debug("ModRdn operation completed.");
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Search
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void search(
            final Source source,
            final SearchRequest request,
            final SearchResponse<SearchResult> response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Search "+source.getName(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        response.setSizeLimit(request.getSizeLimit());

        SelectStatement statement = new SelectStatement();

        SourceRef sourceRef = new SourceRef(source);

        Filter filter = null;

        DN dn = request.getDn();
        if (dn != null) {
            RDN rdn = dn.getRdn();
            for (Iterator i=rdn.getNames().iterator(); i.hasNext(); ) {
                String name = (String)i.next();
                Object value = rdn.get(name);

                SimpleFilter sf = new SimpleFilter(name, "=", value);
                filter = FilterTool.appendAndFilter(filter, sf);
            }
        }

        filter = FilterTool.appendAndFilter(filter, request.getFilter());

        statement.addFieldRefs(sourceRef.getFieldRefs());
        statement.addSourceRef(sourceRef);
        statement.setFilter(filter);
        statement.setOrders(sourceRef.getPrimaryKeyFieldRefs());

        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setStatement(statement);

        QueryResponse queryResponse = new QueryResponse() {
            public void add(Object object) throws Exception {
                ResultSet rs = (ResultSet)object;
                SearchResult searchResult = createSearchResult(source, rs);
                response.add(searchResult);
            }
            public void close() throws Exception {
                response.close();
            }
        };

        client.executeQuery(queryRequest, queryResponse);

        log.debug("Search operation completed.");
    }

    public void search(
            final EntryMapping entryMapping,
            final Collection<SourceRef> sourceRefs,
            final SourceValues sourceValues,
            final SearchRequest request,
            final SearchResponse<SearchResult> response
    ) throws Exception {

        final boolean debug = log.isDebugEnabled();

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Search "+ sourceRefs, 80));
            log.debug(Formatter.displaySeparator(80));

            log.debug("Source values:");
            sourceValues.print();
        }

        response.setSizeLimit(request.getSizeLimit());

        SearchRequestBuilder builder = new SearchRequestBuilder(
                penroseContext,
                partition,
                entryMapping,
                sourceRefs,
                sourceValues,
                request,
                response
        );

        QueryRequest queryRequest = builder.generate();
        QueryResponse queryResponse = new QueryResponse() {

            SearchResult lastEntry;

            public void add(Object object) throws Exception {
                ResultSet rs = (ResultSet)object;

                SearchResult searchResult = createSearchResult(entryMapping, sourceRefs, rs);
                if (searchResult == null) return;

                if (lastEntry == null) {
                    lastEntry = searchResult;

                } else if (searchResult.getDn().equals(lastEntry.getDn())) {
                    mergeSearchResult(searchResult, lastEntry);

                } else {
                    response.add(lastEntry);
                    lastEntry = searchResult;
                }

                if (debug) {
                    searchResult.print();
                }
            }

            public void close() throws Exception {
                if (lastEntry != null) {
                    response.add(lastEntry);
                }
                response.close();
            }
        };

        client.executeQuery(queryRequest, queryResponse);

        log.debug("Search operation completed.");
    }

    public SearchResult createSearchResult(
            Source source,
            ResultSet rs
    ) throws Exception {

        Attributes attributes = new Attributes();
        RDNBuilder rb = new RDNBuilder();

        int column = 1;
        for (Iterator i= source.getFields().iterator(); i.hasNext(); column++) {
            Field field = (Field)i.next();

            Object value = rs.getObject(column);
            if (value == null) continue;

            String fieldName = field.getName();
            attributes.addValue(fieldName, value);

            if (field.isPrimaryKey()) rb.set(fieldName, value);
        }

        DN dn = new DN(rb.toRdn());

        return new SearchResult(dn, attributes);
    }

    public SearchResult createSearchResult(
            EntryMapping entryMapping,
            Collection<SourceRef> sourceRefs,
            ResultSet rs
    ) throws Exception {

        SearchResult searchResult = new SearchResult();
        searchResult.setEntryMapping(entryMapping);

        RDNBuilder rb = new RDNBuilder();

        SourceManager sourceManager = penroseContext.getSourceManager();
        Collection<SourceRef> primarySourceRefs = sourceManager.getPrimarySourceRefs(partition.getName(), entryMapping);

        int column = 1;

        for (SourceRef sourceRef : sourceRefs) {
            String alias = sourceRef.getAlias();
            boolean primarySource = primarySourceRefs.contains(sourceRef);

            Attributes sourceValues = new Attributes();

            for (Iterator j = sourceRef.getFieldRefs().iterator(); j.hasNext(); column++) {
                FieldRef fieldRef = (FieldRef) j.next();

                Object value = rs.getObject(column);

                String fieldName = fieldRef.getName();
                String name = alias + "." + fieldName;

                if (primarySource && fieldRef.isPrimaryKey()) {
                    if (value == null) return null;
                    rb.set(name, value);
                    sourceValues.addValue("primaryKey." + fieldName, value);
                }

                if (value == null) continue;
                sourceValues.addValue(fieldName, value);
            }

            searchResult.setSourceValues(alias, sourceValues);
        }

        searchResult.setDn(new DN(rb.toRdn()));

        return searchResult;
    }

    public void mergeSearchResult(SearchResult source, SearchResult destination) {
        SourceValues sourceValues = source.getSourceValues();
        SourceValues destinationValues = destination.getSourceValues();

        destinationValues.add(sourceValues);
    }

    public Filter convert(EntryMapping entryMapping, SubstringFilter filter) throws Exception {

        String attributeName = filter.getAttribute();
        Collection<Object> substrings = filter.getSubstrings();

        AttributeMapping attributeMapping = entryMapping.getAttributeMapping(attributeName);
        String variable = attributeMapping.getVariable();

        if (variable == null) return null;

        int index = variable.indexOf(".");
        String sourceName = variable.substring(0, index);
        String fieldName = variable.substring(index+1);

        StringBuilder sb = new StringBuilder();
        for (Iterator i=substrings.iterator(); i.hasNext(); ) {
            Object o = i.next();
            if (o.equals(SubstringFilter.STAR)) {
                sb.append("%");
            } else {
                String substring = (String)o;
                sb.append(substring);
            }
        }

        return new SimpleFilter(fieldName, "like", sb.toString());
    }

    public JDBCClient getClient() {
        return client;
    }

    public void setClient(JDBCClient client) {
        this.client = client;
    }
}
