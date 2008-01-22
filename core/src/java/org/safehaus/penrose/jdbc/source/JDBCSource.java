package org.safehaus.penrose.jdbc.source;

import org.safehaus.penrose.source.*;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.directory.SourceRef;
import org.safehaus.penrose.directory.FieldRef;
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.jdbc.*;
import org.safehaus.penrose.jdbc.connection.*;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.filter.SimpleFilter;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.FilterTool;

import java.util.Collection;
import java.sql.ResultSet;

/**
 * @author Endi S. Dewata
 */
public class JDBCSource extends Source {

    public final static String BASE_DN      = "baseDn";
    public final static String CATALOG      = "catalog";
    public final static String SCHEMA       = "schema";
    public final static String TABLE        = "table";
    public final static String FILTER       = "filter";
    public final static String SIZE_LIMIT   = "sizeLimit";

    public final static String AUTHENTICATION          = "authentication";
    public final static String AUTHENTICATION_DEFAULT  = "default";
    public final static String AUTHENTICATION_FULL     = "full";
    public final static String AUTHENTICATION_DISABLED = "disabled";

    JDBCConnection connection;

    public JDBCSource() {
    }

    public void init() throws Exception {
        connection = (JDBCConnection)getConnection();

        boolean create = Boolean.parseBoolean(getParameter(JDBCClient.CREATE));
        if (create) {
            try {
                create();
            } catch (Exception e) {
                log.error(e.getMessage());
            }
        }
    }

    public JDBCClient createClient(Session session) throws Exception {

        if (debug) log.debug("Creating JDBC client.");
        JDBCClient client = connection.getClient();

        if (debug) log.debug("Storing JDBC client in session.");
        if (session != null) session.setAttribute(getPartition().getName()+".connection."+connection.getName(), client);

        return client;
    }

    public JDBCClient getClient(Session session) throws Exception {

        if (session == null) return createClient(session);

        if (debug) log.debug("Getting LDAP client from session.");
        JDBCClient client = (JDBCClient)session.getAttribute(getPartition().getName()+".connection."+connection.getName());
        if (client != null) return client;

        return createClient(session);
    }


    public void closeClient(Session session) throws Exception {
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Add
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void add(
            final Session session,
            final AddRequest request,
            final AddResponse response
    ) throws Exception {

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Add "+getName(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        JDBCClient client = getClient(session);

        try {
            InsertStatement statement = new InsertStatement();
            statement.setSource(this);

            RDN rdn = request.getDn().getRdn();

            if (rdn != null) {
                for (String name : rdn.getNames()) {

                    Object value = rdn.get(name);

                    Field field = getField(name);
                    if (field == null) throw new Exception("Unknown field: " + name);

                    statement.addAssignment(new Assignment(field, value));
                }
            }

            Attributes attributes = request.getAttributes();

            for (String name : attributes.getNames()) {
                if (rdn != null && rdn.contains(name)) continue;

                Object value = attributes.getValue(name); // get first value

                Field field = getField(name);
                if (field == null) throw new Exception("Unknown field: " + name);

                statement.addAssignment(new Assignment(field, value));
            }

            UpdateRequest updateRequest = new UpdateRequest();
            updateRequest.setStatement(statement);

            UpdateResponse updateResponse = new UpdateResponse();

            client.executeUpdate(updateRequest, updateResponse);

            log.debug("Add operation completed.");

        } finally {
            closeClient(session);
        }
    }

    public void add(
            Session session,
            Collection<SourceRef> sourceRefs,
            SourceValues sourceValues,
            AddRequest request,
            AddResponse response
    ) throws Exception {

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Add "+ sourceRefs, 80));
            log.debug(Formatter.displaySeparator(80));

            log.debug("Source values:");
            sourceValues.print();
        }

        Interpreter interpreter = getPartition().newInterpreter();

        AddRequestBuilder builder = new AddRequestBuilder(
                sourceRefs,
                sourceValues,
                interpreter,
                request,
                response
        );

        Collection<org.safehaus.penrose.jdbc.Request> requests = builder.generate();

        JDBCClient client = getClient(session);

        for (org.safehaus.penrose.jdbc.Request req : requests) {
            UpdateRequest updateRequest = (UpdateRequest) req;
            UpdateResponse updateResponse = new UpdateResponse();

            client.executeUpdate(updateRequest, updateResponse);
        }

        log.debug("Add operation completed.");
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Compare
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void compare(
            final Session session,
            final CompareRequest request,
            final CompareResponse response
    ) throws Exception {

        SearchRequest newRequest = new SearchRequest();
        newRequest.setDn(request.getDn());
        newRequest.setScope(SearchRequest.SCOPE_BASE);

        SimpleFilter filter = new SimpleFilter(request.getAttributeName(), "=", request.getAttributeValue());
        newRequest.setFilter(filter);

        SearchResponse newResponse = new SearchResponse();

        search(session, newRequest, newResponse);

        boolean result = newResponse.hasNext();

        if (debug) log.debug("Compare operation completed ["+result+"].");
        response.setReturnCode(result ? LDAP.COMPARE_TRUE : LDAP.COMPARE_FALSE);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Delete
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void delete(
            final Session session,
            final DeleteRequest request,
            final DeleteResponse response
    ) throws Exception {

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Delete "+getName(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        JDBCClient client = getClient(session);

        try {
            DeleteStatement statement = new DeleteStatement();

            SourceRef sourceRef = new SourceRef(this);
            statement.setSourceRef(sourceRef);

            Filter filter = null;

            RDN rdn = request.getDn().getRdn();
            if (rdn != null) {
                for (String name : rdn.getNames()) {
                    Object value = rdn.get(name);

                    SimpleFilter sf = new SimpleFilter(name, "=", value);
                    filter = FilterTool.appendAndFilter(filter, sf);
                }
            }

            statement.setFilter(filter);

            UpdateRequest updateRequest = new UpdateRequest();
            updateRequest.setStatement(statement);

            UpdateResponse updateResponse = new UpdateResponse();

            client.executeUpdate(updateRequest, updateResponse);

            log.debug("Delete operation completed.");

        } finally {
            closeClient(session);
        }
    }

    public void delete(
            Session session,
            Collection<SourceRef> sourceRefs,
            SourceValues sourceValues,
            DeleteRequest request,
            DeleteResponse response
    ) throws Exception {

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Delete "+ sourceRefs, 80));
            log.debug(Formatter.displaySeparator(80));

            log.debug("Source values:");
            sourceValues.print();
        }

        Interpreter interpreter = getPartition().newInterpreter();

        DeleteRequestBuilder builder = new DeleteRequestBuilder(
                sourceRefs,
                sourceValues,
                interpreter,
                request,
                response
        );

        Collection<org.safehaus.penrose.jdbc.Request> requests = builder.generate();

        JDBCClient client = getClient(session);

        for (org.safehaus.penrose.jdbc.Request req : requests) {
            UpdateRequest updateRequest = (UpdateRequest) req;
            UpdateResponse updateResponse = new UpdateResponse();

            client.executeUpdate(updateRequest, updateResponse);
        }

        log.debug("Delete operation completed.");
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Modify
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modify(
            final Session session,
            final ModifyRequest request,
            final ModifyResponse response
    ) throws Exception {

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Modify "+getName(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        JDBCClient client = getClient(session);

        try {
            UpdateStatement statement = new UpdateStatement();

            SourceRef sourceRef = new SourceRef(this);
            statement.setSourceRef(sourceRef);

            RDN rdn = request.getDn().getRdn();

            Collection<Modification> modifications = request.getModifications();
            for (Modification modification : modifications) {

                int type = modification.getType();
                Attribute attribute = modification.getAttribute();
                String name = attribute.getName();

                Field field = getField(name);
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

            log.debug("Modify operation completed.");

        } finally {
            closeClient(session);
        }
    }

    public void modify(
            Session session,
            Collection<SourceRef> sourceRefs,
            SourceValues sourceValues,
            ModifyRequest request,
            ModifyResponse response
    ) throws Exception {

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Modify "+ sourceRefs, 80));
            log.debug(Formatter.displaySeparator(80));

            log.debug("Source values:");
            sourceValues.print();
        }

        Interpreter interpreter = getPartition().newInterpreter();

        ModifyRequestBuilder builder = new ModifyRequestBuilder(
                sourceRefs,
                sourceValues,
                interpreter,
                request,
                response
        );

        Collection<org.safehaus.penrose.jdbc.Request> requests = builder.generate();

        JDBCClient client = getClient(session);

        for (org.safehaus.penrose.jdbc.Request req : requests) {
            UpdateRequest updateRequest = (UpdateRequest) req;
            UpdateResponse updateResponse = new UpdateResponse();

            client.executeUpdate(updateRequest, updateResponse);
        }

        log.debug("Modify operation completed.");
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // ModRdn
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modrdn(
            final Session session,
            final ModRdnRequest request,
            final ModRdnResponse response
    ) throws Exception {

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("ModRdn "+getName(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        JDBCClient client = getClient(session);

        try {
            UpdateStatement statement = new UpdateStatement();

            SourceRef sourceRef = new SourceRef(this);
            statement.setSourceRef(sourceRef);

            RDN newRdn = request.getNewRdn();
            for (String name : newRdn.getNames()) {
                Object value = newRdn.get(name);

                Field field = getField(name);
                if (field == null) continue;

                statement.addAssignment(new Assignment(field, value));
            }

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

            log.debug("ModRdn operation completed.");

        } finally {
            closeClient(session);
        }
    }

    public void modrdn(
            Session session,
            Collection<SourceRef> sourceRefs,
            SourceValues sourceValues,
            ModRdnRequest request,
            ModRdnResponse response
    ) throws Exception {

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("ModRdn "+ sourceRefs, 80));
            log.debug(Formatter.displaySeparator(80));

            log.debug("Source values:");
            sourceValues.print();
        }

        Interpreter interpreter = getPartition().newInterpreter();

        ModRdnRequestBuilder builder = new ModRdnRequestBuilder(
                sourceRefs,
                sourceValues,
                interpreter,
                request,
                response
        );

        Collection<org.safehaus.penrose.jdbc.Request> requests = builder.generate();

        JDBCClient client = getClient(session);

        for (org.safehaus.penrose.jdbc.Request req : requests) {
            UpdateRequest updateRequest = (UpdateRequest) req;
            UpdateResponse updateResponse = new UpdateResponse();

            client.executeUpdate(updateRequest, updateResponse);
        }

        log.debug("ModRdn operation completed.");
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Search
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void search(
            final Session session,
            final SearchRequest request,
            final SearchResponse response
    ) throws Exception {

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Search "+getName(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        JDBCClient client = getClient(session);

        try {
            response.setSizeLimit(request.getSizeLimit());

            SelectStatement statement = new SelectStatement();

            SourceRef sourceRef = new SourceRef(this);

            Filter filter = null;

            DN dn = request.getDn();
            if (dn != null) {
                RDN rdn = dn.getRdn();
                for (String name : rdn.getNames()) {
                    Object value = rdn.get(name);

                    SimpleFilter sf = new SimpleFilter(name, "=", value);
                    filter = FilterTool.appendAndFilter(filter, sf);
                }
            }

            filter = FilterTool.appendAndFilter(filter, request.getFilter());

            statement.addFieldRefs(sourceRef.getFieldRefs());
            statement.addSourceRef(sourceRef);
            statement.setFilter(filter);

            String where = getParameter(FILTER);
            if (where != null) {
                statement.setWhere(where);
            }

            statement.setOrders(sourceRef.getPrimaryKeyFieldRefs());

            QueryRequest queryRequest = new QueryRequest();
            queryRequest.setStatement(statement);

            QueryResponse queryResponse = new QueryResponse() {
                public void add(Object object) throws Exception {
                    ResultSet rs = (ResultSet)object;

                    if (sizeLimit > 0 && totalCount >= sizeLimit) {
                        throw LDAP.createException(LDAP.SIZE_LIMIT_EXCEEDED);
                    }

                    SearchResult searchResult = createSearchResult(rs);
                    response.add(searchResult);

                    totalCount++;
                }
                public void close() throws Exception {
                    response.close();
                    super.close();
                }
            };

            String sizeLimit = getParameter(SIZE_LIMIT);
            if (sizeLimit != null) {
                queryResponse.setSizeLimit(Long.parseLong(sizeLimit));
            }

            client.executeQuery(queryRequest, queryResponse);

            log.debug("Search operation completed.");

        } finally {
            closeClient(session);
        }
    }

    public SearchResult createSearchResult(
            ResultSet rs
    ) throws Exception {

        Attributes attributes = new Attributes();
        RDNBuilder rb = new RDNBuilder();

        int column = 1;

        for (Field field : getFields()) {

            Object value = rs.getObject(column++);
            if (value == null) continue;

            String fieldName = field.getName();
            attributes.addValue(fieldName, value);

            if (field.isPrimaryKey()) rb.set(fieldName, value);
        }

        DNBuilder db = new DNBuilder();
        db.append(rb.toRdn());

        String baseDn = getParameter(BASE_DN);
        if (baseDn != null) {
            db.append(baseDn);
        }

        DN dn = db.toDn();

        return new SearchResult(dn, attributes);
    }

    public void search(
            final Session session,
            //final Collection<SourceRef> primarySourceRefs,
            final Collection<SourceRef> localSourceRefs,
            final Collection<SourceRef> sourceRefs,
            final SourceValues sourceValues,
            final SearchRequest request,
            final SearchResponse response
    ) throws Exception {

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Search "+ sourceRefs, 80));
            log.debug(Formatter.displaySeparator(80));

            log.debug("Source values:");
            sourceValues.print();
        }

        //Interpreter interpreter = partition.newInterpreter();

        response.setSizeLimit(request.getSizeLimit());

        SearchRequestBuilder builder = new SearchRequestBuilder(
                //interpreter,
                getPartition(),
                //primarySourceRefs,
                localSourceRefs,
                sourceRefs,
                sourceValues,
                request,
                response
        );

        QueryRequest queryRequest = builder.generate();
        QueryResponse queryResponse = new QueryResponse() {

            SearchResult lastResult;

            public void add(Object object) throws Exception {
                ResultSet rs = (ResultSet)object;

                if (sizeLimit > 0 && totalCount >= sizeLimit) {
                    throw LDAP.createException(LDAP.SIZE_LIMIT_EXCEEDED);
                }

                //SearchResult searchResult = createSearchResult(primarySourceRefs, sourceRefs, rs);
                SearchResult searchResult = createSearchResult(sourceRefs, rs);
                if (searchResult == null) return;

                if (lastResult == null) {
                    lastResult = searchResult;

                } else if (searchResult.getDn().equals(lastResult.getDn())) {
                    mergeSearchResult(searchResult, lastResult);

                } else {
                    response.add(lastResult);
                    lastResult = searchResult;
                }

                totalCount++;

                if (debug) {
                    searchResult.print();
                }
            }

            public void close() throws Exception {
                if (lastResult != null) {
                    response.add(lastResult);
                }
                response.close();
                super.close();
            }
        };

        String sizeLimit = getParameter(JDBCConnection.SIZE_LIMIT);

        if (sizeLimit != null) {
            queryResponse.setSizeLimit(Long.parseLong(sizeLimit));
        }

        JDBCClient client = getClient(session);

        client.executeQuery(queryRequest, queryResponse);

        log.debug("Search operation completed.");
    }

    public SearchResult createSearchResult(
            //Collection<SourceRef> primarySourceRefs,
            Collection<SourceRef> sourceRefs,
            ResultSet rs
    ) throws Exception {

        SearchResult searchResult = new SearchResult();

        SourceValues sourceValues = new SourceValues();
        RDNBuilder rb = new RDNBuilder();

        int column = 1;

        for (SourceRef sourceRef : sourceRefs) {
            String alias = sourceRef.getAlias();
            //boolean primarySource = primarySourceRefs.contains(sourceRef);

            Attributes fields = new Attributes();

            for (FieldRef fieldRef : sourceRef.getFieldRefs()) {

                Object value = rs.getObject(column++);

                String fieldName = fieldRef.getName();
                String name = alias + "." + fieldName;

                //if (primarySource && fieldRef.isPrimaryKey()) {
                if (sourceRef.isPrimarySourceRef() && fieldRef.isPrimaryKey()) {
                    if (value == null) return null;
                    rb.set(name, value);
                }

                if (value == null) continue;
                fields.addValue(fieldName, value);
            }

            sourceValues.set(alias, fields);
        }

        searchResult.setSourceValues(sourceValues);
        searchResult.setDn(new DN(rb.toRdn()));

        return searchResult;
    }

    public void mergeSearchResult(SearchResult source, SearchResult destination) {
        SourceValues sourceValues = source.getSourceValues();
        SourceValues destinationValues = destination.getSourceValues();

        destinationValues.add(sourceValues);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Storage
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void create() throws Exception {

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Create "+sourceConfig.getName(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        JDBCClient client = connection.getClient();
        client.createTable(sourceConfig);
    }

    public void rename(Source newSource) throws Exception {

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Rename "+sourceConfig.getName()+" to "+newSource.getName(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        JDBCClient client = connection.getClient();
        client.renameTable(sourceConfig, newSource.getSourceConfig());
    }

    public void drop() throws Exception {

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Drop "+sourceConfig.getName(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        JDBCClient client = connection.getClient();
        client.dropTable(sourceConfig);
    }

    public void clear() throws Exception {

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Clear "+sourceConfig.getName(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        JDBCClient client = connection.getClient();
        client.cleanTable(sourceConfig);
    }

    public void status() throws Exception {

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Status "+sourceConfig.getName(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        JDBCClient client = connection.getClient();
        client.showStatus(sourceConfig);
    }

    public long getCount() throws Exception {

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Count "+sourceConfig.getName(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        JDBCClient client = connection.getClient();
        return client.getCount(sourceConfig);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Clone
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public Object clone() throws CloneNotSupportedException {

        JDBCSource source = (JDBCSource)super.clone();

        source.connection       = connection;

        return source;
    }
}
