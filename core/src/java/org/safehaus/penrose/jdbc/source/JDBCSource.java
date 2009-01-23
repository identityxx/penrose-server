package org.safehaus.penrose.jdbc.source;

import org.safehaus.penrose.source.*;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.directory.EntrySource;
import org.safehaus.penrose.directory.EntryField;
import org.safehaus.penrose.util.TextUtil;
import org.safehaus.penrose.jdbc.*;
import org.safehaus.penrose.jdbc.connection.*;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.filter.SimpleFilter;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.FilterTool;

import java.util.*;
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
    public final static String CREATE       = "create";

    public final static String AUTHENTICATION          = "authentication";
    public final static String AUTHENTICATION_DEFAULT  = "default";
    public final static String AUTHENTICATION_FULL     = "full";
    public final static String AUTHENTICATION_DISABLED = "disabled";

    public final static String ADD          = "add";
    public final static String BIND         = "bind";
    public final static String COMPARE      = "compare";
    public final static String DELETE       = "delete";
    public final static String MODIFY       = "modify";
    public final static String MODRDN       = "modrdn";
    public final static String SEARCH       = "search";
    public final static String UNBIND       = "unbind";

    JDBCConnection connection;

    String table;
    String filter;

    String sourceBaseDn;

    Map<String,Map<Collection<String>,SQLOperation>> operations = new LinkedHashMap<String,Map<Collection<String>,SQLOperation>>();

    public JDBCSource() {
    }

    public void init() throws Exception {
        connection = (JDBCConnection)getConnection();

        table = getParameter(TABLE);
        filter = getParameter(FILTER);
        
        sourceBaseDn = getParameter(BASE_DN);

        for (String name : getParameterNames()) {
            int i = name.indexOf('(');
            if (i < 0) continue;

            if (debug) log.debug("Operation "+name+":");

            String action = name.substring(0, i);
            String params = name.substring(i+1, name.length()-1);

            Collection<String> parameters = new LinkedHashSet<String>();
            for (String param : params.split(",")) {
                parameters.add(param.trim());
            }

            String statement = getParameter(name);

            if (debug) {
                log.debug(" - action    : "+ action);
                log.debug(" - parameters: "+parameters);
                log.debug(" - statement : "+statement);
            }

            SQLOperation stmt = new SQLOperation();
            stmt.setAction(action);
            stmt.setParameters(parameters);
            stmt.setStatement(statement);

            Map<Collection<String>,SQLOperation> stmts = operations.get(action);
            if (stmts == null) {
                stmts = new LinkedHashMap<Collection<String>,SQLOperation>();
                operations.put(action, stmts);
            }

            stmts.put(parameters, stmt);
        }

        boolean create = Boolean.parseBoolean(getParameter(CREATE));
        if (create) {
            try {
                create();
            } catch (Exception e) {
                log.error(e.getMessage());
            }
        }
    }

    public String getTable() {
        return table;
    }

    public String getFilter() {
        return filter;
    }

    public SQLOperation getOperation(String operation, Collection<String> parameterNames) {

        Map<Collection<String>,SQLOperation> stmts = operations.get(operation);
        if (stmts == null) return null;

        if (LinkedHashSet.class.equals(parameterNames.getClass())) {
            Collection<String> set = new LinkedHashSet<String>();
            set.addAll(parameterNames);
            parameterNames = set;
        }

        for (Collection<String> key : stmts.keySet()) {
            if (!parameterNames.containsAll(key)) continue;

            return stmts.get(key);
        }

        return null;
    }

    public Collection<Object> getParameters(SQLOperation stmt, Map<String,Object> values) {

        Collection<Object> parameters = new ArrayList<Object>();

        for (String name : stmt.getParameters()) {
            Object value = values.get(name);
            parameters.add(value);
        }

        return parameters;
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
            log.debug(TextUtil.displaySeparator(80));
            log.debug(TextUtil.displayLine("Add "+getName(), 80));
            log.debug(TextUtil.displaySeparator(80));
        }

        JDBCClient client = connection.getClient(session);

        try {
            InsertStatement statement = new InsertStatement();
            statement.setSource(partition.getName(), getName());

            RDN rdn = request.getDn().getRdn();

            if (rdn != null) {
                for (String name : rdn.getNames()) {

                    Object value = rdn.get(name);

                    Field field = getField(name);
                    if (field == null) throw new Exception("Unknown field: " + name);

                    statement.addAssignment(new Assignment(field.getOriginalName(), value));
                }
            }

            Attributes attributes = request.getAttributes();

            for (String name : attributes.getNames()) {
                if (rdn != null && rdn.contains(name)) continue;

                Object value = attributes.getValue(name); // get first value

                Field field = getField(name);
                if (field == null) throw new Exception("Unknown field: " + name);

                statement.addAssignment(new Assignment(field.getOriginalName(), value));
            }

            JDBCStatementBuilder statementBuilder = new JDBCStatementBuilder(sourceContext.getPartition());
            statementBuilder.setQuote(client.getQuote());

            String sql = statementBuilder.generate(statement);
            Collection<Object> parameters = statementBuilder.getParameters();

            client.executeUpdate(sql, parameters);

            log.debug("Add operation completed.");

        } finally {
            connection.closeClient(session);
        }
    }

    public void add(
            Session session,
            Collection<EntrySource> sourceRefs,
            SourceAttributes sourceValues,
            AddRequest request,
            AddResponse response
    ) throws Exception {

        if (debug) {
            log.debug(TextUtil.displaySeparator(80));
            log.debug(TextUtil.displayLine("Add "+ sourceRefs, 80));
            log.debug(TextUtil.displaySeparator(80));

            log.debug("Source values:");
            sourceValues.print();
        }

        Interpreter interpreter = partition.newInterpreter();

        AddRequestBuilder builder = new AddRequestBuilder(
                sourceRefs,
                sourceValues,
                interpreter,
                request,
                response
        );

        Collection<Statement> statements = builder.generate();

        JDBCClient client = connection.getClient(session);

        try {
            for (Statement statement : statements) {

                JDBCStatementBuilder statementBuilder = new JDBCStatementBuilder(sourceContext.getPartition());
                statementBuilder.setQuote(client.getQuote());

                String sql = statementBuilder.generate(statement);
                Collection<Object> parameters = statementBuilder.getParameters();

                client.executeUpdate(sql, parameters);
            }

        } finally {
            connection.closeClient(session);
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
            log.debug(TextUtil.displaySeparator(80));
            log.debug(TextUtil.displayLine("Delete "+getName(), 80));
            log.debug(TextUtil.displaySeparator(80));
        }

        JDBCClient client = connection.getClient(session);

        try {
            DeleteStatement statement = new DeleteStatement();

            statement.setSource(partition.getName(), getName());

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

            JDBCStatementBuilder statementBuilder = new JDBCStatementBuilder(sourceContext.getPartition());
            statementBuilder.setQuote(client.getQuote());

            String sql = statementBuilder.generate(statement);
            Collection<Object> parameters = statementBuilder.getParameters();

            client.executeUpdate(sql, parameters);

            log.debug("Delete operation completed.");

        } finally {
            connection.closeClient(session);
        }
    }

    public void delete(
            Session session,
            Collection<EntrySource> sourceRefs,
            SourceAttributes sourceValues,
            DeleteRequest request,
            DeleteResponse response
    ) throws Exception {

        if (debug) {
            log.debug(TextUtil.displaySeparator(80));
            log.debug(TextUtil.displayLine("Delete "+ sourceRefs, 80));
            log.debug(TextUtil.displaySeparator(80));

            log.debug("Source values:");
            sourceValues.print();
        }

        Interpreter interpreter = partition.newInterpreter();

        DeleteRequestBuilder builder = new DeleteRequestBuilder(
                sourceRefs,
                sourceValues,
                interpreter,
                request,
                response
        );

        Collection<Statement> statements = builder.generate();

        JDBCClient client = connection.getClient(session);

        try {
            for (Statement statement : statements) {

                JDBCStatementBuilder statementBuilder = new JDBCStatementBuilder(sourceContext.getPartition());
                statementBuilder.setQuote(client.getQuote());

                String sql = statementBuilder.generate(statement);
                Collection<Object> parameters = statementBuilder.getParameters();

                client.executeUpdate(sql, parameters);
            }

        } finally {
            connection.closeClient(session);
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
            log.debug(TextUtil.displaySeparator(80));
            log.debug(TextUtil.displayLine("Modify "+getName(), 80));
            log.debug(TextUtil.displaySeparator(80));
        }

        JDBCClient client = connection.getClient(session);

        try {
            UpdateStatement statement = new UpdateStatement();

            statement.setSource(partition.getName(), getName());

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
                        statement.addAssignment(new Assignment(field.getOriginalName(), value));
                        break;

                    case Modification.DELETE:
                        statement.addAssignment(new Assignment(field.getOriginalName(), null));
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

            JDBCStatementBuilder statementBuilder = new JDBCStatementBuilder(sourceContext.getPartition());
            statementBuilder.setQuote(client.getQuote());

            String sql = statementBuilder.generate(statement);
            Collection<Object> parameters = statementBuilder.getParameters();

            client.executeUpdate(sql, parameters);

            log.debug("Modify operation completed.");

        } finally {
            connection.closeClient(session);
        }
    }

    public void modify(
            Session session,
            Collection<EntrySource> sourceRefs,
            SourceAttributes sourceValues,
            ModifyRequest request,
            ModifyResponse response
    ) throws Exception {

        if (debug) {
            log.debug(TextUtil.displaySeparator(80));
            log.debug(TextUtil.displayLine("Modify "+ sourceRefs, 80));
            log.debug(TextUtil.displaySeparator(80));

            log.debug("Source values:");
            sourceValues.print();
        }

        Interpreter interpreter = partition.newInterpreter();

        ModifyRequestBuilder builder = new ModifyRequestBuilder(
                sourceRefs,
                sourceValues,
                interpreter,
                request,
                response
        );

        Collection<Statement> statements = builder.generate();

        JDBCClient client = connection.getClient(session);

        try {
            for (Statement statement : statements) {

                JDBCStatementBuilder statementBuilder = new JDBCStatementBuilder(sourceContext.getPartition());
                statementBuilder.setQuote(client.getQuote());

                String sql = statementBuilder.generate(statement);
                Collection<Object> parameters = statementBuilder.getParameters();

                client.executeUpdate(sql, parameters);
            }

        } finally {
            connection.closeClient(session);
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
            log.debug(TextUtil.displaySeparator(80));
            log.debug(TextUtil.displayLine("ModRdn "+getName(), 80));
            log.debug(TextUtil.displaySeparator(80));
        }

        JDBCClient client = connection.getClient(session);

        try {
            UpdateStatement statement = new UpdateStatement();

            statement.setSource(partition.getName(), getName());

            RDN newRdn = request.getNewRdn();
            for (String name : newRdn.getNames()) {
                Object value = newRdn.get(name);

                Field field = getField(name);
                if (field == null) continue;

                statement.addAssignment(new Assignment(field.getOriginalName(), value));
            }

            RDN rdn = request.getDn().getRdn();
            Filter filter = null;
            for (String name : rdn.getNames()) {
                Object value = rdn.get(name);

                SimpleFilter sf = new SimpleFilter(name, "=", value);
                filter = FilterTool.appendAndFilter(filter, sf);
            }

            statement.setFilter(filter);

            JDBCStatementBuilder statementBuilder = new JDBCStatementBuilder(sourceContext.getPartition());
            statementBuilder.setQuote(client.getQuote());

            String sql = statementBuilder.generate(statement);
            Collection<Object> parameters = statementBuilder.getParameters();

            client.executeUpdate(sql, parameters);

            log.debug("ModRdn operation completed.");

        } finally {
            connection.closeClient(session);
        }
    }

    public void modrdn(
            Session session,
            Collection<EntrySource> sourceRefs,
            SourceAttributes sourceValues,
            ModRdnRequest request,
            ModRdnResponse response
    ) throws Exception {

        if (debug) {
            log.debug(TextUtil.displaySeparator(80));
            log.debug(TextUtil.displayLine("ModRdn "+ sourceRefs, 80));
            log.debug(TextUtil.displaySeparator(80));

            log.debug("Source values:");
            sourceValues.print();
        }

        Interpreter interpreter = partition.newInterpreter();

        ModRdnRequestBuilder builder = new ModRdnRequestBuilder(
                sourceRefs,
                sourceValues,
                interpreter,
                request,
                response
        );

        Collection<Statement> statements = builder.generate();

        JDBCClient client = connection.getClient(session);

        try {
            for (Statement statement : statements) {

                JDBCStatementBuilder statementBuilder = new JDBCStatementBuilder(sourceContext.getPartition());
                statementBuilder.setQuote(client.getQuote());

                String sql = statementBuilder.generate(statement);
                Collection<Object> parameters = statementBuilder.getParameters();

                client.executeUpdate(sql, parameters);
            }

        } finally {
            connection.closeClient(session);
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
            log.debug(TextUtil.displaySeparator(80));
            log.debug(TextUtil.displayLine("Search "+getName(), 80));
            log.debug(TextUtil.displaySeparator(80));
        }

        DN baseDn = request.getDn();
        int scope = request.getScope();

        response.setSizeLimit(request.getSizeLimit());

        SelectStatement statement = new SelectStatement();

        //EntrySource entrySource = new EntrySource(this);

        Filter filter = null;

        DN dn = request.getDn();
        if (dn != null) {
            RDN rdn = dn.getRdn();
            if (rdn != null) {
                for (String name : rdn.getNames()) {
                    Object value = rdn.get(name);

                    SimpleFilter sf = new SimpleFilter(name, "=", value);
                    filter = FilterTool.appendAndFilter(filter, sf);
                }
            }
        }

        filter = FilterTool.appendAndFilter(filter, request.getFilter());

        for (Field field : getFields()) {
            statement.addColumn(getName()+"."+field.getOriginalName());
        }
        //for (EntryField field : entrySource.getFields()) {
        //    statement.addColumn(field.getSourceName()+"."+field.getOriginalName());
        //}
        statement.addSource(getName(), getPartition().getName(), getName());
        //statement.addSource(entrySource.getAlias(), entrySource.getSource().getPartition().getName(), entrySource.getSource().getName());
        statement.setFilter(filter);

        String where = getParameter(FILTER);
        if (where != null) {
            statement.setWhereClause(where);
        }

        for (Field field : getPrimaryKeyFields()) {
            statement.addColumn(getName()+"."+field.getOriginalName());
        }
        //for (EntryField field : entrySource.getPrimaryKeyFields()) {
        //    statement.addOrder(field.getSourceName()+"."+field.getOriginalName());
        //}

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
        };

        String sizeLimit = getParameter(SIZE_LIMIT);
        if (sizeLimit != null) {
            queryResponse.setSizeLimit(Long.parseLong(sizeLimit));
        }

        JDBCClient client = connection.getClient(session);

        JDBCStatementBuilder statementBuilder = new JDBCStatementBuilder(sourceContext.getPartition());
        statementBuilder.setQuote(client.getQuote());

        String sql = statementBuilder.generate(statement);
        Collection<Object> parameters = statementBuilder.getParameters();

        try {

            if (baseDn != null && baseDn.isEmpty()) {

                if (scope == SearchRequest.SCOPE_BASE || scope == SearchRequest.SCOPE_SUB) {

                    if (debug) log.debug("Searching root entry.");

                    SearchResult result = new SearchResult();
                    response.add(result);
                }

                if (scope == SearchRequest.SCOPE_ONE || scope == SearchRequest.SCOPE_SUB) {

                    if (debug) log.debug("Searching top entries.");

                    client.executeQuery(sql, parameters, queryResponse);
                }

            } else if (baseDn != null && (scope == SearchRequest.SCOPE_BASE || scope == SearchRequest.SCOPE_SUB)) {

                if (debug) log.debug("Searching entry: "+filter);

                client.executeQuery(sql, parameters, queryResponse);

            } else if (baseDn == null) {

                if (debug) log.debug("Searching all entries.");

                client.executeQuery(sql, parameters, queryResponse);
            }

        } finally {
            connection.closeClient(session);
            response.close();
        }

        log.debug("Search operation completed.");
    }

    public SearchResult createSearchResult(
            ResultSet rs
    ) throws Exception {

        Attributes attributes = new Attributes();
        RDNBuilder rb = new RDNBuilder();

        int column = 1;

        log.debug("Fields:");
        for (Field field : getFields()) {

            Object value = rs.getObject(column++);
            if (value == null) continue;

            String fieldName = field.getName();

            attributes.addValue(fieldName, value);

            if (field.isPrimaryKey()) {
                rb.set(fieldName, value);
                if (debug) log.debug(" - "+fieldName+": "+value+" (pk)");

            } else {
                if (debug) log.debug(" - "+fieldName+": "+value);
            }
        }

        DNBuilder db = new DNBuilder();
        db.append(rb.toRdn());
        db.append(sourceBaseDn);
        DN dn = db.toDn();

        return new SearchResult(dn, attributes);
    }

    public void search(
            final Session session,
            //final Collection<SourceRef> primarySourceRefs,
            final Collection<EntrySource> localSourceRefs,
            final Collection<EntrySource> sourceRefs,
            final SourceAttributes sourceValues,
            final SearchRequest request,
            final SearchResponse response
    ) throws Exception {

        if (debug) {
            log.debug(TextUtil.displaySeparator(80));
            log.debug(TextUtil.displayLine("Search "+ sourceRefs, 80));
            log.debug(TextUtil.displaySeparator(80));

            log.debug("Source values:");
            sourceValues.print();
        }

        response.setSizeLimit(request.getSizeLimit());

        SearchRequestBuilder builder = new SearchRequestBuilder(
                partition,
                localSourceRefs,
                sourceRefs,
                sourceValues,
                request,
                response
        );

        SelectStatement statement = builder.generate();

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

        String sizeLimit = getParameter(SIZE_LIMIT);

        if (sizeLimit != null) {
            queryResponse.setSizeLimit(Long.parseLong(sizeLimit));
        }

        JDBCClient client = connection.getClient(session);

        try {
            JDBCStatementBuilder statementBuilder = new JDBCStatementBuilder(sourceContext.getPartition());
            statementBuilder.setQuote(client.getQuote());

            String sql = statementBuilder.generate(statement);
            Collection<Object> parameters = statementBuilder.getParameters();

            client.executeQuery(sql, parameters, queryResponse);

        } finally {
            connection.closeClient(session);
        }

        log.debug("Search operation completed.");
    }

    public SearchResult createSearchResult(
            //Collection<SourceRef> primarySourceRefs,
            Collection<EntrySource> sourceRefs,
            ResultSet rs
    ) throws Exception {

        SearchResult searchResult = new SearchResult();

        SourceAttributes sourceValues = new SourceAttributes();
        RDNBuilder rb = new RDNBuilder();

        int column = 1;

        //log.debug("Fields:");
        for (EntrySource sourceRef : sourceRefs) {
            String alias = sourceRef.getAlias();
            //boolean primarySource = primarySourceRefs.contains(sourceRef);

            Attributes fields = new Attributes();

            for (EntryField fieldRef : sourceRef.getFields()) {

                Object value = rs.getObject(column++);

                String fieldName = fieldRef.getName();
                String name = alias + "." + fieldName;

                if (sourceRef.isPrimarySourceRef() && fieldRef.isPrimaryKey()) {
                    if (value == null) return null;
                    rb.set(name, value);
                    //if (debug) log.debug(" - "+name+": "+value+" (pk)");
                } else {
                    if (value == null) continue;
                    //if (debug) log.debug(" - "+name+": "+value);
                }

                fields.addValue(fieldName, value);
            }

            sourceValues.set(alias, fields);
        }

        searchResult.setSourceAttributes(sourceValues);
        searchResult.setDn(new DN(rb.toRdn()));

        return searchResult;
    }

    public void mergeSearchResult(SearchResult source, SearchResult destination) {
        SourceAttributes sourceValues = source.getSourceAttributes();
        SourceAttributes destinationValues = destination.getSourceAttributes();

        destinationValues.add(sourceValues);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Storage
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void create() throws Exception {

        if (debug) {
            log.debug(TextUtil.displaySeparator(80));
            log.debug(TextUtil.displayLine("Create "+getName(), 80));
            log.debug(TextUtil.displaySeparator(80));
        }

        JDBCClient client = connection.createClient();

        StringBuilder sb = new StringBuilder();

        sb.append("create table ");
        sb.append(connection.getTableName(sourceConfig));
        sb.append(" (");

        boolean first = true;
        for (FieldConfig fieldConfig : sourceConfig.getFieldConfigs()) {

            if (first) {
                first = false;
            } else {
                sb.append(", ");
            }

            sb.append(fieldConfig.getName());
            sb.append(" ");

            if (fieldConfig.getOriginalType() == null) {

                sb.append(fieldConfig.getType());

                if (fieldConfig.getLength() > 0) {
                    sb.append("(");
                    sb.append(fieldConfig.getLength());
                    sb.append(")");
                }

                if (fieldConfig.isCaseSensitive()) {
                    sb.append(" binary");
                }

                if (fieldConfig.isAutoIncrement()) {
                    sb.append(" auto_increment");
                }

            } else {
                sb.append(fieldConfig.getOriginalType());
            }
        }
/*
        Collection<String> indexFieldNames = sourceConfig.getIndexFieldNames();
        for (String fieldName : indexFieldNames) {
            sb.append(", index (");
            sb.append(fieldName);
            sb.append(")");
        }
*/
        Collection<String> primaryKeyNames = sourceConfig.getPrimaryKeyNames();
        if (!primaryKeyNames.isEmpty()) {
            sb.append(", primary key (");

            first = true;
            for (String fieldName : primaryKeyNames) {

                if (first) {
                    first = false;
                } else {
                    sb.append(", ");
                }

                sb.append(fieldName);
            }

            sb.append(")");
        }

        sb.append(")");

        String sql = sb.toString();

        client.executeUpdate(sql);

        client.close();
    }

    public void rename(Source newSource) throws Exception {

        if (debug) {
            log.debug(TextUtil.displaySeparator(80));
            log.debug(TextUtil.displayLine("Rename "+getName()+" to "+newSource.getName(), 80));
            log.debug(TextUtil.displaySeparator(80));
        }

        SourceConfig newSourceConfig = newSource.getSourceConfig();

        JDBCClient client = connection.createClient();

        StringBuilder sb = new StringBuilder();

        sb.append("rename table ");
        sb.append(connection.getTableName(sourceConfig));
        sb.append(" to ");
        sb.append(connection.getTableName(newSourceConfig));

        String sql = sb.toString();

        client.executeUpdate(sql);

        client.close();
    }

    public void clear(Session session) throws Exception {

        if (debug) {
            log.debug(TextUtil.displaySeparator(80));
            log.debug(TextUtil.displayLine("Clear "+getName(), 80));
            log.debug(TextUtil.displaySeparator(80));
        }

        JDBCClient client = connection.getClient(session);

        try {
            StringBuilder sb = new StringBuilder();

            sb.append("delete from ");
            sb.append(connection.getTableName(sourceConfig));

            String sql = sb.toString();

            client.executeUpdate(sql);

        } finally {
            connection.closeClient(session);
        }
    }

    public void drop() throws Exception {

        if (debug) {
            log.debug(TextUtil.displaySeparator(80));
            log.debug(TextUtil.displayLine("Drop "+getName(), 80));
            log.debug(TextUtil.displaySeparator(80));
        }

        JDBCClient client = connection.createClient();

        StringBuilder sb = new StringBuilder();

        sb.append("drop table ");
        sb.append(connection.getTableName(sourceConfig));

        String sql = sb.toString();

        client.executeUpdate(sql);

        client.close();
    }

    public void status() throws Exception {

        if (debug) {
            log.debug(TextUtil.displaySeparator(80));
            log.debug(TextUtil.displayLine("Status "+getName(), 80));
            log.debug(TextUtil.displaySeparator(80));
        }

        JDBCClient client = connection.createClient();

        final String tableName = connection.getTableName(sourceConfig);

        StringBuilder sb = new StringBuilder();

        sb.append("select count(*) from ");
        sb.append(tableName);

        String sql = sb.toString();

        QueryResponse response = new QueryResponse() {
            public void add(Object object) throws Exception {
                ResultSet rs = (ResultSet)object;
                log.error("Table "+tableName+": "+rs.getObject(1));
            }
        };

        client.executeQuery(sql, response);

        sb = new StringBuilder();

        sb.append("select ");

        boolean first = true;
        for (FieldConfig fieldConfig : sourceConfig.getFieldConfigs()) {

            if (first) {
                first = false;
            } else {
                sb.append(", ");
            }

            sb.append("max(length(");
            sb.append(fieldConfig.getOriginalName());
            sb.append("))");
        }

        sb.append(" from ");
        sb.append(tableName);

        sql = sb.toString();

        response = new QueryResponse() {
            public void add(Object object) throws Exception {
                ResultSet rs = (ResultSet)object;

                int index = 1;
                for (FieldConfig fieldConfig : sourceConfig.getFieldConfigs()) {
                    Object length = rs.getObject(index++);
                    int maxLength = fieldConfig.getLength();
                    log.error(" - Field " + fieldConfig.getName() + ": " + length + (maxLength > 0 ? "/" + maxLength : ""));
                }
            }
        };

        client.executeQuery(sql, response);

        client.close();
    }

    public long getCount(Session session) throws Exception {

        if (debug) {
            log.debug(TextUtil.displaySeparator(80));
            log.debug(TextUtil.displayLine("Count "+getName(), 80));
            log.debug(TextUtil.displaySeparator(80));
        }

        final String tableName = connection.getTableName(sourceConfig);

        StringBuilder sb = new StringBuilder();

        sb.append("select count(*) from ");
        sb.append(tableName);

        String sql = sb.toString();

        QueryResponse response = new QueryResponse() {
            public void add(Object object) throws Exception {
                ResultSet rs = (ResultSet)object;
                Long count = rs.getLong(1);
                super.add(count);
            }
        };

        executeQuery(session, sql, response);

        if (!response.hasNext()) {
            throw LDAP.createException(LDAP.OPERATIONS_ERROR);
        }

        Long count = (Long)response.next();
        log.error("Table "+tableName+": "+count);

        return count;
/*
        JDBCClient client = connection.getClient(session);

        try {
            client.executeQuery(sql, response);

            if (!response.hasNext()) {
                throw LDAP.createException(LDAP.OPERATIONS_ERROR);
            }

            Long count = (Long)response.next();
            log.error("Table "+tableName+": "+count);

            return count;

        } finally {
            connection.closeClient(session);
        }
*/
    }

    public String getTableName() throws Exception {
        return connection.getTableName(sourceConfig);
    }

    public void executeQuery(Session session, String sql, QueryResponse response) throws Exception {

        JDBCClient client = connection.getClient(session);

        try {
            client.executeQuery(sql, response);

        } finally {
            connection.closeClient(session);
        }
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
