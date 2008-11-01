package org.safehaus.penrose.jdbc.source;

import org.safehaus.penrose.source.*;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.util.TextUtil;
import org.safehaus.penrose.jdbc.*;
import org.safehaus.penrose.jdbc.connection.*;
import org.safehaus.penrose.filter.SimpleFilter;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.FilterTool;

import java.util.*;
import java.sql.ResultSet;

/**
 * @author Endi S. Dewata
 */
public class JDBCJoinSource extends Source {

    public final static String SOURCES         = "sources";

    public final static String JOIN_TYPES      = "joinTypes";
    public final static String JOIN_CONDITIONS = "joinConditions";

    public final static String SIZE_LIMIT      = "sizeLimit";

    JDBCConnection connection;

    List<JDBCSource> sources = new ArrayList<JDBCSource>();
    Map<String,JDBCSource> sourcesByAlias = new LinkedHashMap<String,JDBCSource>();

    String primarySourceAlias;
    List<String> secondarySourceAliases = new ArrayList<String>();

    Map<String,String> joinTypes      = new LinkedHashMap<String,String>();
    Map<String,String> joinConditions = new LinkedHashMap<String,String>();

    public JDBCJoinSource() {
    }

    public void init() throws Exception {
        connection = (JDBCConnection)getConnection();

        SourceManager sourceManager = getSourceContext().getPartition().getSourceManager();

        String s = getParameter(SOURCES);
        String[] list = s.split(",");

        for (int i = 0; i<list.length; i++) {
            String sourceAndAlias = list[i];

            if (debug) log.debug("Source "+sourceAndAlias+":");

            String[] s3 = sourceAndAlias.split(" ");
            String sourceName = s3[0];
            String alias = s3[1];

            JDBCSource source = (JDBCSource)sourceManager.getSource(sourceName);
            sources.add(source);
            sourcesByAlias.put(alias, source);

            if (i == 0) {
                primarySourceAlias = alias;
            } else {
                secondarySourceAliases.add(alias);
            }
        }

        s = getParameter(JOIN_TYPES);
        list = s.split(",");

        for (int i = 0; i<list.length; i++) {
            String joinType = list[i];
            String alias = secondarySourceAliases.get(i);
            joinTypes.put(alias, joinType);
        }

        s = getParameter(JOIN_CONDITIONS);
        list = s.split(",");

        for (int i = 0; i<list.length; i++) {
            String joinCondition = list[i];
            String alias = secondarySourceAliases.get(i);
            joinConditions.put(alias, joinCondition);
        }
    }

    public Collection<JDBCSource> getSources() {
        return sources;
    }

    public Collection<String> getSourceAliases() {
        return sourcesByAlias.keySet();
    }

    public JDBCSource getSource(String alias) {
        return sourcesByAlias.get(alias);
    }

    public JDBCSource getPrimarySource() {
        return sourcesByAlias.get(primarySourceAlias);
    }

    public boolean isPrimarySourceAlias(String alias) {
        return primarySourceAlias.equals(alias);
    }

    public String getPrimarySourceAlias() {
        return primarySourceAlias;
    }

    public Collection<JDBCSource> getSecondarySources() {
        Collection<JDBCSource> list = new ArrayList<JDBCSource>();
        for (String alias : secondarySourceAliases) {
            list.add(sourcesByAlias.get(alias));
        }
        return list;
    }

    public Collection<String> getSecondarySourceAliases() {
        return secondarySourceAliases;
    }

    public String getJoinType(String alias) {
        return joinTypes.get(alias);
    }

    public String getJoinCondition(String alias) {
        return joinConditions.get(alias);
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
            RDN rdn = request.getDn().getRdn();
            Attributes attributes = request.getAttributes();

            Map<String,Object> values = new LinkedHashMap<String,Object>();

            for (Field field : getFields()) {
                String name = field.getName();

                Object value = rdn.get(name);
                if (value == null) value = attributes.getValue(name);
                if (value == null) continue;

                values.put(name, value);
            }

            Collection<String> parameterNames = values.keySet();
            if (debug) log.debug("Parameters: "+parameterNames);

            for (JDBCSource source : sources) {
                if (debug) log.debug("Adding into table "+source.getName()+".");

                SQLOperation operation = source.getOperation(JDBCSource.ADD, parameterNames);
                if (operation == null) continue;

                Collection<Object> parameters = source.getParameters(operation, values);

                client.executeUpdate(operation.getStatement(), parameters);
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
            RDN rdn = request.getDn().getRdn();

            Map<String,Object> values = new LinkedHashMap<String,Object>();

            for (Field field : getFields()) {
                String name = field.getName();

                Object value = rdn.get(name);
                if (value == null) continue;

                values.put(name, value);
            }

            Collection<String> parameterNames = values.keySet();
            if (debug) log.debug("Parameters: "+parameterNames);

            for (int i = sources.size()-1; i>=0; i--) {

                JDBCSource source = sources.get(i);
                if (debug) log.debug("Deleting from "+source.getName()+".");

                SQLOperation operation = source.getOperation(JDBCSource.DELETE, parameterNames);
                if (operation == null) continue;

                Collection<Object> parameters = source.getParameters(operation, values);

                client.executeUpdate(operation.getStatement(), parameters);
            }

            log.debug("Delete operation completed.");

        } finally {
            connection.closeClient(session);
        }
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

        response.setSizeLimit(request.getSizeLimit());

        NewSearchRequestBuilder builder = new NewSearchRequestBuilder(
                this,
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

                SearchResult searchResult = createSearchResult(rs);
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
        //db.append(sourceBaseDn);
        DN dn = db.toDn();

        return new SearchResult(dn, attributes);
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

        JDBCJoinSource source = (JDBCJoinSource)super.clone();

        source.connection       = connection;

        return source;
    }
}