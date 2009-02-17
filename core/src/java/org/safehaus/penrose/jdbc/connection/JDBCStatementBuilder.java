package org.safehaus.penrose.jdbc.connection;

import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.jdbc.*;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.source.SourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class JDBCStatementBuilder {

    public Logger log = LoggerFactory.getLogger(getClass());

    protected Partition partition;
    protected String sql;
    protected Collection<Object> parameters = new ArrayList<Object>();

    private String quote;

    public JDBCStatementBuilder(Partition partition) {
        this.partition = partition;
    }

    public String generate(Statement statement) throws Exception {
        if (statement instanceof SelectStatement) {
            return generate((SelectStatement)statement);

        } else if (statement instanceof InsertStatement) {
            return generate((InsertStatement)statement);

        } else if (statement instanceof UpdateStatement) {
            return generate((UpdateStatement)statement);

        } else if (statement instanceof DeleteStatement) {
            return generate((DeleteStatement)statement);

        } else {
            return null;
        }
    }

    public String generate(SelectStatement statement) throws Exception {

        log.debug("Generating select statement.");

        StringBuilder sb = new StringBuilder();
        sb.append("select distinct\n    ");

        boolean first = true;
        for (String columnName : statement.getColumnNames()) {

            if (first) {
                first = false;
            } else {
                sb.append(",\n    ");
            }

            int i = columnName.indexOf('.');
            String sourceName = columnName.substring(0, i);
            String fieldName = columnName.substring(i+1);

            sb.append(sourceName);
            sb.append('.');

            if (quote != null) sb.append(quote);
            sb.append(fieldName);
            if (quote != null) sb.append(quote);
        }

        sb.append("\nfrom\n    ");

        Collection<String> aliases = statement.getSourceAliases();
        Iterator i = aliases.iterator();
        Iterator j = statement.getJoinClauses().iterator();

        String alias = (String)i.next();

        StatementSource source = statement.getSource(alias);
        String table = getTableName(source);

        sb.append(table);

        sb.append(" ");
        sb.append(alias);

        while (i.hasNext() && j.hasNext()) {
            alias = (String)i.next();

            source = statement.getSource(alias);
            table = getTableName(source);

            JoinClause joinClause = (JoinClause)j.next();
            String joinType = joinClause.getType();
            Filter joinCondition = joinClause.getCondition();
            String joinWhere = joinClause.getWhere();

            sb.append("\n");
            sb.append(joinType);

            sb.append("\n    ");
            sb.append(table);

            sb.append(" ");
            sb.append(alias);

            sb.append(" on ");

            if (joinWhere != null) {
                sb.append("(");
            }

            JDBCFilterBuilder filterBuilder = new JDBCFilterBuilder(partition);
            filterBuilder.setExtractValues(false);
            filterBuilder.setQuote(quote);
            filterBuilder.setAllowCaseSensitive(false);

            for (String cn : statement.getSourceAliases()) {
                source = statement.getSource(cn);
                filterBuilder.addSource(cn, source);
            }

            filterBuilder.generate(joinCondition);

            String sqlFilter = filterBuilder.getSql();
            sb.append(sqlFilter);

            if (joinWhere != null) {
                sb.append(") and (");
                sb.append(joinWhere);
                sb.append(")");
            }
        }

        Filter filter = statement.getFilter();

        JDBCFilterBuilder filterBuilder = new JDBCFilterBuilder(partition);
        filterBuilder.setQuote(quote);

        for (String cn : statement.getSourceAliases()) {
            source = statement.getSource(cn);
            filterBuilder.addSource(cn, source);
        }

        filterBuilder.generate(filter);
        String sql = filterBuilder.getSql();

        if (statement.getWhereClause() != null) {
            if (sql.length() > 0) {
                sql = "("+sql+") and ("+statement.getWhereClause()+")";
            } else {
                sql = statement.getWhereClause();
            }
        }

        if (sql.length() > 0) {
            sb.append("\nwhere\n    ");
            sb.append(sql);
        }

        parameters.addAll(filterBuilder.getParameters());

        first = true;
        for (String columnName : statement.getOrders()) {

            if (first) {
                sb.append("\norder by\n    ");
                first = false;
            } else {
                sb.append(", ");
            }

            int p = columnName.indexOf('.');

            if (p >= 0) {
                String sn = columnName.substring(0, p);
                String fn = columnName.substring(p+1);

                sb.append(sn);
                sb.append('.');

                columnName = fn;
            }

            if (quote != null) sb.append(quote);
            sb.append(columnName);
            if (quote != null) sb.append(quote);
        }

/*
        int totalCount = response.getTotalCount();
        long sizeLimit = request.getSizeLimit();

        if (sizeLimit == 0) {
            log.debug("Retrieving all entries.");

        } else {
            int size = sizeLimit - totalCount + 1;
            if (debug) log.debug("Retrieving "+size+" entries.");

            sb.append(" limit ");
            sb.append(size);
        }
*/

        return sb.toString();
    }

    public String generate(InsertStatement statement) throws Exception {
        log.debug("Generating insert statement.");

        StringBuilder sb = new StringBuilder();
        sb.append("insert into ");

        StatementSource source = statement.getSource();
        String table = getTableName(source);

        sb.append(table);

        StringBuilder sb1 = new StringBuilder();
        StringBuilder sb2 = new StringBuilder();
        boolean first = true;

        for (Assignment assignment : statement.getAssignments()) {
            String name = assignment.getName();

            if (first) {
                first = false;
            } else {
                sb1.append(", ");
                sb2.append(", ");
            }

            if (quote != null) sb1.append(quote);
            sb1.append(name);
            if (quote != null) sb1.append(quote);

            sb2.append("?");

            parameters.add(assignment.getValue());
        }

        sb.append(" (");
        sb.append(sb1);
        sb.append(") values (");
        sb.append(sb2);
        sb.append(")");

        return sb.toString();
    }

    public String generate(UpdateStatement statement) throws Exception {

        log.debug("Generating update statement.");

        StringBuilder sb = new StringBuilder();
        sb.append("update ");

        StatementSource source = statement.getSource();
        String table = getTableName(source);

        sb.append(table);

        sb.append(" set ");

        boolean first = true;
        for (Assignment assignment : statement.getAssignments()) {
            String name = assignment.getName();

            if (first) {
                first = false;
            } else {
                sb.append(", ");
            }

            if (quote != null) sb.append(quote);
            sb.append(name);
            if (quote != null) sb.append(quote);

            sb.append("=?");

            parameters.add(assignment.getValue());
        }

        Filter filter = statement.getFilter();

        JDBCFilterBuilder filterBuilder = new JDBCFilterBuilder(partition);
        filterBuilder.setQuote(quote);
        filterBuilder.setAppendSourceAlias(false);
        filterBuilder.addSource("s", source);

        filterBuilder.generate(filter);

        String whereClause = filterBuilder.getSql();
        if (whereClause.length() > 0) {
            sb.append(" where ");
            sb.append(whereClause);
        }

        parameters.addAll(filterBuilder.getParameters());

        return sb.toString();
    }

    public String generate(DeleteStatement statement) throws Exception {

        log.debug("Generating delete statement.");

        StringBuilder sb = new StringBuilder();
        sb.append("delete from ");

        StatementSource source = statement.getSource();
        String table = getTableName(source);

        sb.append(table);

        Filter filter = statement.getFilter();

        JDBCFilterBuilder filterBuilder = new JDBCFilterBuilder(partition);
        filterBuilder.setQuote(quote);
        filterBuilder.setAppendSourceAlias(false);
        filterBuilder.addSource("s", source);

        filterBuilder.generate(filter);

        String whereClause = filterBuilder.getSql();
        if (whereClause.length() > 0) {
            sb.append(" where ");
            sb.append(whereClause);
        }

        parameters.addAll(filterBuilder.getParameters());

        return sb.toString();
    }

    public String getTableName(StatementSource source) {

        Partition sourcePartition = partition.getPartitionContext().getPartition(source.getPartitionName());
        PartitionConfig partitionConfig = sourcePartition.getPartitionConfig();
        SourceConfig sourceConfig = partitionConfig.getSourceConfigManager().getSourceConfig(source.getSourceName());

        StringBuilder sb = new StringBuilder();

        String catalog = sourceConfig.getParameter(JDBC.CATALOG);
        if (catalog != null) {
            if (quote != null) sb.append(quote);
            sb.append(catalog);
            if (quote != null) sb.append(quote);
            sb.append(".");
        }

        String schema = sourceConfig.getParameter(JDBC.SCHEMA);
        if (schema != null) {
            if (quote != null) sb.append(quote);
            sb.append(schema);
            if (quote != null) sb.append(quote);
            sb.append(".");
        }

        String table = sourceConfig.getParameter(JDBC.TABLE);
        if (quote != null) sb.append(quote);
        sb.append(table);
        if (quote != null) sb.append(quote);

        return sb.toString();
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public Collection<Object> getParameters() {
        return parameters;
    }

    public String getQuote() {
        return quote;
    }

    public void setQuote(String quote) {
        this.quote = quote;
    }
}
