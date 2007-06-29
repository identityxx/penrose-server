package org.safehaus.penrose.adapter.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.jdbc.*;
import org.safehaus.penrose.source.SourceRef;
import org.safehaus.penrose.source.FieldRef;
import org.safehaus.penrose.source.Source;
import org.safehaus.penrose.source.Field;
import org.safehaus.penrose.filter.Filter;

import java.util.Iterator;
import java.util.Collection;
import java.util.ArrayList;

/**
 * @author Endi S. Dewata
 */
public class JDBCStatementBuilder {

    Logger log = LoggerFactory.getLogger(getClass());

    protected String sql;
    protected Collection<Assignment> assigments = new ArrayList<Assignment>();

    private String quote;

    public JDBCStatementBuilder() {
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
        sb.append("select distinct ");

        int count = statement.getSourceAliases().size();

        boolean first = true;
        for (FieldRef fieldRef : statement.getFieldRefs()) {

            if (first) {
                first = false;
            } else {
                sb.append(", ");
            }

            sb.append(fieldRef.getSourceName());
            sb.append('.');

            if (quote != null) sb.append(quote);
            sb.append(fieldRef.getOriginalName());
            if (quote != null) sb.append(quote);
        }

        sb.append(" from ");

        Collection<String> aliases = statement.getSourceAliases();
        Iterator i = aliases.iterator();
        Iterator j = statement.getJoinClauses().iterator();

        String alias = (String)i.next();
        SourceRef sourceRef = statement.getSourceRef(alias);
        Source source = sourceRef.getSource();
        String table = getTableName(source);

        if (quote != null) sb.append(quote);
        sb.append(table);
        if (quote != null) sb.append(quote);

        sb.append(" ");
        sb.append(alias);

        while (i.hasNext() && j.hasNext()) {
            alias = (String)i.next();
            sourceRef = statement.getSourceRef(alias);
            source = sourceRef.getSource();
            table = getTableName(source);

            JoinClause joinClause = (JoinClause)j.next();
            String joinType = joinClause.getType();
            Filter joinFilter = joinClause.getFilter();
            String joinOn = joinClause.getSql();

            sb.append(" ");
            sb.append(joinType);

            sb.append(" ");
            if (quote != null) sb.append(quote);
            sb.append(table);
            if (quote != null) sb.append(quote);

            sb.append(" ");
            sb.append(alias);

            sb.append(" on ");

            if (joinOn != null) {
                sb.append("(");
            }

            JDBCFilterBuilder filterBuilder = new JDBCFilterBuilder(false);
            filterBuilder.setQuote(quote);

            for (String cn : statement.getSourceAliases()) {
                SourceRef sr = statement.getSourceRef(cn);
                filterBuilder.addSourceRef(cn, sr);
            }

            filterBuilder.generate(joinFilter);

            String sqlFilter = filterBuilder.getSql();
            sb.append(sqlFilter);

            if (joinOn != null) {
                sb.append(") and (");
                sb.append(joinOn);
                sb.append(")");
            }
        }

        Filter filter = statement.getFilter();

        JDBCFilterBuilder filterBuilder;

        if (count > 1) {
            filterBuilder = new JDBCFilterBuilder();
            filterBuilder.setQuote(quote);

            for (String cn : statement.getSourceAliases()) {
                sourceRef = statement.getSourceRef(cn);
                filterBuilder.addSourceRef(cn, sourceRef);
            }

        } else {
            filterBuilder = new JDBCFilterBuilder(source);
            filterBuilder.setQuote(quote);
        }

        filterBuilder.generate(filter);
        String sql = filterBuilder.getSql();

        if (statement.getWhere() != null) {
            if (sql.length() > 0) {
                sql = "("+sql+") and ("+statement.getWhere()+")";
            } else {
                sql = statement.getWhere();
            }
        }

        if (sql.length() > 0) {
            sb.append(" where ");
            sb.append(sql);
        }

        assigments.addAll(filterBuilder.getAssignments());

        first = true;
        for (FieldRef fieldRef : statement.getOrders()) {

            if (first) {
                sb.append(" order by ");
                first = false;
            } else {
                sb.append(", ");
            }

            if (count > 1) {
                sb.append(fieldRef.getSourceName());
                sb.append('.');
            }

            if (quote != null) sb.append(quote);
            sb.append(fieldRef.getOriginalName());
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

        Source source = statement.getSource();
        String table = getTableName(source);

        if (quote != null) sb.append(quote);
        sb.append(table);
        if (quote != null) sb.append(quote);

        StringBuilder sb1 = new StringBuilder();
        StringBuilder sb2 = new StringBuilder();
        boolean first = true;

        for (Assignment assignment : statement.getAssignments()) {
            Field field = assignment.getField();

            if (first) {
                first = false;
            } else {
                sb1.append(", ");
                sb2.append(", ");
            }

            if (quote != null) sb1.append(quote);
            sb1.append(field.getOriginalName());
            if (quote != null) sb1.append(quote);

            sb2.append("?");

            assigments.add(assignment);
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

        Source source = statement.getSource();
        String table = getTableName(source);

        if (quote != null) sb.append(quote);
        sb.append(table);
        if (quote != null) sb.append(quote);

        sb.append(" set ");

        boolean first = true;
        for (Assignment assignment : statement.getAssignments()) {
            Field field = assignment.getField();

            if (first) {
                first = false;
            } else {
                sb.append(", ");
            }

            if (quote != null) sb.append(quote);
            sb.append(field.getOriginalName());
            if (quote != null) sb.append(quote);

            sb.append("=?");

            assigments.add(assignment);
        }

        Filter filter = statement.getFilter();

        JDBCFilterBuilder filterBuilder = new JDBCFilterBuilder(source);
        filterBuilder.setQuote(quote);

        filterBuilder.generate(filter);

        String whereClause = filterBuilder.getSql();
        if (whereClause.length() > 0) {
            sb.append(" where ");
            sb.append(whereClause);
        }

        assigments.addAll(filterBuilder.getAssignments());

        return sb.toString();
    }

    public String generate(DeleteStatement statement) throws Exception {

        log.debug("Generating delete statement.");

        StringBuilder sb = new StringBuilder();
        sb.append("delete from ");

        Source source = statement.getSource();
        String table = getTableName(source);

        if (quote != null) sb.append(quote);
        sb.append(table);
        if (quote != null) sb.append(quote);

        Filter filter = statement.getFilter();

        JDBCFilterBuilder filterBuilder = new JDBCFilterBuilder(source);
        filterBuilder.setQuote(quote);

        filterBuilder.generate(filter);

        String whereClause = filterBuilder.getSql();
        if (whereClause.length() > 0) {
            sb.append(" where ");
            sb.append(whereClause);
        }

        assigments.addAll(filterBuilder.getAssignments());

        return sb.toString();
    }

    public String getTableName(Source source) {
        StringBuilder sb = new StringBuilder();

        String catalog = source.getParameter(JDBCClient.CATALOG);
        if (catalog != null) {
            if (quote != null) sb.append(quote);
            sb.append(catalog);
            if (quote != null) sb.append(quote);
            sb.append(".");
        }

        String schema = source.getParameter(JDBCClient.SCHEMA);
        if (schema != null) {
            if (quote != null) sb.append(quote);
            sb.append(schema);
            if (quote != null) sb.append(quote);
            sb.append(".");
        }

        String table = source.getParameter(JDBCClient.TABLE);
        if (table == null) table = source.getParameter(JDBCClient.TABLE_NAME);

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

    public Collection<Assignment> getAssigments() {
        return assigments;
    }

    public void setAssigments(Collection<Assignment> assigments) {
        this.assigments = assigments;
    }

    public String getQuote() {
        return quote;
    }

    public void setQuote(String quote) {
        this.quote = quote;
    }
}
