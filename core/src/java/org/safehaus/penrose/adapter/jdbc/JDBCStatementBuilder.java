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

/**
 * @author Endi S. Dewata
 */
public class JDBCStatementBuilder {

    Logger log = LoggerFactory.getLogger(getClass());

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

        boolean first = true;
        for (Iterator i=statement.getFieldRefs().iterator(); i.hasNext(); ) {
            FieldRef fieldRef = (FieldRef)i.next();
            if (first) {
                first = false;
            } else {
                sb.append(", ");
            }
            sb.append(fieldRef.getSourceName()+"."+ fieldRef.getOriginalName());
        }

        sb.append(" from ");

        Iterator i = statement.getSourceAliases().iterator();
        Iterator j = statement.getJoinTypes().iterator();
        Iterator k = statement.getJoinOns().iterator();

        String alias = (String)i.next();
        SourceRef sourceRef = statement.getSourceRef(alias);
        Source source = sourceRef.getSource();
        String table = getTableName(source);

        sb.append(table);
        sb.append(" ");
        sb.append(alias);

        while (i.hasNext() && j.hasNext() && k.hasNext()) {
            alias = (String)i.next();
            sourceRef = statement.getSourceRef(alias);
            source = sourceRef.getSource();
            table = getTableName(source);

            String joinType = (String)j.next();
            String joinOn = (String)k.next();
            sb.append(" ");
            sb.append(joinType);
            sb.append(" ");
            sb.append(table);
            sb.append(" ");
            sb.append(alias);
            sb.append(" on ");
            sb.append(joinOn);
        }

        Filter filter = statement.getFilter();

        JDBCFilterBuilder filterBuilder = new JDBCFilterBuilder();

        for (Iterator iterator=statement.getSourceAliases().iterator(); iterator.hasNext(); ) {
            alias = (String)iterator.next();
            sourceRef = statement.getSourceRef(alias);
            filterBuilder.addSourceRef(alias, sourceRef);
        }

        String whereClause = filterBuilder.generate(filter);

        if (whereClause.length() > 0) {
            sb.append(" where ");
            sb.append(whereClause);
        }

        Collection orders = statement.getOrders();
        first = true;
        for (Iterator iterator=orders.iterator(); iterator.hasNext(); ) {
            FieldRef fieldRef = (FieldRef)iterator.next();

            if (first) {
                sb.append(" order by ");
                first = false;
            } else {
                sb.append(", ");
            }

            sb.append(fieldRef.getSourceName());
            sb.append('.');
            sb.append(fieldRef.getOriginalName());
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
        sb.append(table);

        sb.append(" (");

        boolean first = true;
        for (Iterator i=statement.getFields().iterator(); i.hasNext(); ) {
            Field field = (Field)i.next();

            if (first) {
                first = false;
            } else {
                sb.append(", ");
            }

            sb.append(field.getOriginalName());
        }

        sb.append(") values (");

        first = true;
        for (Iterator i=statement.getFields().iterator(); i.hasNext(); ) {
            i.next();

            if (first) {
                first = false;
            } else {
                sb.append(", ");
            }

            sb.append("?");
        }

        sb.append(")");

        return sb.toString();
    }

    public String generate(UpdateStatement statement) throws Exception {

        log.debug("Generating update statement.");

        StringBuilder sb = new StringBuilder();
        sb.append("update ");

        SourceRef sourceRef = statement.getSource();
        Source source = sourceRef.getSource();
        String table = getTableName(source);
        sb.append(table);

        sb.append(" set ");

        boolean first = true;
        for (Iterator j=statement.getFields().iterator(); j.hasNext(); ) {
            FieldRef fieldRef = (FieldRef)j.next();

            if (first) {
                first = false;
            } else {
                sb.append(", ");
            }

            sb.append(fieldRef.getOriginalName());
            sb.append("=?");
        }

        Filter filter = statement.getFilter();

        JDBCFilterBuilder filterBuilder = new JDBCFilterBuilder(source);
        String whereClause = filterBuilder.generate(filter);

        if (whereClause.length() > 0) {
            sb.append(" where ");
            sb.append(whereClause);
        }

        return sb.toString();
    }

    public String generate(DeleteStatement statement) throws Exception {

        log.debug("Generating delete statement.");

        StringBuilder sb = new StringBuilder();
        sb.append("delete from ");

        Source source = statement.getSource();
        String table = getTableName(source);
        sb.append(table);

        Filter filter = statement.getFilter();

        JDBCFilterBuilder filterBuilder = new JDBCFilterBuilder(source);
        String whereClause = filterBuilder.generate(filter);

        if (whereClause.length() > 0) {
            sb.append(" where ");
            sb.append(whereClause);
        }

        return sb.toString();
    }

    public String getTableName(Source source) {
        String table = source.getParameter(JDBCClient.TABLE);
        if (table == null) table = source.getParameter(JDBCClient.TABLE_NAME);

        String catalog = source.getParameter(JDBCClient.CATALOG);
        if (catalog != null) table = catalog +"."+table;

        String schema = source.getParameter(JDBCClient.SCHEMA);
        if (schema != null) table = schema +"."+table;

        return table;
    }
}
