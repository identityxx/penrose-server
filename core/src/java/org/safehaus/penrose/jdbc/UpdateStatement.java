package org.safehaus.penrose.jdbc;

import java.util.Collection;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class UpdateStatement extends Statement {

    protected Collection tables = new ArrayList();
    protected Collection joinTypes = new ArrayList();
    protected Collection joinOns = new ArrayList();
    protected Collection columns = new ArrayList();
    protected String whereClause;

    public Collection getTables() {
        return tables;
    }

    public void addTable(String table) {
        tables.add(table);
    }

    public void setTables(Collection tables) {
        this.tables = tables;
    }

    public Collection getJoinTypes() {
        return joinTypes;
    }

    public void setJoinTypes(Collection joinTypes) {
        this.joinTypes = joinTypes;
    }

    public Collection getJoinOns() {
        return joinOns;
    }

    public void setJoinOns(Collection joinOns) {
        this.joinOns = joinOns;
    }

    public Collection getColumns() {
        return columns;
    }

    public void addColumn(String column) {
        columns.add(column);
    }

    public void setColumns(Collection columns) {
        this.columns = columns;
    }

    public String getWhereClause() {
        return whereClause;
    }

    public void setWhereClause(String whereClause) {
        this.whereClause = whereClause;
    }

    public String getSql() {

        log.debug("Generating update statement.");

        StringBuilder sb = new StringBuilder();
        sb.append("update ");

        Iterator i=tables.iterator();
        sb.append(i.next());

        for (Iterator j=joinTypes.iterator(), k=joinOns.iterator(); i.hasNext() && j.hasNext() && k.hasNext(); ) {
            String table = (String)i.next();
            String joinType = (String)j.next();
            String joinOn = (String)k.next();
            sb.append(" ");
            sb.append(joinType);
            sb.append(" ");
            sb.append(table);
            sb.append(" on ");
            sb.append(joinOn);
        }

        sb.append(" set ");

        boolean first = true;
        for (Iterator j=columns.iterator(); j.hasNext(); ) {
            String column = (String)j.next();

            if (first) {
                first = false;
            } else {
                sb.append(", ");
            }

            sb.append(column);
            sb.append("=?");
        }

        if (whereClause != null) {
            sb.append(" where ");
            sb.append(whereClause);
        }

        return sb.toString();
    }

}
