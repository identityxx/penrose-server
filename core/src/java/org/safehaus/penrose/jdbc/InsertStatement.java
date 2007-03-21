package org.safehaus.penrose.jdbc;

import java.util.Collection;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class InsertStatement extends Statement {

    protected String table;
    protected Collection columns = new ArrayList();

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
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

    public boolean isEmpty() {
        return columns.isEmpty();
    }

    public String getSql() {

        log.debug("Generating insert statement.");

        StringBuilder sb = new StringBuilder();
        sb.append("insert into ");
        sb.append(table);
        sb.append(" (");

        boolean first = true;
        for (Iterator i=columns.iterator(); i.hasNext(); ) {
            String column = (String)i.next();

            if (first) {
                first = false;
            } else {
                sb.append(", ");
            }

            sb.append(column);
        }

        sb.append(") values (");

        first = true;
        for (Iterator i=parameters.iterator(); i.hasNext(); ) {
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

}
