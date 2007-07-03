package org.safehaus.penrose.sql;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class SQLTableList {

    private String string;
    Collection tableRefs = new ArrayList();

    public String getString() {
        return string;
    }

    public void setString(String string) {
        this.string = string;
    }

    public void addTableRef(SQLTableRef tableRef) {
        tableRefs.add(tableRef);
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();

        for (Iterator i=tableRefs.iterator(); i.hasNext(); ) {
            SQLTableRef tableRef = (SQLTableRef)i.next();
            sb.append(tableRef.toString());
        }

        return sb.toString();
    }
}
