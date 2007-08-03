package org.safehaus.penrose.jdbc;

import org.safehaus.penrose.filter.Filter;

/**
 * @author Endi Sukma Dewata
 */
public class JoinClause {

    private String type;
    private Filter condition;
    private String where;

    public Filter getCondition() {
        return condition;
    }

    public void setCondition(Filter condition) {
        this.condition = condition;
    }

    public String getWhere() {
        return where;
    }

    public void setWhere(String where) {
        this.where = where;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
