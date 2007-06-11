package org.safehaus.penrose.jdbc;

import org.safehaus.penrose.filter.Filter;

/**
 * @author Endi Sukma Dewata
 */
public class JoinClause {

    private String type;
    private Filter filter;
    private String sql;

    public Filter getFilter() {
        return filter;
    }

    public void setFilter(Filter filter) {
        this.filter = filter;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
