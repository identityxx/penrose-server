package org.safehaus.penrose.jdbc;

import org.safehaus.penrose.filter.Filter;

/**
 * @author Endi S. Dewata
 */
public class DeleteStatement extends Statement {

    protected String sourceName;
    protected Filter filter;

    public String getSourceName() {
        return sourceName;
    }

    public void setSourceName(String sourceName) {
        this.sourceName = sourceName;
    }

    public Filter getFilter() {
        return filter;
    }

    public void setFilter(Filter filter) {
        this.filter = filter;
    }
}
