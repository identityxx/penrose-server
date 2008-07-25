package org.safehaus.penrose.jdbc;

import org.safehaus.penrose.filter.Filter;

/**
 * @author Endi S. Dewata
 */
public class DeleteStatement extends Statement {

    protected StatementSource source = new StatementSource();
    protected Filter filter;

    public StatementSource getSource() {
        return source;
    }

    public void setSource(String partitionName, String sourceName) {
        source.setPartitionName(partitionName);
        source.setSourceName(sourceName);
    }

    public Filter getFilter() {
        return filter;
    }

    public void setFilter(Filter filter) {
        this.filter = filter;
    }
}
