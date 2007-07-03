package org.safehaus.penrose.jdbc;

import org.safehaus.penrose.source.Source;
import org.safehaus.penrose.filter.Filter;

/**
 * @author Endi S. Dewata
 */
public class DeleteStatement extends Statement {

    protected Source source;
    protected Filter filter;

    public Source getSource() {
        return source;
    }

    public void setSource(Source source) {
        this.source = source;
    }

    public Filter getFilter() {
        return filter;
    }

    public void setFilter(Filter filter) {
        this.filter = filter;
    }
}
