package org.safehaus.penrose.jdbc;

import org.safehaus.penrose.directory.SourceRef;
import org.safehaus.penrose.filter.Filter;

/**
 * @author Endi S. Dewata
 */
public class DeleteStatement extends Statement {

    protected SourceRef sourceRef;
    protected Filter filter;

    public SourceRef getSourceRef() {
        return sourceRef;
    }

    public void setSourceRef(SourceRef sourceRef) {
        this.sourceRef = sourceRef;
    }

    public Filter getFilter() {
        return filter;
    }

    public void setFilter(Filter filter) {
        this.filter = filter;
    }
}
