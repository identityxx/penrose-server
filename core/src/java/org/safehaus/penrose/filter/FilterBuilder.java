package org.safehaus.penrose.filter;

/**
 * @author Endi Sukma Dewata
 */
public class FilterBuilder implements ContainerFilter {

    Filter filter;

    public Filter getFilter() {
        return filter;
    }

    public void setFilter(Filter filter) {
        this.filter = filter;
        filter.setParent(this);
    }

    public void replace(Filter oldFilter, Filter newFilter) {
        if (oldFilter != filter) return;
        filter = newFilter;
        if (newFilter != null) newFilter.setParent(this);
    }

    public String toString() {
        return filter == null ? "" : filter.toString();
    }
}
