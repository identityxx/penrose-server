package org.safehaus.penrose.jdbc;

import org.safehaus.penrose.filter.Filter;

import java.util.Collection;
import java.util.ArrayList;

/**
 * @author Endi S. Dewata
 */
public class UpdateStatement extends Statement {

    protected String sourceName;
    protected Collection<Assignment> assignments = new ArrayList<Assignment>();
    protected Filter filter;

    public String getSourceName() {
        return sourceName;
    }

    public void setSourceName(String sourceName) {
        this.sourceName = sourceName;
    }

    public Collection<Assignment> getAssignments() {
        return assignments;
    }

    public void addAssignment(Assignment assignment) {
        assignments.add(assignment);
    }

    public void setAssignments(Collection<Assignment> assignments) {
        this.assignments = assignments;
    }

    public Filter getFilter() {
        return filter;
    }

    public void setFilter(Filter filter) {
        this.filter = filter;
    }

    public boolean isEmpty() {
        return assignments.isEmpty();
    }
}
