package org.safehaus.penrose.jdbc;

import org.safehaus.penrose.source.Source;

import java.util.Collection;
import java.util.ArrayList;

/**
 * @author Endi S. Dewata
 */
public class InsertStatement extends Statement {

    protected Source source;
    protected Collection<Assignment> assignments = new ArrayList<Assignment>();

    public Source getSource() {
        return source;
    }

    public void setSource(Source source) {
        this.source = source;
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

    public boolean isEmpty() {
        return assignments.isEmpty();
    }
}
