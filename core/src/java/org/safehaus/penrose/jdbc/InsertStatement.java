package org.safehaus.penrose.jdbc;

import java.util.Collection;
import java.util.ArrayList;

/**
 * @author Endi S. Dewata
 */
public class InsertStatement extends Statement {

    protected StatementSource source = new StatementSource();
    protected Collection<Assignment> assignments = new ArrayList<Assignment>();

    public StatementSource getSource() {
        return source;
    }

    public void setSource(String partitionName, String sourceName) {
        source.setPartitionName(partitionName);
        source.setSourceName(sourceName);
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
