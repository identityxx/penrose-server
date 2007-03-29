package org.safehaus.penrose.jdbc;

import org.safehaus.penrose.source.SourceRef;
import org.safehaus.penrose.source.FieldRef;
import org.safehaus.penrose.filter.Filter;

import java.util.Collection;
import java.util.ArrayList;

/**
 * @author Endi S. Dewata
 */
public class UpdateStatement extends Statement {

    protected SourceRef sourceRef;
    protected Collection fields = new ArrayList();
    protected Filter filter;

    public SourceRef getSource() {
        return sourceRef;
    }

    public void setSource(SourceRef sourceRef) {
        this.sourceRef = sourceRef;
    }

    public Collection getFields() {
        return fields;
    }

    public void addField(FieldRef fieldRef) {
        fields.add(fieldRef);
    }

    public void setFields(Collection fields) {
        this.fields = fields;
    }

    public Filter getFilter() {
        return filter;
    }

    public void setFilter(Filter filter) {
        this.filter = filter;
    }

    public boolean isEmpty() {
        return fields.isEmpty();
    }
}
