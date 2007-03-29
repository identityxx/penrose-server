package org.safehaus.penrose.jdbc;

import org.safehaus.penrose.source.Source;
import org.safehaus.penrose.source.Field;

import java.util.Collection;
import java.util.ArrayList;

/**
 * @author Endi S. Dewata
 */
public class InsertStatement extends Statement {

    protected Source source;
    protected Collection fields = new ArrayList();

    public Source getSource() {
        return source;
    }

    public void setSource(Source source) {
        this.source = source;
    }

    public Collection getFields() {
        return fields;
    }

    public void addField(Field field) {
        fields.add(field);
    }

    public void addFields(Collection fields) {
        this.fields.addAll(fields);
    }
    
    public void setFields(Collection fields) {
        this.fields = fields;
    }

    public boolean isEmpty() {
        return fields.isEmpty();
    }
}
