package org.safehaus.penrose.source;

import org.safehaus.penrose.mapping.SourceMapping;
import org.safehaus.penrose.mapping.FieldMapping;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class SourceRef {

    private Source source;

    private String alias;
    private boolean required;

    Collection primaryKeyFieldRefs = new ArrayList();
    Map fieldRefs = new LinkedHashMap();

    public SourceRef(Source source) {
        this.source = source;

        this.alias = source.getName();
        this.required = true;

        for (Iterator i=source.getFields().iterator(); i.hasNext(); ) {
            Field field = (Field)i.next();
            String fieldName = field.getName();

            FieldRef fieldRef = new FieldRef(field, alias, null);
            fieldRefs.put(fieldName, fieldRef);

            if (field.isPrimaryKey()) primaryKeyFieldRefs.add(fieldRef);
        }
    }

    public SourceRef(Source source, SourceMapping sourceMapping) {
        this.source = source;

        this.alias = sourceMapping.getName();
        this.required = sourceMapping.isRequired();

        Collection fieldMappings = sourceMapping.getFieldMappings();
        for (Iterator i=fieldMappings.iterator(); i.hasNext(); ) {
            FieldMapping fieldMapping = (FieldMapping)i.next();
            String fieldName = fieldMapping.getName();
            Field field = source.getField(fieldName);

            FieldRef fieldRef = new FieldRef(field, alias, fieldMapping);
            fieldRefs.put(fieldName, fieldRef);

            if (field.isPrimaryKey()) primaryKeyFieldRefs.add(fieldRef);
        }
    }

    public Collection getPrimaryKeyFieldRefs() {
        return primaryKeyFieldRefs;
    }

    public Collection getFieldRefs() {
        return fieldRefs.values();
    }

    public FieldRef getFieldRef(String fieldName) {
        return (FieldRef)fieldRefs.get(fieldName);
    }

    public String getAlias() {
        return alias;
    }

    public boolean isRequired() {
        return required;
    }

    public String toString() {
        return alias;
    }

    public Source getSource() {
        return source;
    }

    public void setSource(Source source) {
        this.source = source;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public void setRequired(boolean required) {
        this.required = required;
    }
}
