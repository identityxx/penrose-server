package org.safehaus.penrose.directory;

import org.safehaus.penrose.source.Field;
import org.safehaus.penrose.mapping.Expression;

import java.util.Collection;
import java.util.ArrayList;

/**
 * @author Endi S. Dewata
 */
public class FieldRef implements Cloneable {

    public final static Collection<String> EMPTY = new ArrayList<String>();

    public String sourceName;
    private Field field;

    private String name;
    private boolean rdn;

    private FieldMapping fieldMapping;

    public FieldRef(SourceRef sourceRef, Field field) throws Exception {
        this.sourceName = sourceRef.getAlias();
        this.field = field;

        name = field.getName();
    }
    
    public FieldRef(SourceRef sourceRef, Field field, FieldMapping fieldMapping) throws Exception {
        this.sourceName = sourceRef.getAlias();
        this.field = field;
        this.fieldMapping = fieldMapping;

        name = fieldMapping.getName();

        String variable = fieldMapping.getVariable();

        if (variable != null && variable.indexOf('.') < 0) {
            Entry entry = sourceRef.getEntry();
            AttributeMapping attributeMapping = entry.getAttributeMapping(variable);
            if (attributeMapping == null) throw new Exception("Unknown attribute "+variable);

            rdn = attributeMapping.isRdn();

        } else {
            rdn = fieldMapping.isRdn();
        }
    }

    public String getSourceName() {
        return sourceName;
    }

    public FieldMapping getFieldMapping() {
        return fieldMapping;
    }

    public void setFieldMapping(FieldMapping fieldMapping) {
        this.fieldMapping = fieldMapping;
    }

    public String getName() {
        return name;
    }

    public String getOriginalName() {
        return field.getOriginalName();
    }

    public boolean isCaseSensitive() {
        return field.isCaseSensitive();
    }

    public boolean isPrimaryKey() {
        return field.isPrimaryKey();
    }

    public String getType() {
        return field.getType();
    }

    public void setName(String name) {
        this.name = name;
    }

    public Field getField() {
        return field;
    }

    public void setField(Field field) {
        this.field = field;
    }

    public Collection<String> getOperations() {
        return fieldMapping == null ? EMPTY : fieldMapping.getOperations();
    }

    public boolean isRdn() {
        return rdn;
    }

    public void setRdn(boolean rdn) {
        this.rdn = rdn;
    }

    public Object getConstant() {
        return fieldMapping == null ? field.getConstant() : fieldMapping.getConstant();
    }

    public String getVariable() {
        return fieldMapping == null ? field.getVariable() : fieldMapping.getVariable();
    }

    public Expression getExpression() {
        return fieldMapping == null ? field.getExpression() : fieldMapping.getExpression();
    }

    public String toString() {
        return sourceName+"."+name;
    }

    public Object clone() throws CloneNotSupportedException {

        FieldRef fieldRef = (FieldRef)super.clone();

        fieldRef.sourceName = sourceName;
        fieldRef.field = field;

        fieldRef.name = name;
        fieldRef.rdn = rdn;

        fieldRef.fieldMapping = fieldMapping;

        return fieldRef;
    }
}
