package org.safehaus.penrose.source;

import java.io.Serializable;
import java.util.Collection;
import java.util.ArrayList;

/**
 * @author Endi Sukma Dewata
 */
public class IndexConfig implements Serializable, Cloneable {

    private String name;
    private Collection<String> fieldNames = new ArrayList<String>();

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Collection<String> getFieldNames() {
        return fieldNames;
    }

    public void addFieldName(String fieldName) {
        fieldNames.add(fieldName);
    }

    public void removeFieldName(String fieldName) {
        fieldNames.remove(fieldName);
    }

    public void setFieldNames(Collection<String> fieldNames) {
        if (this.fieldNames == fieldNames) return;
        this.fieldNames.clear();
        this.fieldNames.addAll(fieldNames);
    }

    public int hashCode() {
        return name == null ? 0 : name.hashCode();
    }

    boolean equals(Object o1, Object o2) {
        if (o1 == null && o2 == null) return true;
        if (o1 != null) return o1.equals(o2);
        return o2.equals(o1);
    }

    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null) return false;
        if (object.getClass() != this.getClass()) return false;

        IndexConfig indexConfig = (IndexConfig)object;

        if (!equals(name, indexConfig.name)) return false;
        if (!equals(fieldNames, indexConfig.fieldNames)) return false;

        return true;
    }

    public Object clone() throws CloneNotSupportedException {
        IndexConfig indexConfig = (IndexConfig)super.clone();

        indexConfig.name = name;

        indexConfig.fieldNames = new ArrayList<String>();
        indexConfig.fieldNames.addAll(fieldNames);

        return indexConfig;
    }
}
