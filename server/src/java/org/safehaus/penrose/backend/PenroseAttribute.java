package org.safehaus.penrose.backend;

import org.safehaus.penrose.ldap.Attribute;

import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class PenroseAttribute implements com.identyx.javabackend.Attribute {

    Attribute attribute;

    public PenroseAttribute(String name) throws Exception {
        this.attribute = new Attribute(name);
    }

    public PenroseAttribute(String name, Collection values) throws Exception {
        this.attribute = new Attribute(name, values);
    }

    public PenroseAttribute(Attribute attribute) {
        this.attribute = attribute;
    }

    public void setName(String name) throws Exception {
        attribute.setName(name);
    }

    public String getName() throws Exception {
        return attribute.getName();
    }

    public void addValue(Object value) throws Exception {
        attribute.addValue(value);
    }

    public void removeValue(Object value) throws Exception {
        attribute.removeValue(value);
    }

    public Object getValue() throws Exception {
        return attribute.getValue();
    }

    public Collection getValues() throws Exception {
        return attribute.getValues();
    }

    public Attribute getAttribute() {
        return attribute;
    }
}
