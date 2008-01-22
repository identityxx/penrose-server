package org.safehaus.penrose.backend;

import org.safehaus.penrose.ldap.Attributes;
import org.safehaus.penrose.ldap.Attribute;

import java.util.Collection;
import java.util.ArrayList;

/**
 * @author Endi S. Dewata
 */
public class PenroseAttributes implements com.identyx.javabackend.Attributes {

    Attributes attributes;

    public PenroseAttributes(Attributes attributes) {
        this.attributes = attributes;
    }

    public Collection<String> getNames() throws Exception {
        return attributes.getNames();
    }

    public Collection<com.identyx.javabackend.Attribute> getAll() throws Exception {
        Collection<com.identyx.javabackend.Attribute> list = new ArrayList<com.identyx.javabackend.Attribute>();
        for (Attribute attribute : attributes.getAll()) {
            list.add(new PenroseAttribute(attribute));
        }
        return list;
    }

    public void add(com.identyx.javabackend.Attribute attribute) throws Exception {
        PenroseAttribute penroseAttribute = (PenroseAttribute)attribute;
        attributes.add(penroseAttribute.getAttribute());
    }

    public void set(com.identyx.javabackend.Attribute attribute) throws Exception {
        PenroseAttribute penroseAttribute = (PenroseAttribute)attribute;
        attributes.set(penroseAttribute.getAttribute());
    }

    public com.identyx.javabackend.Attribute get(String name) throws Exception {
        Attribute attribute = attributes.get(name);
        return new PenroseAttribute(attribute);
    }

    public Attributes getAttributes() {
        return attributes;
    }
}
