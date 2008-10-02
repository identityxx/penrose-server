package org.safehaus.penrose.backend;

import org.safehaus.penrose.ldap.Attributes;
import org.safehaus.penrose.ldap.Attribute;

import java.util.Collection;
import java.util.ArrayList;

/**
 * @author Endi S. Dewata
 */
public class PenroseAttributes implements org.safehaus.penrose.ldapbackend.Attributes {

    Attributes attributes;

    public PenroseAttributes(Attributes attributes) {
        this.attributes = attributes;
    }

    public Collection<String> getNames() throws Exception {
        return attributes.getNames();
    }

    public Collection<org.safehaus.penrose.ldapbackend.Attribute> getAll() throws Exception {
        Collection<org.safehaus.penrose.ldapbackend.Attribute> list = new ArrayList<org.safehaus.penrose.ldapbackend.Attribute>();
        for (Attribute attribute : attributes.getAll()) {
            list.add(new PenroseAttribute(attribute));
        }
        return list;
    }

    public void add(org.safehaus.penrose.ldapbackend.Attribute attribute) throws Exception {
        PenroseAttribute penroseAttribute = (PenroseAttribute)attribute;
        attributes.add(penroseAttribute.getAttribute());
    }

    public void set(org.safehaus.penrose.ldapbackend.Attribute attribute) throws Exception {
        PenroseAttribute penroseAttribute = (PenroseAttribute)attribute;
        attributes.set(penroseAttribute.getAttribute());
    }

    public org.safehaus.penrose.ldapbackend.Attribute get(String name) throws Exception {
        Attribute attribute = attributes.get(name);
        return new PenroseAttribute(attribute);
    }

    public Attributes getAttributes() {
        return attributes;
    }
}
