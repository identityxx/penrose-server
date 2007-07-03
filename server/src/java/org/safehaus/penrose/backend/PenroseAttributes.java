package org.safehaus.penrose.backend;

import org.safehaus.penrose.ldap.Attributes;
import org.safehaus.penrose.ldap.Attribute;

import java.util.Collection;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class PenroseAttributes implements com.identyx.javabackend.Attributes {

    Attributes attributes;

    public PenroseAttributes(Attributes attributes) {
        this.attributes = attributes;
    }

    public Collection getNames() throws Exception {
        return attributes.getNames();
    }

    public Collection getAll() throws Exception {
        Collection list = new ArrayList();
        for (Iterator i=attributes.getAll().iterator(); i.hasNext(); ) {
            Attribute attribute = (Attribute)i.next();
            list.add(new PenroseAttribute(attribute));
        }
        return list;
    }

    public void add(com.identyx.javabackend.Attribute attribute) throws Exception {
        PenroseAttribute penroseAttribute = (PenroseAttribute)attribute;
        attributes.add(penroseAttribute.getAttribute());
    }

    public com.identyx.javabackend.Attribute get(String name) throws Exception {
        Attribute attribute = attributes.get(name);
        return new PenroseAttribute(attribute);
    }

    public Attributes getAttributes() {
        return attributes;
    }
}
