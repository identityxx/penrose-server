package org.safehaus.penrose.backend;

import org.safehaus.penrose.entry.Attributes;
import org.safehaus.penrose.entry.Attribute;

import java.util.Collection;

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
