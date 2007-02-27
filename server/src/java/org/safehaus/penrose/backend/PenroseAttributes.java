package org.safehaus.penrose.backend;

import org.safehaus.penrose.entry.AttributeValues;

import java.util.Collection;

import com.identyx.javabackend.Attributes;
import com.identyx.javabackend.Attribute;

/**
 * @author Endi S. Dewata
 */
public class PenroseAttributes implements Attributes {

    AttributeValues attributeValues;

    public PenroseAttributes(AttributeValues attributeValues) {
        this.attributeValues = attributeValues;
    }

    public Collection getNames() throws Exception {
        return attributeValues.getNames();
    }

    public Attribute get(String name) throws Exception {
        Collection values = attributeValues.get(name);
        if (values == null) return null;
        return new PenroseAttribute(name, values);
    }
}
