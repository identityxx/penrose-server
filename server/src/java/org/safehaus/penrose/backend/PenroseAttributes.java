package org.safehaus.penrose.backend;

import java.util.Collection;
import java.util.ArrayList;

import javax.naming.directory.Attributes;
import javax.naming.directory.Attribute;
import javax.naming.NamingEnumeration;

/**
 * @author Endi S. Dewata
 */
public class PenroseAttributes implements com.identyx.javabackend.Attributes {

    Attributes attributes;

    public PenroseAttributes(Attributes attributeValues) {
        this.attributes = attributeValues;
    }

    public Collection getNames() throws Exception {
        Collection list = new ArrayList();
        for (NamingEnumeration e = attributes.getAll(); e.hasMore(); ) {
            Attribute attribute = (Attribute)e.next();
            list.add(attribute.getID());
        }
        return list;
    }

    public com.identyx.javabackend.Attribute get(String name) throws Exception {
        Attribute attribute = attributes.get(name);
        if (attribute == null) return null;
        return new PenroseAttribute(name, attribute);
    }
}
