package org.safehaus.penrose.backend;

import javax.naming.directory.Attribute;
import javax.naming.NamingEnumeration;
import java.util.Collection;
import java.util.ArrayList;

/**
 * @author Endi S. Dewata
 */
public class PenroseAttribute implements com.identyx.javabackend.Attribute {

    String name;
    Attribute attribute;

    public PenroseAttribute(String name, Attribute attribute) throws Exception {
        this.name = name;
        this.attribute = attribute;
    }

    public String getName() throws Exception {
        return name;
    }

    public Object getValue() throws Exception {
        return attribute.get();
    }

    public Collection getValues() throws Exception {
        Collection values = new ArrayList();
        for (NamingEnumeration e = attribute.getAll(); e.hasMore(); ) {
            Object value = e.next();
            values.add(value);
        }
        return values;
    }
}
