package org.safehaus.penrose.backend;

import com.identyx.javabackend.Attribute;

import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class PenroseAttribute implements Attribute {

    String name;
    Collection values;

    public PenroseAttribute(String name, Collection values) throws Exception {
        this.name = name;
        this.values = values;
    }

    public String getName() throws Exception {
        return name;
    }

    public Object getValue() throws Exception {
        if (values == null || values.isEmpty()) return null;
        return values.iterator().next();
    }

    public Collection getValues() throws Exception {
        return values;
    }
}
