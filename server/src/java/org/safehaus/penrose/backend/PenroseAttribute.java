package org.safehaus.penrose.backend;

import com.identyx.javabackend.Attribute;

import java.util.Collection;
import java.util.ArrayList;

/**
 * @author Endi S. Dewata
 */
public class PenroseAttribute implements Attribute {

    String name;
    Collection values;

    public PenroseAttribute(String name) throws Exception {
        this.name = name;
        this.values = new ArrayList();
    }

    public PenroseAttribute(String name, Collection values) throws Exception {
        this.name = name;
        this.values = values;
    }

    public void setName(String name) throws Exception {
        this.name = name;
    }

    public String getName() throws Exception {
        return name;
    }

    public void addValue(Object value) throws Exception {
        values.add(value);
    }

    public void removeValue(Object value) throws Exception {
        values.remove(value);
    }

    public Object getValue() throws Exception {
        if (values == null || values.isEmpty()) return null;
        return values.iterator().next();
    }

    public Collection getValues() throws Exception {
        return values;
    }
}
