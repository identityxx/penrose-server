package org.safehaus.penrose.ldapbackend;

import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public interface Attributes {

    public Collection<String> getNames() throws Exception;
    public Collection<Attribute> getAll() throws Exception;

    public void set(Attribute attribute) throws Exception;
    public void add(Attribute attribute) throws Exception;
    public Attribute get(String name) throws Exception;
}
