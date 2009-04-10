package org.safehaus.penrose.ldapbackend;

import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public interface Attribute {

    public String getName() throws Exception;

    public void addValue(Object value) throws Exception;
    public void removeValue(Object value) throws Exception;

    public Object getValue() throws Exception;
    public Collection<Object> getValues() throws Exception;
}
