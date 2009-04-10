package org.safehaus.penrose.ldapbackend;

import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public interface RDN {

    public Collection<String> getNames() throws Exception;
    public Collection<Object> getValues(String name) throws Exception;
}
