package org.safehaus.penrose.ldapbackend;

import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public interface DN {
    public RDN getRdn() throws Exception;
    public Collection<RDN> getRdns() throws Exception;
}
