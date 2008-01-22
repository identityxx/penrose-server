package org.safehaus.penrose.management;

import org.safehaus.penrose.directory.EntryMapping;
import org.safehaus.penrose.ldap.RDN;
import org.safehaus.penrose.ldap.DN;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public interface EntryServiceMBean {
    public DN getDn() throws Exception;
    public EntryMapping getEntryMapping() throws Exception;
    public Collection<String> getChildIds(RDN rdn) throws Exception;
}
