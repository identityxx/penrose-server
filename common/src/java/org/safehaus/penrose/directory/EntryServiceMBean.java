package org.safehaus.penrose.directory;

import org.safehaus.penrose.directory.EntryConfig;
import org.safehaus.penrose.ldap.RDN;
import org.safehaus.penrose.ldap.DN;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public interface EntryServiceMBean {

    public DN getDn() throws Exception;
    public EntryConfig getEntryConfig() throws Exception;

    public String getParentId() throws Exception;
    public Collection<String> getChildIds() throws Exception;
}
