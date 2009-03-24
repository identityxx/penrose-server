package org.safehaus.penrose.synchronization;

import org.safehaus.penrose.ldap.DN;

/**
 * @author Endi Sukma Dewata
 */
public interface SynchronizationModuleMBean {

    public void createBase() throws Exception;
    public void removeBase() throws Exception;

    public void create(DN dn) throws Exception;
    public void clear(DN dn) throws Exception;
    public void remove(DN dn) throws Exception;

    public SynchronizationResult synchronize(DN dn) throws Exception;
    public SynchronizationResult synchronize() throws Exception;

    public Long getSourceCount(DN dn) throws Exception;
    public Long getTargetCount(DN dn) throws Exception;
}
