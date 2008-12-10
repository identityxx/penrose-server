package org.safehaus.penrose.ldap.module;

import org.safehaus.penrose.federation.SynchronizationResult;

/**
 * @author Endi Sukma Dewata
 */
public interface SyncModuleMBean {

    public SynchronizationResult synchronize() throws Exception;
}
