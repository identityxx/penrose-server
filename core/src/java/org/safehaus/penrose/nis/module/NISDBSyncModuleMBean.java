package org.safehaus.penrose.nis.module;

import org.safehaus.penrose.ldap.DN;

/**
 * @author Endi Sukma Dewata
 */
public interface NISDBSyncModuleMBean {

    public void create(String targetDn) throws Exception;
    public void create(DN targetDn) throws Exception;

    public void load(String targetDn) throws Exception;
    public void load(DN targetDn) throws Exception;

    public void clear(String targetDn) throws Exception;
    public void clear(DN targetDn) throws Exception;

    public void remove(String targetDn) throws Exception;
    public void remove(DN targetDn) throws Exception;

    public long getCount(String targetDn) throws Exception;
    public long getCount(DN targetDn) throws Exception;
}
