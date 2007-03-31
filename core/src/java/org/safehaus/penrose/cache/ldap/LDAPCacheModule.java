package org.safehaus.penrose.cache.ldap;

import org.safehaus.penrose.changelog.ChangeLogUtil;
import org.safehaus.penrose.cache.CacheModule;

/**
 * @author Endi S. Dewata
 */
public class LDAPCacheModule extends CacheModule {

    public ChangeLogUtil createChangeLogUtil() throws Exception {
        return new LDAPChangeLogUtil();
    }
}
