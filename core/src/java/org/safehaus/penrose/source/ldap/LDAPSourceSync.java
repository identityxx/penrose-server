package org.safehaus.penrose.source.ldap;

import org.safehaus.penrose.changelog.ChangeLogUtil;

/**
 * @author Endi S. Dewata
 */
public class LDAPSourceSync extends org.safehaus.penrose.source.SourceSync {

    public ChangeLogUtil createChangeLogUtil() throws Exception {
        return new LDAPChangeLogUtil();
    }
}
