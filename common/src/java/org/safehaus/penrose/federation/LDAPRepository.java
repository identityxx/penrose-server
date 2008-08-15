package org.safehaus.penrose.federation;

import java.util.Map;

/**
 * @author Endi Sukma Dewata
 */
public class LDAPRepository extends Repository {

    public final static String LDAP_URL           = "ldapUrl";
    public final static String LDAP_SUFFIX        = "ldapSuffix";
    public final static String LDAP_USER          = "ldapUser";
    public final static String LDAP_PASSWORD      = "ldapPassword";

    public final static String ENABLED            = "enabled";
    public final static String SUFFIX             = "suffix";
    public final static String TEMPLATE           = "template";

    public LDAPRepository() {
        setType("LDAP");
    }

    public LDAPRepository(Repository repository) {
        super(repository);
    }
}
