package org.safehaus.penrose.federation;

/**
 * @author Endi Sukma Dewata
 */
public class GlobalRepository extends Repository {

    public final static String LDAP_URL           = "ldapUrl";
    public final static String LDAP_SUFFIX        = "ldapSuffix";
    public final static String LDAP_USER          = "ldapUser";
    public final static String LDAP_PASSWORD      = "ldapPassword";

    public final static String SUFFIX             = "suffix";
    public final static String TEMPLATE           = "template";

    public GlobalRepository() {
    }

    public GlobalRepository(Repository repository) {
        super(repository);
    }
}
