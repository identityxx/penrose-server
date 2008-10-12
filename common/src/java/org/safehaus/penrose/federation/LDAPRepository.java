package org.safehaus.penrose.federation;

/**
 * @author Endi Sukma Dewata
 */
public class LDAPRepository extends FederationRepositoryConfig {

    public final static String URL      = "url";
    public final static String SUFFIX   = "suffix";
    public final static String USER     = "user";
    public final static String PASSWORD = "password";

    public final static String ENABLED  = "enabled";
    public final static String TEMPLATE = "template";

    public LDAPRepository() {
        setType("LDAP");
    }

    public LDAPRepository(FederationRepositoryConfig repository) {
        super(repository);
    }
}
