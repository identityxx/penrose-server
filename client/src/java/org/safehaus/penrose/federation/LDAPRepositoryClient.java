package org.safehaus.penrose.federation;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/**
 * @author Endi Sukma Dewata
 */
public class LDAPRepositoryClient extends RepositoryClient {

    public Logger log = LoggerFactory.getLogger(getClass());

    public LDAPRepositoryClient(FederationClient federationClient, String repositoryName) throws Exception {
        super(federationClient, repositoryName, "LDAP");
    }
}
