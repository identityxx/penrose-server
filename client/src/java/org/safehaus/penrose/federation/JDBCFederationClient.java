package org.safehaus.penrose.federation;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/**
 * @author Endi Sukma Dewata
 */
public class JDBCFederationClient extends FederationRepositoryClient {

    public Logger log = LoggerFactory.getLogger(getClass());

    public JDBCFederationClient(FederationClient federationClient) throws Exception {
        super(federationClient, "JDBC");
    }
}