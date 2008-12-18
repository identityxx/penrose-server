package org.safehaus.penrose.federation.module;

import org.safehaus.penrose.federation.FederationRepositoryConfig;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class JDBCRepositoryModule extends FederationRepositoryModule {

    public Collection<String> getRepositoryNames() throws Exception {
        return getRepositoryNames("JDBC");
    }

    public Collection<FederationRepositoryConfig> getRepositories() throws Exception {
        return getRepositories("JDBC");
    }
}