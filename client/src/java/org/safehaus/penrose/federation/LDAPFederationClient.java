package org.safehaus.penrose.federation;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class LDAPFederationClient {

    public final static String LOCAL_CONNECTION = "LDAP";
    public final static String LOCAL_SOURCE = "Local";

    public final static String GLOBAL_CONNECTION = "Global";
    public final static String GLOBAL_SOURCE = "Global";

    public Logger log = Logger.getLogger(getClass());

    private FederationClient federation;

    public LDAPFederationClient(FederationClient federation) {
        this.federation = federation;
    }

    public LDAPRepository getRepository(String name) throws Exception {
        return (LDAPRepository) federation.getRepository(name);
    }

    public Collection<LDAPRepository> getRepositories() throws Exception {
        Collection<LDAPRepository> list = new ArrayList<LDAPRepository>();
        for (Repository repository : federation.getRepositories("LDAP")) {
            list.add((LDAPRepository)repository);
        }
        return list;
    }

    public void addRepository(LDAPRepository repository) throws Exception {
        federation.addRepository(repository);
        federation.storeFederationConfig();
    }

    public void updateRepository(LDAPRepository repository) throws Exception {

        String name = repository.getName();

        federation.stopPartitions(name);
        federation.removePartitions(name);

        federation.removeRepository(name);
        federation.addRepository(repository);
        federation.storeFederationConfig();

        federation.createPartitions(name);
        federation.startPartitions(name);
    }

    public void removeRepository(String name) throws Exception {
        federation.removeRepository(name);
        federation.storeFederationConfig();
    }

    public void createPartitions(String name) throws Exception {
        federation.createPartitions(name);
        federation.startPartitions(name);
    }

    public void removePartitions(String name) throws Exception {
        federation.stopPartitions(name);
        federation.removePartitions(name);
    }
}
