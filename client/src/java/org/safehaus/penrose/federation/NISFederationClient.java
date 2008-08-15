package org.safehaus.penrose.federation;

import org.safehaus.penrose.partition.*;
import org.safehaus.penrose.module.ModuleClient;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * @author Endi Sukma Dewata
 */
public class NISFederationClient {

    public Logger log = Logger.getLogger(getClass());

    public final static String NIS_TOOL              = "nis_tool";

    public final static String CACHE_USERS           = "cache_users";
    public final static String CACHE_GROUPS          = "cache_groups";
    public final static String CACHE_CONNECTION_NAME = "Cache";

    public final static String CHANGE_USERS          = "change_users";
    public final static String CHANGE_GROUPS         = "change_groups";

    public final static String LDAP_CONNECTION_NAME  = "LDAP";

    FederationClient federation;
    ModuleClient moduleClient;

    public NISFederationClient(FederationClient federation) {
        this.federation = federation;
        moduleClient = federation.getFederationModuleClient();
    }

    public void createYPPartition(String name) throws Exception {
        moduleClient.invoke(
                "createYPPartition",
                new Object[] { name },
                new String[] { String.class.getName() }
        );
    }

    public void removeYPPartition(String name) throws Exception {
        moduleClient.invoke(
                "removeYPPartition",
                new Object[] { name },
                new String[] { String.class.getName() }
        );
    }

    public void createNISPartition(String name) throws Exception {
        moduleClient.invoke(
                "createNISPartition",
                new Object[] { name },
                new String[] { String.class.getName() }
        );
    }

    public void removeNISPartition(String name) throws Exception {
        moduleClient.invoke(
                "removeNISPartition",
                new Object[] { name },
                new String[] { String.class.getName() }
        );
    }

    public void createNSSPartition(String name) throws Exception {
        moduleClient.invoke(
                "createNSSPartition",
                new Object[] { name },
                new String[] { String.class.getName() }
        );
    }

    public void removeNSSPartition(String name) throws Exception {
        moduleClient.invoke(
                "removeNSSPartition",
                new Object[] { name },
                new String[] { String.class.getName() }
        );
    }

    public void addRepository(NISDomain repository) throws Exception {
        federation.addRepository(repository);
        federation.storeFederationConfig();
    }

    public void updateRepository(NISDomain repository) throws Exception {

        federation.removePartitions(repository.getName());

        federation.removeRepository(repository.getName());
        federation.addRepository(repository);
        federation.storeFederationConfig();

        federation.createPartitions(repository.getName());
    }

    public void removeRepository(String name) throws Exception {
        federation.removeRepository(name);
        federation.storeFederationConfig();
    }

    public NISDomain getRepository(String name) throws Exception {
        return (NISDomain)federation.getRepository(name);
    }
    
    public Collection<String> getRepositoryNames() throws Exception {
        return federation.getRepositoryNames();
    }
    
    public Collection<NISDomain> getRepositories() throws Exception {
        Collection<NISDomain> list = new ArrayList<NISDomain>();
        for (Repository repository : federation.getRepositories("NIS")) {
            list.add((NISDomain)repository);
        }
        return list;
    }

    public void createDatabase(NISDomain domain, PartitionConfig nisPartitionConfig) throws Exception {
        moduleClient.invoke(
                "createDatabase",
                new Object[] { domain, nisPartitionConfig },
                new String[] { NISDomain.class.getName(), PartitionConfig.class.getName() }
        );
    }

    public void removeDatabase(NISDomain domain) throws Exception {
        moduleClient.invoke(
                "removeDatabase",
                new Object[] { domain },
                new String[] { NISDomain.class.getName() }
        );
    }

    public void createPartitions(String name) throws Exception {
        federation.createPartitions(name);
    }

    public void removePartitions(String name) throws Exception {
        federation.removePartitions(name);
    }
}
