package org.safehaus.penrose.federation.module;

import org.safehaus.penrose.module.Module;
import org.safehaus.penrose.federation.FederationReader;
import org.safehaus.penrose.federation.FederationConfig;
import org.safehaus.penrose.federation.FederationWriter;
import org.safehaus.penrose.federation.repository.Repository;

import java.io.File;
import java.util.Collection;
import java.util.ArrayList;

/**
 * @author Endi Sukma Dewata
 */
public class FederationModule extends Module {

    FederationConfig federationConfig = new FederationConfig();

    FederationReader reader = new FederationReader();
    FederationWriter writer = new FederationWriter();

    public void init() throws Exception {
        load();
    }

    public void load() throws Exception {

        File path = partition.getPartitionContext().getPath();
        File file = new File(path, "federation.xml");

        if (file.exists()) {
            log.debug("Loading "+file);
            reader.read(file, federationConfig);
        }
    }

    public void store() throws Exception {

        File path = partition.getPartitionContext().getPath();
        File file = new File(path, "federation.xml");

        log.debug("Storing "+file);
        writer.write(file, federationConfig);
    }

    public void clear() throws Exception {
        federationConfig.clear();
    }

    public FederationConfig getFederationConfig() throws Exception {
        return federationConfig;
    }

    public void setFederationConfig(FederationConfig federationConfig) throws Exception {
        this.federationConfig.clear();
        this.federationConfig.copy(federationConfig);
    }

    public Collection<Repository> getRepositories(String type) {

        Collection<Repository> list = new ArrayList<Repository>();

        for (Repository repository : federationConfig.getRepositories()) {
            if (type.equals(repository.getType())) {
                list.add(repository);
            }
        }

        return list;
    }
}
