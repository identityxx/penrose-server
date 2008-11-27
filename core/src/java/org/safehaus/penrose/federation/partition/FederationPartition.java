package org.safehaus.penrose.federation.partition;

import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.federation.*;
import org.safehaus.penrose.federation.module.FederationModule;
import org.safehaus.penrose.util.FileUtil;
import org.safehaus.penrose.module.Module;
import org.safehaus.penrose.module.ModuleManager;
import org.safehaus.penrose.ldap.module.SnapshotSyncModule;
import org.apache.tools.ant.taskdefs.Copy;
import org.apache.tools.ant.types.FileSet;
import org.apache.tools.ant.types.FilterChain;
import org.apache.tools.ant.filters.ExpandProperties;

import java.io.File;
import java.util.Collection;
import java.util.ArrayList;

/**
 * @author Endi Sukma Dewata
 */
public class FederationPartition extends Partition implements FederationMBean {

    FederationConfig federationConfig = new FederationConfig();

    FederationReader reader = new FederationReader();
    FederationWriter writer = new FederationWriter();

    String config = "federation.xml";
    boolean create = true;

    File samplesDir;
    File partitionsDir;

    public void init() throws Exception {

        String s = getParameter("config");
        if (s != null) config = s;

        s = getParameter("create");
        if (s != null) create = Boolean.parseBoolean(s);

        load();

        File home = partitionContext.getPenroseContext().getHome();
        samplesDir = new File(home, "samples");

        PartitionManager partitionManager = partitionContext.getPartitionManager();
        partitionsDir = partitionManager.getPartitionsDir();

        if (create) {
            createPartitions();
            //startPartitions();
        }
    }

    public Collection<String> getTypes() {
        Collection<String> types = new ArrayList<String>();

        for (Module module : getModuleManager().getModules()) {
            if (!(module instanceof FederationModule)) continue;

            types.add(module.getName());
        }

        return types;
    }

    public void load() throws Exception {

        File path = partitionContext.getPath();

        File dir = path == null ?
                new File(partitionContext.getPenroseContext().getHome(), "conf") :
                new File(path, "DIR-INF");
        File file = new File(dir, config);

        if (file.exists()) {
            log.debug("Loading "+file);
            reader.read(file, federationConfig);
        }
    }

    public void store() throws Exception {

        File path = partitionContext.getPath();

        File dir = path == null ?
                new File(partitionContext.getPenroseContext().getHome(), "conf") :
                new File(path, "DIR-INF");
        File file = new File(dir, config);

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

    public void startPartition(String partitionName) throws Exception {

        PartitionManager partitionManager = partitionContext.getPartitionManager();
        partitionManager.loadPartition(partitionName);
        partitionManager.startPartition(partitionName);
    }

    public void stopPartition(String partitionName) throws Exception {

        PartitionManager partitionManager = partitionContext.getPartitionManager();
        partitionManager.stopPartition(partitionName);
        partitionManager.removePartition(partitionName);
    }

    public void removePartition(String partitionName) {

        File partitionDir = new File(partitionsDir, partitionName);
        FileUtil.delete(partitionDir);
    }

    public Collection<String> getRepositoryNames(String type) throws Exception {
        return federationConfig.getRepositoryNames(type);
    }

    public Collection<String> getRepositoryNames() throws Exception {
        return federationConfig.getRepositoryNames();
    }

    public Collection<FederationRepositoryConfig> getRepositories() throws Exception {
        return federationConfig.getRepositories();
    }

    public Collection<FederationRepositoryConfig> getRepositories(String type) {
        return federationConfig.getRepositories(type);
    }

    public FederationRepositoryConfig getRepository(String name) throws Exception {
        return federationConfig.getRepository(name);
    }

    public void addRepository(FederationRepositoryConfig repository) throws Exception {
        federationConfig.addRepository(repository);
    }

    public void removeRepository(String name) throws Exception {
        federationConfig.removeRepository(name);
    }

    public void createPartitions() throws Exception {
        PartitionManager partitionManager = getPartitionContext().getPartitionManager();
        for (String name : getPartitionNames()) {
            createPartition(name);
            partitionManager.getQueue().add(name);
        }
    }

    public void startPartitions() throws Exception {
        for (String name : getPartitionNames()) {
            startPartition(name);
        }
    }

    public void stopPartitions() throws Exception {
        for (String name : getPartitionNames()) {
            stopPartition(name);
        }
    }

    public void removePartitions() throws Exception {
        for (String name : getPartitionNames()) {
            removePartition(name);
        }
    }

    public void createPartition(String partitionName) throws Exception {

        FederationPartitionConfig partitionConfig = federationConfig.getPartition(partitionName);
        if (partitionConfig == null) {
            log.debug(partitionName+" partition undefined.");
            return;
        }

        File partitionDir = new File(partitionsDir, partitionName);

        if (partitionDir.exists()) {
            log.debug(partitionName+" partition already exists.");
            return;
        }

        String template = partitionConfig.getTemplate();
        File templateDir = new File(template);

        log.debug("Creating "+partitionName+" partition from "+template+".");

        org.apache.tools.ant.Project antProject = new org.apache.tools.ant.Project();

        for (String refName : partitionConfig.getRepositoryRefNames()) {
            String repositoryName = partitionConfig.getRepository(refName);
            log.debug(" - Repository "+refName+": "+repositoryName);

            antProject.setProperty(refName+".name", repositoryName);

            FederationRepositoryConfig repositoryConfig = federationConfig.getRepository(repositoryName);

            for (String paramName : repositoryConfig.getParameterNames()) {
                String paramValue = repositoryConfig.getParameter(paramName);
                log.debug("   - "+paramName+": "+paramValue);
                
                antProject.setProperty(refName+"."+paramName, paramValue);
            }
        }

        for (String paramName : partitionConfig.getParameterNames()) {
            String paramValue = partitionConfig.getParameter(paramName);
            log.debug(" - "+paramName+": "+paramValue);

            antProject.setProperty(paramName, paramValue);
        }

        Copy copy = new Copy();
        copy.setOverwrite(true);
        copy.setProject(antProject);

        FileSet fs = new FileSet();
        fs.setDir(templateDir);
        fs.setIncludes("**/*");
        copy.addFileset(fs);

        copy.setTodir(partitionDir);

        FilterChain filterChain = copy.createFilterChain();
        ExpandProperties expandProperties = new ExpandProperties();
        expandProperties.setProject(antProject);
        filterChain.addExpandProperties(expandProperties);

        copy.execute();
    }

    public void synchronize(String name) throws Exception {
        FederationRepositoryConfig repository = federationConfig.getRepository(name);
        if (repository == null) return;

        PartitionManager partitionManager = partitionContext.getPartitionManager();
        Partition partition = partitionManager.getPartition(name);
        if (partition == null) return;

        ModuleManager moduleManager = partition.getModuleManager();
        SnapshotSyncModule module = (SnapshotSyncModule)moduleManager.getModule(Federation.SYNCHRONIZATION_MODULE);
        if (module == null) return;

        module.synchronize();
    }

    public Collection<String> getPartitionNames() {
        return federationConfig.getPartitionNames();
    }

    public Collection<FederationPartitionConfig> getPartitions() {
        return federationConfig.getPartitions();
    }

    public FederationPartitionConfig getPartition(String name) {
        return federationConfig.getPartition(name);
    }
}
