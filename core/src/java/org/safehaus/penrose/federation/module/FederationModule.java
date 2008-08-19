package org.safehaus.penrose.federation.module;

import org.apache.tools.ant.filters.ExpandProperties;
import org.apache.tools.ant.taskdefs.Copy;
import org.apache.tools.ant.types.FileSet;
import org.apache.tools.ant.types.FilterChain;
import org.safehaus.penrose.federation.*;
import org.safehaus.penrose.jdbc.connection.JDBCConnection;
import org.safehaus.penrose.jdbc.source.JDBCSource;
import org.safehaus.penrose.module.Module;
import org.safehaus.penrose.nis.module.NISLDAPSyncModule;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.util.FileUtil;

import java.io.File;
import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class FederationModule extends Module implements FederationMBean {

    public final static String FEDERATION = "federation";

    public final static String TEMPLATE   = "template";
    public final static String SUFFIX     = "suffix";

    public final static String GLOBAL     = "global";

    public final static String LDAP       = "ldap";
    public final static String YP         = "yp";
    public final static String NIS        = "nis";
    public final static String NSS        = "nss";

    public final static String JDBC       = "JDBC";

    public final static String CACHE_CONNECTION_NAME = "Cache";

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

        File home = partition.getPartitionContext().getPenroseContext().getHome();
        samplesDir = new File(home, "samples");

        PartitionManager partitionManager = partition.getPartitionContext().getPenroseContext().getPartitionManager();
        partitionsDir = partitionManager.getPartitionsDir();

        if (create) {
            createPartitions();
        }
    }

    public void load() throws Exception {

        File path = partition.getPartitionContext().getPath();

        File dir = path == null ?
                new File(partition.getPartitionContext().getPenroseContext().getHome(), "conf") :
                path;
        File file = new File(dir, config);

        if (file.exists()) {
            log.debug("Loading "+file);
            reader.read(file, federationConfig);
        }
    }

    public void store() throws Exception {

        File path = partition.getPartitionContext().getPath();
        File dir = path == null ?
                new File(partition.getPartitionContext().getPenroseContext().getHome(), "conf") :
                path;
        File file = new File(dir, "federation.xml");

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

    public void createGlobalPartition() throws Exception {

        GlobalRepository repository = (GlobalRepository)federationConfig.getRepository(GLOBAL);
        if (repository == null) return;

        String partitionName = GLOBAL;
        File partitionDir = new File(partitionsDir, partitionName);

        if (partitionDir.exists()) {
            log.debug("Partition "+partitionName+" already exists.");
            return;
        }

        String templateName = repository.getParameter(TEMPLATE);
        File templateDir;
        if (templateName == null) {
            templateDir = new File(samplesDir, FEDERATION+"_"+GLOBAL);
        } else {
            templateDir = new File(templateName);
        }

        log.debug("Creating partition "+partitionName+" from "+templateName+".");

        org.apache.tools.ant.Project antProject = new org.apache.tools.ant.Project();

        antProject.setProperty("LDAP_URL",      repository.getParameter(GlobalRepository.LDAP_URL));
        antProject.setProperty("LDAP_SUFFIX",   repository.getParameter(GlobalRepository.LDAP_SUFFIX));
        antProject.setProperty("LDAP_USER",     repository.getParameter(GlobalRepository.LDAP_USER));
        antProject.setProperty("LDAP_PASSWORD", repository.getParameter(GlobalRepository.LDAP_PASSWORD));

        antProject.setProperty("SUFFIX",        repository.getParameter(GlobalRepository.SUFFIX));

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

    public void startGlobalPartition() throws Exception {
        startPartition(GLOBAL);
    }

    public void startLDAPPartitions(String name) throws Exception {
        startPartition(name);
    }

    public void startPartition(String partitionName) throws Exception {

        File partitionDir = new File(partitionsDir, partitionName);

        PartitionConfig partitionConfig = new PartitionConfig(partitionName);
        partitionConfig.load(partitionDir);

        PartitionManager partitionManager = partition.getPartitionContext().getPenroseContext().getPartitionManager();
        partitionManager.addPartitionConfig(partitionConfig);
        partitionManager.startPartition(partitionName);
    }

    public void stopGlobalPartition() throws Exception {
        stopPartition(GLOBAL);
    }

    public void removeGlobalPartition() throws Exception {
        removePartition(GLOBAL);
    }

    public void createLDAPPartitions(String name) throws Exception {

        LDAPRepository repository = (LDAPRepository)federationConfig.getRepository(name);

        String partitionName = repository.getName();
        File partitionDir = new File(partitionsDir, partitionName);

        if (partitionDir.exists()) {
            log.debug("Partition "+partitionName+" already exists.");
            return;
        }

        String templateName = repository.getParameter(TEMPLATE);
        File templateDir;
        if (templateName == null) {
            templateDir = new File(samplesDir, FEDERATION+"_"+LDAP);
        } else {
            templateDir = new File(templateName);
        }

        log.debug("Creating partition "+partitionName+" from "+templateName+".");

        org.apache.tools.ant.Project antProject = new org.apache.tools.ant.Project();

        antProject.setProperty("LDAP_URL",      repository.getParameter(LDAPRepository.LDAP_URL));
        antProject.setProperty("LDAP_SUFFIX",   repository.getParameter(LDAPRepository.LDAP_SUFFIX));
        antProject.setProperty("LDAP_USER",     repository.getParameter(LDAPRepository.LDAP_USER));
        antProject.setProperty("LDAP_PASSWORD", repository.getParameter(LDAPRepository.LDAP_PASSWORD));

        antProject.setProperty("SUFFIX",        repository.getParameter(LDAPRepository.SUFFIX));

        for (String paramName : repository.getParameterNames()) {
            String paramValue = repository.getParameter(paramName);
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

    public void stopLDAPPartitions(String partitionName) throws Exception {
        stopPartition(partitionName);
    }

    public void removeLDAPPartitions(String partitionName) throws Exception {
        removePartition(partitionName);
    }

    public void createNISPartitions(String name) throws Exception {
        createYPPartition(name);
        createNISPartition(name);
        createNSSPartition(name);
    }

    public void startNISPartitions(String name) throws Exception {
        startYPPartition(name);
        startNISPartition(name);
        startNSSPartition(name);
    }

    public void stopNISPartitions(String domainName) throws Exception {
        stopNSSPartition(domainName);
        stopNISPartition(domainName);
        stopYPPartition(domainName);
    }

    public void removeNISPartitions(String domainName) throws Exception {
        removeNSSPartition(domainName);
        removeNISPartition(domainName);
        removeYPPartition(domainName);
    }

    public void createYPPartition(String name) throws Exception {

        NISDomain domain = (NISDomain)federationConfig.getRepository(name);
        if (domain == null) return;
        if (!domain.getBooleanParameter(NISDomain.YP_ENABLED)) return;

        String partitionName = name+"_"+YP;
        File partitionDir = new File(partitionsDir, partitionName);

        if (partitionDir.exists()) {
            log.debug("Partition "+partitionName+" already exists.");
            return;
        }

        String templateName = domain.getParameter(NISDomain.YP_TEMPLATE);
        File templateDir;
        if (templateName == null) {
            templateDir = new File(samplesDir, FEDERATION+"_"+YP);
        } else {
            templateDir = new File(templateName);
        }

        log.debug("Creating partition "+partitionName+" from "+templateName+".");

        org.apache.tools.ant.Project antProject = new org.apache.tools.ant.Project();

        antProject.setProperty("DOMAIN",       name);

        antProject.setProperty("NIS_SERVER",   domain.getParameter(NISDomain.NIS_SERVER));
        antProject.setProperty("NIS_DOMAIN",   domain.getParameter(NISDomain.NIS_DOMAIN));

        antProject.setProperty("SUFFIX",       domain.getParameter(NISDomain.YP_SUFFIX));

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

    public void startYPPartition(String name) throws Exception {
        startPartition(name+"_"+YP);
    }

    public void stopYPPartition(String name) throws Exception {
        stopPartition(name+"_"+YP);
    }

    public void removeYPPartition(String name) throws Exception {
        removePartition(name+"_"+YP);
    }

    public void createNISPartition(String name) throws Exception {

        NISDomain domain = (NISDomain)federationConfig.getRepository(name);
        if (domain == null) return;
        if (!domain.getBooleanParameter(NISDomain.NIS_ENABLED)) return;

        String partitionName = name+"_"+NIS;
        File partitionDir = new File(partitionsDir, partitionName);

        if (partitionDir.exists()) {
            log.debug("Partition "+partitionName+" already exists.");
            return;
        }

        String templateName = domain.getParameter(NISDomain.NIS_TEMPLATE);
        File templateDir;
        if (templateName == null) {
            templateDir = new File(samplesDir, FEDERATION+"_"+NIS);
        } else {
            templateDir = new File(templateName);
        }

        log.debug("Creating partition "+partitionName+" from "+templateName+".");

        org.apache.tools.ant.Project antProject = new org.apache.tools.ant.Project();

        antProject.setProperty("DOMAIN",        name);

        antProject.setProperty("NIS_DOMAIN",    domain.getParameter(NISDomain.NIS_DOMAIN));

        antProject.setProperty("SUFFIX",        domain.getParameter(NISDomain.NIS_SUFFIX));

        for (String paramName : domain.getParameterNames()) {
            String paramValue = domain.getParameter(paramName);
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

    public void startNISPartition(String name) throws Exception {
        startPartition(name+"_"+NIS);
    }

    public void stopNISPartition(String name) throws Exception {
        stopPartition(name+"_"+NIS);
    }

    public void removeNISPartition(String name) throws Exception {
        removePartition(name+"_"+NIS);
    }

    public void createNSSPartition(String name) throws Exception {

        NISDomain domain = (NISDomain)federationConfig.getRepository(name);
        if (domain == null) return;
        if (!domain.getBooleanParameter(NISDomain.NSS_ENABLED)) return;

        String partitionName = name+"_"+NSS;
        File partitionDir = new File(partitionsDir, partitionName);

        if (partitionDir.exists()) {
            log.debug("Partition "+partitionName+" already exists.");
            return;
        }

        String templateName = domain.getParameter(NISDomain.NSS_TEMPLATE);
        File templateDir;
        if (templateName == null) {
            templateDir = new File(samplesDir, FEDERATION+"_"+NSS);
        } else {
            templateDir = new File(templateName);
        }

        log.debug("Creating partition "+partitionName+" from "+templateName+".");

        org.apache.tools.ant.Project antProject = new org.apache.tools.ant.Project();

        antProject.setProperty("DOMAIN",        name);

        antProject.setProperty("NIS_DOMAIN",    domain.getParameter(NISDomain.NIS_DOMAIN));
        antProject.setProperty("NIS_SUFFIX",    domain.getParameter(NISDomain.NIS_SUFFIX));

        antProject.setProperty("SUFFIX",        domain.getParameter(NISDomain.NSS_SUFFIX));

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

    public void startNSSPartition(String name) throws Exception {
        startPartition(name+"_"+NSS);
    }

    public void stopNSSPartition(String name) throws Exception {
        stopPartition(name+"_"+NSS);
    }
    
    public void removeNSSPartition(String name) throws Exception {
        removePartition(name+"_"+NSS);
    }

    public void stopPartition(String partitionName) throws Exception {

        log.debug("Removing partition "+partitionName+".");

        PartitionManager partitionManager = partition.getPartitionContext().getPenroseContext().getPartitionManager();

        partitionManager.stopPartition(partitionName);
        partitionManager.unloadPartition(partitionName);
    }

    public void removePartition(String partitionName) {

        File partitionDir = new File(partitionsDir, partitionName);

        FileUtil.delete(partitionDir);
    }

    public Collection<String> getRepositoryNames() throws Exception {
        return federationConfig.getRepositoryNames();
    }

    public Collection<Repository> getRepositories() throws Exception {
        return federationConfig.getRepositories();
    }

    public Collection<Repository> getRepositories(String type) {
        return federationConfig.getRepositories(type);
    }

    public Repository getRepository(String name) throws Exception {
        return federationConfig.getRepository(name);
    }

    public void addRepository(Repository repository) throws Exception {
        federationConfig.addRepository(repository);
    }

    public void removeRepository(String name) throws Exception {
        federationConfig.removeRepository(name);
    }

    public void updateGlobalRepository(Repository repository) throws Exception {

        log.debug("Updating global repository.");

        if (getRepository(GLOBAL) != null) {
            stopGlobalPartition();
            removeGlobalPartition();
        }

        addRepository(repository);

        store();

        createGlobalPartition();
        startGlobalPartition();
    }

    public void createDatabase(NISDomain domain, PartitionConfig nisPartitionConfig) throws Exception {

        log.debug("Creating database "+domain.getName()+".");

        PartitionManager partitionManager = partition.getPartitionContext().getPenroseContext().getPartitionManager();

        Partition nisPartition = partitionManager.getPartition(domain.getName()+" "+NIS);
        JDBCSource source = (JDBCSource)nisPartition.getSourceManager().getSource(CACHE_CONNECTION_NAME);
        JDBCConnection connection = (JDBCConnection)source.getConnection();

        try {
            connection.createDatabase(domain.getName());
        } catch (Exception e) {
            log.debug(e.getMessage());
        }

        try {
            source.create();
        } catch (Exception e) {
            log.debug(e.getMessage());
        }
    }

    public void removeDatabase(NISDomain domain) throws Exception {

        log.debug("Removing cache "+domain.getName()+".");

        PartitionManager partitionManager = partition.getPartitionContext().getPenroseContext().getPartitionManager();

        Partition federationPartition = partitionManager.getPartition(FEDERATION);
        JDBCConnection connection = (JDBCConnection)federationPartition.getConnectionManager().getConnection(JDBC);

        connection.dropDatabase(domain.getName());
    }

    public void createPartitions() throws Exception {
        for (String name : getRepositoryNames()) {
            createPartitions(name);
        }
    }

    public void startPartitions() throws Exception {
        for (String name : getRepositoryNames()) {
            startPartitions(name);
        }
    }

    public void stopPartitions() throws Exception {
        for (String name : getRepositoryNames()) {
            stopPartitions(name);
        }
    }

    public void removePartitions() throws Exception {
        for (String name : getRepositoryNames()) {
            removePartitions(name);
        }
    }

    public void createPartitions(String name) throws Exception {
        Repository repository = federationConfig.getRepository(name);
        if (repository == null) return;

        if ("GLOBAL".equals(repository.getType())) {
            createGlobalPartition();

        } else if ("LDAP".equals(repository.getType())) {
            createLDAPPartitions(name);

        } else if ("NIS".equals(repository.getType())) {
            createNISPartitions(name);
        }
    }

    public void startPartitions(String name) throws Exception {
        Repository repository = federationConfig.getRepository(name);
        if (repository == null) return;

        if ("GLOBAL".equals(repository.getType())) {
            startGlobalPartition();

        } else if ("LDAP".equals(repository.getType())) {
            startLDAPPartitions(name);

        } else if ("NIS".equals(repository.getType())) {
            startNISPartitions(name);
        }
    }

    public void stopPartitions(String name) throws Exception {
        Repository repository = federationConfig.getRepository(name);
        if (repository == null) return;

        if ("GLOBAL".equals(repository.getType())) {
            stopGlobalPartition();

        } else if ("LDAP".equals(repository.getType())) {
            stopLDAPPartitions(name);

        } else if ("NIS".equals(repository.getType())) {
            stopNISPartitions(name);
        }
    }

    public void removePartitions(String name) throws Exception {
        Repository repository = federationConfig.getRepository(name);
        if (repository == null) return;

        if ("GLOBAL".equals(repository.getType())) {
            removeGlobalPartition();

        } else if ("LDAP".equals(repository.getType())) {
            removeLDAPPartitions(name);

        } else if ("NIS".equals(repository.getType())) {
            removeNISPartitions(name);
        }
    }

    public void synchronize(String name) throws Exception {
        Repository repository = federationConfig.getRepository(name);
        if (repository == null) return;

        if ("GLOBAL".equals(repository.getType())) {

        } else if ("LDAP".equals(repository.getType())) {

        } else if ("NIS".equals(repository.getType())) {

            Partition nisPartition = partition.getPartitionContext().getPartition(name+"_"+NIS);

            NISLDAPSyncModule module = (NISLDAPSyncModule)nisPartition.getModuleManager().getModule("NISLDAPSyncModule");
            module.synchronize();
        }
    }

    public void synchronizeNISMaps(String name, Collection<String> maps) throws Exception {

        Repository repository = federationConfig.getRepository(name);
        if (repository == null) return;

        Partition nisPartition = partition.getPartitionContext().getPartition(name+"_"+NIS);

        NISLDAPSyncModule module = (NISLDAPSyncModule)nisPartition.getModuleManager().getModule("NISLDAPSyncModule");

        if (maps == null || maps.isEmpty()) {
            module.synchronize();

        } else {
            for (String map : maps) {
                module.synchronizeMap(map);
            }
        }
    }
}
