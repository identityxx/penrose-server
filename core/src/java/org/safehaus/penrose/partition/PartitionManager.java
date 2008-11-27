package org.safehaus.penrose.partition;

import org.safehaus.penrose.adapter.AdapterConfig;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.connection.ConnectionConfig;
import org.safehaus.penrose.directory.Directory;
import org.safehaus.penrose.directory.Entry;
import org.safehaus.penrose.directory.EntrySourceConfig;
import org.safehaus.penrose.ldap.DN;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.partition.event.PartitionEvent;
import org.safehaus.penrose.partition.event.PartitionListener;
import org.safehaus.penrose.source.SourceConfig;
import org.safehaus.penrose.util.TextUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.net.URL;
import java.net.URLClassLoader;

/**
 * @author Endi S. Dewata
 */
public class PartitionManager {

    public Logger log      = LoggerFactory.getLogger(getClass());
    public Logger errorLog = org.safehaus.penrose.log.Error.log;
    public boolean debug   = log.isDebugEnabled();

    protected File home;
    protected File partitionsDir;
    protected File confDir;

    PenroseConfig penroseConfig;
    PenroseContext penroseContext;

    Map<String,Partition> partitions = new LinkedHashMap<String,Partition>();
    PartitionConfigManager partitionConfigManager = new PartitionConfigManager();

    Queue<String> queue = new LinkedList<String>();
    Collection<PartitionListener> listeners = new LinkedHashSet<PartitionListener>();

    public PartitionManager(File home, PenroseConfig penroseConfig, PenroseContext penroseContext) {
        this.home           = home;
        this.penroseConfig  = penroseConfig;
        this.penroseContext = penroseContext;

        this.partitionsDir  = new File(home, "partitions");
        this.confDir        = new File(home, "conf");
    }

    public PartitionConfigManager getPartitionConfigManager() {
        return partitionConfigManager;
    }

    public void addPartitionConfig(PartitionConfig partitionConfig) throws Exception {
        log.debug("Adding partition config "+partitionConfig.getName()+".");
        partitionConfigManager.addPartitionConfig(partitionConfig);

        PartitionEvent event = new PartitionEvent(PartitionEvent.PARTITION_ADDED, partitionConfig);
        for (PartitionListener listener : listeners) {
            listener.partitionAdded(event);
        }
    }

    public File getPartitionsDir() {
        return partitionsDir;
    }

    public Queue<String> getQueue() {
        return queue;
    }

    public void startPartitions() throws Exception {
        
        loadDefaultPartition();

        startPartition("DEFAULT");

        for (String partitionName : getAvailablePartitionNames()) {
            try {
                loadPartition(partitionName);

            } catch (Exception e) {
                errorLog.error("Failed loading partition "+partitionName+".", e);
            }
        }

        Collection<String> partitionNames = new ArrayList<String>();
        partitionNames.addAll(partitionConfigManager.getPartitionNames());

        for (String partitionName : partitionNames) {
            if ("DEFAULT".equals(partitionName)) continue;
            
            try {
                startPartition(partitionName);

            } catch (Exception e) {
                errorLog.error("Failed starting partition "+partitionName+".", e);
            }
        }

        while (!queue.isEmpty()) {

            String partitionName = queue.remove();

            try {
                loadPartition(partitionName);

            } catch (Exception e) {
                errorLog.error("Failed loading partition "+partitionName+".", e);
                continue;
            }

            try {
                startPartition(partitionName);

            } catch (Exception e) {
                errorLog.error("Failed starting partition "+partitionName+".", e);
            }
        }
    }

    public void loadDefaultPartition() throws Exception {

        if (debug) {
            log.debug(TextUtil.repeat("-", 70));
            log.debug("Loading default partition.");
        }

        DefaultPartitionConfig partitionConfig = new DefaultPartitionConfig();

        for (AdapterConfig adapterConfig : penroseConfig.getAdapterConfigs()) {
            partitionConfig.addAdapterConfig(adapterConfig);
        }

        partitionConfig.load(home);

        partitionConfigManager.addPartitionConfig(partitionConfig);

        if (debug) log.debug("Default partition loaded.");
    }

    public PartitionConfig loadPartition(String partitionName) throws Exception {

        PartitionConfig partitionConfig = partitionConfigManager.getPartitionConfig(partitionName);
        if (partitionConfig != null) {
            log.debug("Partition "+partitionName+" is already loaded.");
            return partitionConfig;
        }

        if (debug) {
            log.debug(TextUtil.repeat("-", 70));
            log.debug("Loading partition "+partitionName+".");
        }

        File partitionDir = new File(partitionsDir, partitionName);

        partitionConfig = new PartitionConfig(partitionName);
        partitionConfig.load(partitionDir);

        addPartitionConfig(partitionConfig);

        if (debug) log.debug("Partition "+partitionName+" loaded.");

        return partitionConfig;
    }

    public void startPartition(String name) throws Exception {

        Partition partition = partitions.get(name);
        if (partition != null) {
            log.debug("Partition "+name+" already started.");
            return;
        }

        PartitionConfig partitionConfig = partitionConfigManager.getPartitionConfig(name);
        if (partitionConfig == null) {
            log.error("Can't start partition "+name+": Partition not found.");
            return;
        }

        if (!partitionConfig.isEnabled()) {
            log.debug("Partition "+name+" disabled.");
            return;
        }

        for (String depend : partitionConfig.getDepends()) {
            if (partitionConfigManager.getPartitionConfig(depend) == null) {
                log.error("Can't start partition "+name+": Missing dependency ["+depend+"].");
                return;
            }

            log.debug("Partition "+name+" is dependent on partition "+depend+".");
            startPartition(depend);
        }

        if (debug) {
            log.debug(TextUtil.repeat("-", 70));
            log.debug("Starting partition "+name+".");
        }

        partition = createPartition(partitionConfig);

        PartitionEvent event = new PartitionEvent(PartitionEvent.PARTITION_STARTED, partition);
        for (PartitionListener listener : listeners) {
            listener.partitionStarted(event);
        }

        log.debug("Partition "+name+" started.");
    }

    public ClassLoader createClassLoader(PartitionConfig partitionConfig) throws Exception {
        Collection<URL> classPaths = partitionConfig.getClassPaths();
        return new URLClassLoader(classPaths.toArray(new URL[classPaths.size()]), getClass().getClassLoader());
    }

    public PartitionContext createPartitionContext(PartitionConfig partitionConfig) throws Exception {

        ClassLoader classLoader = createClassLoader(partitionConfig);

        PartitionContext partitionContext = new PartitionContext();

        if (partitionConfig instanceof DefaultPartitionConfig) {
            partitionContext.setPath(null);
        } else {
            partitionContext.setPath(partitionsDir == null ? null : new File(partitionsDir, partitionConfig.getName()));
        }

        partitionContext.setPenroseConfig(penroseConfig);
        partitionContext.setPenroseContext(penroseContext);

        partitionContext.setPartitionManager(penroseContext.getPartitionManager());
        partitionContext.setClassLoader(classLoader);

        return partitionContext;
    }

    public Partition createPartition(PartitionConfig partitionConfig) throws Exception {

        PartitionContext partitionContext = createPartitionContext(partitionConfig);

        Partition partition = createPartition(partitionConfig, partitionContext);

        log.debug("Adding partition "+partitionConfig.getName()+".");
        partitions.put(partitionConfig.getName(), partition);

        partition.init(partitionConfig, partitionContext);

        return partition;
    }

    public Partition createPartition(PartitionConfig partitionConfig, PartitionContext partitionContext) throws Exception {

        Partition partition;

        String className = partitionConfig.getPartitionClass();
        if (className == null) {
            partition = new Partition();

        } else {
            ClassLoader classLoader = partitionContext.getClassLoader();
            Class clazz = classLoader.loadClass(className);
            partition = (Partition)clazz.newInstance();
        }

        return partition;
    }

    public void stopPartitions() throws Exception {

        List<String> list = new ArrayList<String>();
        for (String partitionName : partitions.keySet()) {
            list.add(0, partitionName);
        }

        for (String partitionName : list) {
            try {
                stopPartition(partitionName);
                removePartition(partitionName);

            } catch (Exception e) {
                errorLog.error(e.getMessage(), e);
            }
        }
    }

    public void stopPartition(String name) throws Exception {

        if (debug) {
            log.debug(TextUtil.repeat("-", 70));
            log.debug("Stopping partition "+name+".");
        }

        Partition partition = partitions.get(name);

        if (partition == null) {
            log.debug("Partition "+name+" not started.");
            return;
        }

        partition.destroy();

        partitions.remove(name);

        PartitionEvent event = new PartitionEvent(PartitionEvent.PARTITION_STOPPED, partition);
        for (PartitionListener listener : listeners) {
            listener.partitionStopped(event);
        }

        log.debug("Partition "+name+" stopped.");
    }

    public void removePartition(String name) throws Exception {

        PartitionConfig partitionConfig = partitionConfigManager.removePartitionConfig(name);

        PartitionEvent event = new PartitionEvent(PartitionEvent.PARTITION_REMOVED, partitionConfig);
        for (PartitionListener listener : listeners) {
            listener.partitionRemoved(event);
        }

        log.debug("Partition "+name+" removed.");
    }

    public void clear() throws Exception {
        partitionConfigManager.clear();
        partitions.clear();
    }

    public void storePartition(String name) throws Exception {

        File baseDir;

        if ("DEFAULT".equals(name)) {
            baseDir = home;

        } else {
            baseDir = new File(partitionsDir, name);
        }

        PartitionConfig partitionConfig = partitionConfigManager.getPartitionConfig(name);
        partitionConfig.store(baseDir);
    }

    public Partition getPartition(String name) {
        return partitions.get(name);
    }

    public PartitionConfig getPartitionConfig(String name) {
        return partitionConfigManager.getPartitionConfig(name);
    }
    
    public Collection<PartitionConfig> getPartitionConfigs() {
        return partitionConfigManager.getPartitionConfigs();
    }

    public Partition getPartition(EntrySourceConfig sourceMapping) throws Exception {

        if (sourceMapping == null) return null;

        String sourceName = sourceMapping.getSourceName();
        for (Partition partition : partitions.values()) {
            PartitionConfig partitionConfig = partition.getPartitionConfig();
            if (partitionConfig.getSourceConfigManager().getSourceConfig(sourceName) != null) return partition;
        }
        return null;
    }

    public Partition getPartition(SourceConfig sourceConfig) throws Exception {

        if (sourceConfig == null) return null;

        String connectionName = sourceConfig.getConnectionName();
        for (Partition partition : partitions.values()) {
            PartitionConfig partitionConfig = partition.getPartitionConfig();
            if (partitionConfig.getConnectionConfigManager().getConnectionConfig(connectionName) != null) return partition;
        }
        return null;
    }

    public Partition getPartition(ConnectionConfig connectionConfig) throws Exception {

        if (connectionConfig == null) return null;

        String connectionName = connectionConfig.getName();
        for (Partition partition : partitions.values()) {
            PartitionConfig partitionConfig = partition.getPartitionConfig();
            if (partitionConfig.getConnectionConfigManager().getConnectionConfig(connectionName) != null) return partition;
        }
        return null;
    }

    public Partition getPartition(DN dn) throws Exception {

        if (debug) log.debug("Searching partition for \""+dn+"\".");

        Collection<Partition> results = getPartitions(dn);

        if (results.isEmpty()) {
            if (debug) log.debug("Returning default partition.");
            return getPartition("DEFAULT");
        }

        Partition partition = results.iterator().next();
        if (debug) log.debug("Returning partition "+partition.getName()+".");
        
        return partition;
    }

    public Collection<Partition> getPartitions(DN dn) throws Exception {

        Collection<Partition> results = new HashSet<Partition>();

        for (Entry entry : findEntries(dn)) {
            Partition partition = entry.getPartition();
            results.add(partition);
        }

        return results;
    }

    public Collection<Entry> findEntries(DN dn) throws Exception {

        Collection<Entry> results = new ArrayList<Entry>();

        for (Partition partition : partitions.values()) {

            //if (debug) log.debug("Searching for \""+dn+"\" in "+partition.getName()+".");

            Directory directory = partition.getDirectory();
            results.addAll(directory.findEntries(dn));
        }

        return results;
    }
/*
    public Collection<Partition> getPartitions(DN dn) throws Exception {

        Collection<Partition> results = new ArrayList<Partition>();

        if (debug) log.debug("Finding partitions for \""+dn+"\".");

        if (dn == null) {
            log.debug("DN is null.");
            //results.add(getPartition("DEFAULT"));
            return results;
        }

        Partition p = getPartition("DEFAULT");

        if (dn.isEmpty()) {
            log.debug("Root DSE.");
            results.add(p);
            return results;
        }

        DN s = null;

        for (Partition partition : partitions.values()) {
            if (debug) log.debug("Checking "+partition.getName()+" partition.");

            PartitionConfig partitionConfig = partition.getPartitionConfig();
            Collection<DN> suffixes = partitionConfig.getDirectoryConfig().getSuffixes();
            for (DN suffix : suffixes) {
                if (suffix.isEmpty() && dn.isEmpty() // Root DSE
                        || dn.endsWith(suffix)) {

                    if (s == null || s.getSize() < suffix.getSize()) {
                        p = partition;
                        s = suffix;
                    }
                }
            }
        }

        if (debug) {
            if (p == null) {
                log.debug("Partition not found.");
            } else {
                log.debug("Found partition "+p.getName()+".");
            }
        }

        return results;
    }
*/
    public Collection<Partition> getPartitions() {
        return partitions.values();
    }

    public Collection<String> getAvailablePartitionNames() throws Exception {
        Collection<String> list = new ArrayList<String>();
        for (File partitionDir : partitionsDir.listFiles()) {
            list.add(partitionDir.getName());
        }
        return list;
    }

    public Collection<String> getPartitionNames() {
        return partitions.keySet();
    }

    public int size() {
        return partitions.size();
    }

    public File getHome() {
        return home;
    }

    public void setHome(File home) {
        this.home = home;
    }

    public void setPartitionsDir(File partitionsDir) {
        this.partitionsDir = partitionsDir;
    }

    public File getConfDir() {
        return confDir;
    }

    public void setConfDir(File confDir) {
        this.confDir = confDir;
    }

    public void addListener(PartitionListener listener) {
        listeners.add(listener);
    }

    public void removeListener(PartitionListener listener) {
        listeners.remove(listener);
    }
}
