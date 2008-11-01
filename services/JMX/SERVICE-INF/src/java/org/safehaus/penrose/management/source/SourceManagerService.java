package org.safehaus.penrose.management.source;

import org.safehaus.penrose.management.BaseService;
import org.safehaus.penrose.management.PenroseJMXService;
import org.safehaus.penrose.source.*;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.directory.Directory;
import org.safehaus.penrose.directory.Entry;

import java.util.Collection;
import java.util.ArrayList;

/**
 * @author Endi Sukma Dewata
 */
public class SourceManagerService extends BaseService implements SourceManagerServiceMBean {

    private PartitionManager partitionManager;
    private String partitionName;

    public SourceManagerService(PenroseJMXService jmxService, PartitionManager partitionManager, String partitionName) throws Exception {

        this.jmxService = jmxService;
        this.partitionManager = partitionManager;
        this.partitionName = partitionName;
    }

    public String getObjectName() {
        return SourceManagerClient.getStringObjectName(partitionName);
    }

    public Object getObject() {
        return getSourceManager();
    }

    public PartitionConfig getPartitionConfig() {
        return partitionManager.getPartitionConfig(partitionName);
    }

    public Partition getPartition() {
        return partitionManager.getPartition(partitionName);
    }

    public SourceConfigManager getSourceConfigManager() {
        PartitionConfig partitionConfig = getPartitionConfig();
        if (partitionConfig == null) return null;
        return partitionConfig.getSourceConfigManager();
    }

    public SourceManager getSourceManager() {
        Partition partition = getPartition();
        if (partition == null) return null;
        return partition.getSourceManager();
    }

    public SourceService getSourceService(String sourceName) throws Exception {

        SourceService sourceService = new SourceService(jmxService, partitionManager, partitionName, sourceName);
        sourceService.init();

        return sourceService;
    }

    public void register() throws Exception {

        super.register();

        SourceConfigManager sourceConfigManager = getSourceConfigManager();
        for (String sourceName : sourceConfigManager.getSourceNames()) {
            SourceService sourceService = getSourceService(sourceName);
            sourceService.register();
        }
    }

    public void unregister() throws Exception {
        SourceConfigManager sourceConfigManager = getSourceConfigManager();
        for (String sourceName : sourceConfigManager.getSourceNames()) {
            SourceService sourceService = getSourceService(sourceName);
            sourceService.unregister();
        }

        super.unregister();
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Sources
    ////////////////////////////////////////////////////////////////////////////////

    public Collection<String> getSourceNames() throws Exception {

        PartitionConfig partitionConfig = getPartitionConfig();
        SourceConfigManager sourceConfigManager = partitionConfig.getSourceConfigManager();

        Collection<String> list = new ArrayList<String>();
        list.addAll(sourceConfigManager.getSourceNames());

        return list;
    }

    public void createSource(SourceConfig sourceConfig) throws Exception {

        String sourceName = sourceConfig.getName();

        PartitionConfig partitionConfig = getPartitionConfig();
        SourceConfigManager sourceConfigManager = partitionConfig.getSourceConfigManager();
        sourceConfigManager.addSourceConfig(sourceConfig);

        Partition partition = getPartition();
        if (partition != null) {
            SourceManager sourceManager = partition.getSourceManager();
            sourceManager.createSource(sourceConfig);
        }

        SourceService sourceService = getSourceService(sourceName);
        sourceService.register();
    }

    public void updateSource(String name, SourceConfig sourceConfig) throws Exception {

        Partition partition = getPartition();

        SourceManager sourceManager = partition.getSourceManager();

        Source oldSource = sourceManager.removeSource(name);
        SourceService oldSourceService = getSourceService(oldSource.getName());
        oldSourceService.unregister();

        sourceManager.updateSourceConfig(name, sourceConfig);

        Source newSource = sourceManager.createSource(sourceConfig);
        SourceService newSourceService = getSourceService(newSource.getName());
        newSourceService.register();
    }

    public void removeSource(String name) throws Exception {

        Partition partition = getPartition();

        Directory directory = partition.getDirectory();
        Collection<Entry> entries = directory.getEntriesBySourceName(name);
        if (entries != null && !entries.isEmpty()) {
            throw new Exception("Source "+name+" is in use.");
        }

        SourceManager sourceManager = partition.getSourceManager();
        Source source = sourceManager.removeSource(name);

        SourceConfigManager sourceConfigManager = sourceManager.getSourceConfigManager();
        sourceConfigManager.removeSourceConfig(name);

        SourceService sourceService = getSourceService(source.getName());
        sourceService.unregister();
    }
}
