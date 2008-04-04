package org.safehaus.penrose.management.directory;

import org.safehaus.penrose.directory.DirectoryConfig;
import org.safehaus.penrose.directory.Entry;
import org.safehaus.penrose.directory.EntryConfig;
import org.safehaus.penrose.ldap.DN;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.management.BaseService;
import org.safehaus.penrose.management.PenroseJMXService;

import java.util.ArrayList;
import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class EntryService extends BaseService implements EntryServiceMBean {

    private PartitionManager partitionManager;
    private String partitionName;
    private String entryId;
    
    public EntryService(PenroseJMXService jmxService, PartitionManager partitionManager, String partitionName, String entryId) throws Exception {
        super(EntryServiceMBean.class);

        this.jmxService = jmxService;
        this.partitionManager = partitionManager;
        this.partitionName = partitionName;
        this.entryId = entryId;
    }

    public String getObjectName() {
        return EntryClient.getStringObjectName(partitionName, entryId);
    }

    public Object getObject() {
        return getEntry();
    }

    public EntryConfig getEntryConfig() {
        return getPartitionConfig().getDirectoryConfig().getEntryConfig(entryId);
    }

    public Entry getEntry() {
        return getPartition().getDirectory().getEntry(entryId);
    }

    public PartitionConfig getPartitionConfig() {
        return partitionManager.getPartitionConfig(partitionName);
    }

    public Partition getPartition() {
        return partitionManager.getPartition(partitionName);
    }

    public DN getDn() throws Exception {
        EntryConfig entryConfig = getEntryConfig();
        return entryConfig.getDn();
    }
    
    public Collection<String> getChildIds() throws Exception {
        Collection<String> list = new ArrayList<String>();
        DirectoryConfig directoryConfig = getPartitionConfig().getDirectoryConfig();
        for (EntryConfig childConfig : directoryConfig.getChildren(entryId)) {
            list.add(childConfig.getId());
        }
        return list;
    }

    public String getParentId() throws Exception {
        EntryConfig entryConfig = getEntryConfig();
        return entryConfig.getParentId();
    }
}
