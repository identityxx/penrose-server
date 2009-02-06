package org.safehaus.penrose.management.directory;

import org.safehaus.penrose.directory.*;
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
    private String entryName;
    
    public EntryService(PenroseJMXService jmxService, PartitionManager partitionManager, String partitionName, String entryName) throws Exception {

        this.jmxService = jmxService;
        this.partitionManager = partitionManager;
        this.partitionName = partitionName;
        this.entryName = entryName;
    }

    public String getObjectName() {
        return EntryClient.getStringObjectName(partitionName, entryName);
    }

    public Object getObject() {
        return getEntry();
    }

    public EntryConfig getEntryConfig() {
        return getDirectoryConfig().getEntryConfig(entryName);
    }

    public Entry getEntry() {
        Directory directory = getDirectory();
        if (directory == null) return null;
        return directory.getEntry(entryName);
    }

    public DirectoryConfig getDirectoryConfig() {
        PartitionConfig partitionConfig = partitionManager.getPartitionConfig(partitionName);
        if (partitionConfig == null) return null;
        return partitionConfig.getDirectoryConfig();
    }

    public Directory getDirectory() {
        Partition partition = partitionManager.getPartition(partitionName);
        if (partition == null) return null;
        return partition.getDirectory();
    }

    public DN getDn() throws Exception {
        EntryConfig entryConfig = getEntryConfig();
        return entryConfig.getDn();
    }
    
    public Collection<String> getChildNames() throws Exception {
        Collection<String> list = new ArrayList<String>();
        DirectoryConfig directoryConfig = getDirectoryConfig();
        for (EntryConfig childConfig : directoryConfig.getChildren(entryName)) {
            list.add(childConfig.getName());
        }
        return list;
    }

    public String getParentName() throws Exception {
        DirectoryConfig directoryConfig = getDirectoryConfig();
        return directoryConfig.getParentName(entryName);
    }
}
