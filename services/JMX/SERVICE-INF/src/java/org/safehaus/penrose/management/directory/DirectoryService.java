package org.safehaus.penrose.management.directory;

import org.safehaus.penrose.directory.*;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.management.BaseService;
import org.safehaus.penrose.management.PenroseJMXService;
import org.safehaus.penrose.ldap.DN;

import java.util.ArrayList;
import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class DirectoryService extends BaseService implements DirectoryServiceMBean {

    private PartitionManager partitionManager;
    private String partitionName;

    public DirectoryService(PenroseJMXService jmxService, PartitionManager partitionManager, String partitionName) throws Exception {
        super(DirectoryServiceMBean.class);

        this.jmxService = jmxService;
        this.partitionManager = partitionManager;
        this.partitionName = partitionName;
    }

    public String getObjectName() {
        return DirectoryClient.getStringObjectName(partitionName);
    }

    public Object getObject() {
        return getDirectory();
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

    public Collection<DN> getSuffixes() throws Exception {
        Collection<DN> list = new ArrayList<DN>();
        DirectoryConfig directoryConfig = getDirectoryConfig();
        for (EntryConfig entryConfig : directoryConfig.getRootEntryConfigs()) {
            list.add(entryConfig.getDn());
        }
        return list;
    }

    public Collection<String> getRootEntryIds() throws Exception {
        Collection<String> list = new ArrayList<String>();
        DirectoryConfig directoryConfig = getDirectoryConfig();
        for (EntryConfig entryConfig : directoryConfig.getRootEntryConfigs()) {
            list.add(entryConfig.getId());
        }
        return list;
    }

    public Collection<String> getEntryIds() throws Exception {
        Collection<String> list = new ArrayList<String>();
        DirectoryConfig directoryConfig = getDirectoryConfig();
        for (EntryConfig entryConfig : directoryConfig.getEntryConfigs()) {
            list.add(entryConfig.getId());
        }
        return list;
    }

    public EntryService getEntryService(String entryId) throws Exception {

        EntryService entryService = new EntryService(jmxService, partitionManager, partitionName, entryId);
        entryService.init();

        return entryService;
    }

    public String createEntry(EntryConfig entryConfig) throws Exception {

        DirectoryConfig directoryConfig = getDirectoryConfig();
        directoryConfig.addEntryConfig(entryConfig);

        Directory directory = getDirectory();
        if (directory != null) {
            try {
                directory.createEntry(entryConfig);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }

        EntryService entryService = getEntryService(entryConfig.getId());
        entryService.register();

        return entryConfig.getId();
    }

    public void updateEntry(String id, EntryConfig entryConfig) throws Exception {

        Directory directory = getDirectory();
        Collection<Entry> children = null;

        if (directory != null) {
            try {
                Entry oldEntry = directory.removeEntry(id);
                children = oldEntry.getChildren();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }

        EntryService oldEntryService = getEntryService(id);
        oldEntryService.unregister();

        DirectoryConfig directoryConfig = getDirectoryConfig();
        directoryConfig.updateEntryConfig(id, entryConfig);

        if (directory != null) {
            try {
                Entry newEntry = directory.createEntry(entryConfig);
                if (children != null) newEntry.addChildren(children);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }

        EntryService newEntryService = getEntryService(entryConfig.getId());
        newEntryService.register();
    }

    public void removeEntry(String id) throws Exception {

        Directory directory = getDirectory();
        if (directory != null) {
            try {
                directory.removeEntry(id);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }

        DirectoryConfig directoryConfig = getDirectoryConfig();
        directoryConfig.removeEntryConfig(id);

        EntryService entryService = getEntryService(id);
        entryService.unregister();
    }

    public void register() throws Exception {
        jmxService.register(getObjectName(), this);

        DirectoryConfig directoryConfig = getDirectoryConfig();

        for (String entryId : directoryConfig.getEntryIds()) {
            EntryService entryService = getEntryService(entryId);
            entryService.register();
        }
    }

    public void unregister() throws Exception {

        DirectoryConfig directoryConfig = getDirectoryConfig();

        for (String entryId : directoryConfig.getEntryIds()) {
            EntryService entryService = getEntryService(entryId);
            entryService.unregister();
        }

        jmxService.unregister(getObjectName());
    }
}