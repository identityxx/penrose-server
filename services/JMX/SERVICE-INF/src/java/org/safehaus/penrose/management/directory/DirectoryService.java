package org.safehaus.penrose.management.directory;

import org.safehaus.penrose.directory.*;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.management.BaseService;
import org.safehaus.penrose.management.PenroseJMXService;
import org.safehaus.penrose.ldap.DN;
import org.safehaus.penrose.util.TextUtil;

import java.util.ArrayList;
import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class DirectoryService extends BaseService implements DirectoryServiceMBean {

    private PartitionManager partitionManager;
    private String partitionName;

    public DirectoryService(PenroseJMXService jmxService, PartitionManager partitionManager, String partitionName) throws Exception {

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
        PartitionConfig partitionConfig = getPartitionConfig();
        if (partitionConfig == null) return null;
        return partitionConfig.getDirectoryConfig();
    }

    public Directory getDirectory() {
        Partition partition = getPartition();
        if (partition == null) return null;
        return partition.getDirectory();
    }

    public PartitionConfig getPartitionConfig() {
        return partitionManager.getPartitionConfig(partitionName);
    }

    public Partition getPartition() {
        return partitionManager.getPartition(partitionName);
    }

    public DN getSuffix() throws Exception {
        DirectoryConfig directoryConfig = getDirectoryConfig();
        return directoryConfig.getSuffix();
    }

    public Collection<DN> getSuffixes() throws Exception {
        Collection<DN> list = new ArrayList<DN>();
        DirectoryConfig directoryConfig = getDirectoryConfig();
        list.addAll(directoryConfig.getSuffixes());
        return list;
    }

    public Collection<String> getRootEntryNames() throws Exception {
        Collection<String> list = new ArrayList<String>();
        DirectoryConfig directoryConfig = getDirectoryConfig();
        list.addAll(directoryConfig.getRootNames());
        return list;
    }

    public Collection<String> getEntryNames() throws Exception {
        Collection<String> list = new ArrayList<String>();
        DirectoryConfig directoryConfig = getDirectoryConfig();
        list.addAll(directoryConfig.getEntryNames());
        return list;
    }

    public Collection<String> getChildNames(String entryName) throws Exception {
        Collection<String> list = new ArrayList<String>();
        DirectoryConfig directoryConfig = getDirectoryConfig();
        list.addAll(directoryConfig.getChildNames(entryName));
        return list;
    }

    public EntryService getEntryService(String entryId) throws Exception {

        EntryService entryService = new EntryService(jmxService, partitionManager, partitionName, entryId);
        entryService.init();

        return entryService;
    }

    public String getEntryName(DN dn) throws Exception {
        DirectoryConfig directoryConfig = getDirectoryConfig();
        return directoryConfig.getEntryNameByDn(dn);
    }

    public DN getEntryDn(String entryName) throws Exception {
        DirectoryConfig directoryConfig = getDirectoryConfig();
        EntryConfig entryConfig = directoryConfig.getEntryConfig(entryName);
        if (entryConfig == null) return null;
        return entryConfig.getDn();
    }

    public String createEntry(EntryConfig entryConfig) throws Exception {

        log.debug(TextUtil.repeat("-", 70));

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

        EntryService entryService = getEntryService(entryConfig.getName());
        entryService.register();

        return entryConfig.getName();
    }

    public void updateEntry(String name, EntryConfig entryConfig) throws Exception {

        log.debug(TextUtil.repeat("-", 70));

        Directory directory = getDirectory();
        Collection<Entry> children = null;

        if (directory != null) {
            try {
                Entry oldEntry = directory.removeEntry(entryConfig.getName());
                if (oldEntry != null) {
                    children = oldEntry.getChildren();
                    oldEntry.destroy();
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }

        EntryService oldEntryService = getEntryService(entryConfig.getName());
        oldEntryService.unregister();

        DirectoryConfig directoryConfig = getDirectoryConfig();
        directoryConfig.updateEntryConfig(name, entryConfig);

        if (directory != null) {
            try {
                Entry newEntry = directory.createEntry(entryConfig);
                if (children != null) newEntry.addChildren(children);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }

        EntryService newEntryService = getEntryService(entryConfig.getName());
        newEntryService.register();
    }

    public void removeEntry(String entryName) throws Exception {

        log.debug(TextUtil.repeat("-", 70));

        Directory directory = getDirectory();
        if (directory != null) {
            try {
                directory.destroy(entryName);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }

        DirectoryConfig directoryConfig = getDirectoryConfig();
        directoryConfig.removeEntryConfig(entryName);

        EntryService entryService = getEntryService(entryName);
        entryService.unregister();
    }

    public void register() throws Exception {
        jmxService.register(getObjectName(), this);

        DirectoryConfig directoryConfig = getDirectoryConfig();

        for (String entryId : directoryConfig.getEntryNames()) {
            EntryService entryService = getEntryService(entryId);
            entryService.register();
        }
    }

    public void unregister() throws Exception {

        DirectoryConfig directoryConfig = getDirectoryConfig();

        for (String entryId : directoryConfig.getEntryNames()) {
            EntryService entryService = getEntryService(entryId);
            entryService.unregister();
        }

        jmxService.unregister(getObjectName());
    }
}