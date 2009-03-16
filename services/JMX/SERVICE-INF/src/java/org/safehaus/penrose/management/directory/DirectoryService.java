package org.safehaus.penrose.management.directory;

import org.safehaus.penrose.directory.*;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.management.BaseService;
import org.safehaus.penrose.management.PenroseJMXService;
import org.safehaus.penrose.ldap.DN;
import org.safehaus.penrose.util.TextUtil;

import java.util.*;

/**
 * @author Endi Sukma Dewata
 */
public class DirectoryService extends BaseService implements DirectoryServiceMBean {

    private PartitionManager partitionManager;
    private String partitionName;

    Map<String,EntryService> entryServices = new LinkedHashMap<String,EntryService>();

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

    public String getParentName(String entryName) throws Exception {
        DirectoryConfig directoryConfig = getDirectoryConfig();
        return directoryConfig.getParentName(entryName);
    }

    public List<String> getChildNames(String entryName) throws Exception {
        DirectoryConfig directoryConfig = getDirectoryConfig();
        return directoryConfig.getChildNames(entryName);
    }

    public void setChildNames(String entryName, List<String> childNames) throws Exception {
        DirectoryConfig directoryConfig = getDirectoryConfig();
        directoryConfig.setChildNames(entryName, childNames);
    }

    public void createEntryService(String entryName) throws Exception {

        EntryService entryService = new EntryService(jmxService, partitionManager, partitionName, entryName);
        entryService.init();

        entryServices.put(entryName, entryService);
    }

    public EntryService getEntryService(String entryName) throws Exception {
        return entryServices.get(entryName);
    }

    public void removeEntryService(String entryName) throws Exception {

        EntryService entryService = entryServices.remove(entryName);
        if (entryService == null) return;

        entryService.destroy();
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

    public EntryConfig getEntryConfig(String entryName) throws Exception {
        DirectoryConfig directoryConfig = getDirectoryConfig();
        return directoryConfig.getEntryConfig(entryName);
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

        createEntryService(entryConfig.getName());

        return entryConfig.getName();
    }

    public void updateEntry(String name, EntryConfig entryConfig) throws Exception {

        log.debug(TextUtil.repeat("-", 70));

        Directory directory = getDirectory();
        List<Entry> children = null;

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

        removeEntryService(entryConfig.getName());

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

        createEntryService(entryConfig.getName());
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

        removeEntryService(entryName);
    }

    public void init() throws Exception {
        jmxService.register(getObjectName(), this);

        DirectoryConfig directoryConfig = getDirectoryConfig();

        for (String entryName : directoryConfig.getEntryNames()) {
            createEntryService(entryName);
        }
    }

    public void destroy() throws Exception {

        DirectoryConfig directoryConfig = getDirectoryConfig();

        for (String entryName : directoryConfig.getEntryNames()) {
            removeEntryService(entryName);
        }

        jmxService.unregister(getObjectName());
    }
}