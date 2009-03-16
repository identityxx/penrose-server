package org.safehaus.penrose.directory;

import org.safehaus.penrose.ldap.DN;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.partition.PartitionContext;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.directory.event.DirectoryListener;
import org.safehaus.penrose.directory.event.DirectoryEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author Endi Sukma Dewata
 */
public class Directory implements Cloneable {

    public Logger log = LoggerFactory.getLogger(getClass());

    public final static Collection<Entry> EMPTY_ENTRIES = new ArrayList<Entry>();

    protected Partition partition;

    protected DirectoryConfig directoryConfig;

    protected Collection<Entry> rootEntries = new ArrayList<Entry>();
    protected Map<String,Entry> entries = new LinkedHashMap<String,Entry>();

    Collection<DirectoryListener> listeners = new LinkedHashSet<DirectoryListener>();

    public Directory(Partition partition) throws Exception {
        this.partition = partition;

        PartitionConfig partitionConfig = partition.getPartitionConfig();
        directoryConfig = partitionConfig.getDirectoryConfig();
    }

    public void init() throws Exception {

        boolean debug = log.isDebugEnabled();
        if (debug) log.debug("Root entries: "+directoryConfig.getRootNames());

        for (String entryName : directoryConfig.getRootNames()) {

            EntryConfig entryConfig = directoryConfig.getEntryConfig(entryName);
            if (!entryConfig.isEnabled()) continue;

            try {
                createEntry(entryName);
            } catch (Exception e) {
                Penrose.errorLog.error("Failed creating entry "+entryConfig.getName()+" ("+entryConfig.getDn()+") in partition "+partition.getName()+".", e);
            }
        }
    }

    public void addEntryConfig(EntryConfig entryConfig) throws Exception {
        directoryConfig.addEntryConfig(entryConfig);

        if (!listeners.isEmpty()) {
            DirectoryEvent event = new DirectoryEvent(DirectoryEvent.ENTRY_ADDED, partition.getName(), entryConfig.getName());
            for (DirectoryListener listener : listeners) {
                listener.entryAdded(event);
            }
        }
    }

    public void removeEntryConfig(String entryName) throws Exception {
        directoryConfig.removeEntryConfig(entryName);

        if (!listeners.isEmpty()) {
            DirectoryEvent event = new DirectoryEvent(DirectoryEvent.ENTRY_REMOVED, partition.getName(), entryName);
            for (DirectoryListener listener : listeners) {
                listener.entryRemoved(event);
            }
        }
    }

    public void createEntry(String entryName) throws Exception {

        EntryConfig entryConfig = directoryConfig.getEntryConfig(entryName);
        createEntry(entryConfig);

        for (String childName : directoryConfig.getChildNames(entryName)) {

            EntryConfig childConfig = directoryConfig.getEntryConfig(childName);
            if (!childConfig.isEnabled()) continue;

            createEntry(childName);
        }
    }

    public void destroy() throws Exception {

        for (String entryName : getRootNames()) {
            try {
                log.debug("Removing subtree "+entryName+".");

                destroy(entryName);

                log.debug("Subtree "+entryName+" removed.");

            } catch (Exception e) {
                Penrose.errorLog.error("Failed removing entry "+entryName+" in partition "+partition.getName()+".", e);
            }
        }
    }

    public void destroy(String entryName) throws Exception {

        Entry entry = getEntry(entryName);
        if (entry == null) throw new Exception("Entry "+entryName+" not found.");

        destroy(entry);
    }

    public void destroy(Entry entry) throws Exception {

        boolean debug = log.isDebugEnabled();

        List<Entry> children = new ArrayList<Entry>();
        children.addAll(entry.getChildren());

        for (Entry child : children) {

            if (child.getPartition() != partition) {
                if (debug) log.debug("Entry "+child.getName()+" is in partition "+child.getPartition().getName()+".");
                continue;
            }

            if (debug) log.debug("Removing entry "+child.getName()+" under entry "+entry.getName()+".");
            destroy(child);
        }

        removeEntry(entry);

        entry.destroy();
    }

    public Entry createEntry(EntryConfig entryConfig) throws Exception {

        boolean debug = log.isDebugEnabled();
        if (debug) log.debug("Creating entry \""+ entryConfig.getDn()+"\".");

        EntryContext entryContext = new EntryContext();
        entryContext.setDirectory(this);

        Entry parent = findParent(entryConfig);
        entryContext.setParent(parent);

        PartitionContext partitionContext = partition.getPartitionContext();
        ClassLoader cl = partitionContext.getClassLoader();

        String className = entryConfig.getEntryClass();
        if (className == null) className = Entry.class.getName();

        Class clazz = cl.loadClass(className);

        if (debug) log.debug("Creating "+className+".");
        Entry entry = (Entry)clazz.newInstance();
        entry.init(entryConfig, entryContext);

        String entryName = entry.getName();
        if (debug) log.debug("Adding entry \""+entryName+"\".");

        entries.put(entryName, entry);

        if (parent == null || parent.getPartition() != partition) {
            if (debug) log.debug("Adding into root entry list.");
            rootEntries.add(entry);
        }

        if (parent != null) {
            if (debug) log.debug("Attaching to parent \""+parent.getDn()+"\".");
            parent.addChild(entry);
        }

        return entry;
    }

    public Entry removeEntry(String entryName) throws Exception {

        boolean debug = log.isDebugEnabled();
        if (debug) log.debug("Removing entry \""+entryName+"\".");

        Entry entry = entries.get(entryName);
        if (entry == null) {
            if (debug) log.debug("Entry \""+entryName+"\" not found.");
            return null;
        }

        removeEntry(entry);

        return entry;
    }

    public void removeEntry(Entry entry) throws Exception {

        boolean debug = log.isDebugEnabled();
        String entryName = entry.getName();

        if (debug) log.debug("Removing entry "+entryName+" ("+entry.getDn()+").");

        Entry parent = entry.getParent();

        if (parent == null) {
            if (debug) log.debug("Entry "+entryName+" has no parent.");
            rootEntries.remove(entry);

        } else if (parent.getPartition() != partition) {
            if (debug) log.debug("Entry "+entryName+" has a parent in "+parent.getPartition().getName()+" partition.");
            rootEntries.remove(entry);
        }

        if (parent != null) {
            if (debug) log.debug("Detaching from parent "+parent.getName()+" ("+parent.getDn()+").");
            parent.removeChild(entry);
        }

        entries.remove(entry.getName());

        if (debug) log.debug("Entry "+entry.getName()+" removed.");
    }

    public Entry findParent(EntryConfig entryConfig) throws Exception {

        boolean debug = log.isDebugEnabled();
        if (debug) log.debug("Searching parent of \""+entryConfig.getDn()+"\".");

        String parentName = entryConfig.getParentName();

        if (parentName != null) {
            if (debug) log.debug("Searching parent: "+parentName);
            Entry parent = getEntry(parentName);

            if (parent != null) {
                if (debug) log.debug("Found parent \""+parent.getDn()+"\".");
                return parent;
            }
        }

        DN parentDn = entryConfig.getParentDn();

        if (entryConfig.isAttached() && !parentDn.isEmpty()) {

            if (debug) log.debug("Searching parent DN: "+parentDn);
            Entry parent = getEntry(parentDn);

            if (parent != null) {
                if (debug) log.debug("Found parent \""+parent.getDn()+"\".");
                return parent;
            }

            if (debug) log.debug("Searching parent DN in other partitions.");
            PartitionContext partitionContext = partition.getPartitionContext();
            PartitionManager partitionManager = partitionContext.getPartitionManager();
            Collection<String> depends = partition.getDepends();

            for (String depend : depends) {
                Partition p = partitionManager.getPartition(depend);

                Collection<Entry> parents = p.findEntries(parentDn);
                if (parents.isEmpty()) continue;

                parent = parents.iterator().next();
                if (debug) log.debug("Found parent \""+parent.getDn()+"\" in partition "+p.getName()+".");
                return parent;
            }

            Partition p = partitionManager.getPartition(PartitionConfig.ROOT);
            if (p != partition) {
                Collection<Entry> parents = p.findEntries(parentDn);
                if (!parents.isEmpty()) {
                    parent = parents.iterator().next();
                    if (debug) log.debug("Found parent \""+parent.getDn()+"\" in partition "+p.getName()+".");
                    return parent;
                }
            }
        }

        if (debug) log.debug("Parent not found.");
        return null;
    }

    public Entry getEntry(String entryName) {
        return entries.get(entryName);
    }

    public Entry getEntry(DN dn) throws Exception {
        Collection<Entry> entries = getEntries(dn);
        if (entries.isEmpty()) return null;
        return entries.iterator().next();
    }

    public Collection<Entry> getEntries() {
        Collection<Entry> results = new ArrayList<Entry>();
        results.addAll(entries.values());
        return results;
    }
    
    public Collection<Entry> getEntries(Collection<String> names) {

        if (names == null) return EMPTY_ENTRIES;

        Collection<Entry> results = new ArrayList<Entry>();
        for (String name : names) {
            Entry entry = entries.get(name);
            results.add(entry);
        }
        return results;
    }

    public Collection<Entry> getEntries(DN dn) throws Exception {

        if (dn == null) return EMPTY_ENTRIES;

        Collection<String> ids = directoryConfig.getEntryNamesByDn(dn);
        return getEntries(ids);
    }

    public List<Entry> findEntries(DN dn) throws Exception {

        boolean debug = log.isDebugEnabled();
        if (debug) log.debug("Searching for \""+dn+"\" in \""+partition.getName()+"\":");

        List<Entry> results = new ArrayList<Entry>();

        for (Entry entry : rootEntries) {
            if (debug) log.debug(" - Suffix: "+entry.getDn());

            List<Entry> list = entry.findEntries(dn);
            for (Entry e : list) {
                if (debug) log.debug("   - Found "+e.getName()+": "+e.getDn());
                results.add(e);
            }
        }

        return results;
    }

    public DirectoryConfig getDirectoryConfig() {
        return directoryConfig;
    }

    public void setDirectoryConfig(DirectoryConfig directoryConfig) {
        this.directoryConfig = directoryConfig;
    }

    public Partition getPartition() {
        return partition;
    }

    public String getRootName() {
        return directoryConfig.getRootName();
    }

    public Collection<String> getRootNames() {
        Collection<String> entryNames = new ArrayList<String>();
        for (Entry entry : rootEntries) {
            entryNames.add(entry.getName());
        }
        return entryNames;
    }

    public Entry getRootEntry() {
        return getEntry(getRootName());
    }

    public Collection<Entry> getRootEntries() {
        return rootEntries;
    }

    public DN getSuffix() {
        return directoryConfig.getSuffix();
    }

    public Collection<DN> getSuffixes() {
        return directoryConfig.getSuffixes();
    }

    public Collection<Entry> getEntriesBySourceName(String sourceName) throws Exception {
        Collection<String> ids = directoryConfig.getEntryNamesBySource(sourceName);
        return getEntries(ids);
    }

    public void addListener(DirectoryListener listener) {
        listeners.add(listener);
    }

    public void removeListener(DirectoryListener listener) {
        listeners.remove(listener);
    }
}
