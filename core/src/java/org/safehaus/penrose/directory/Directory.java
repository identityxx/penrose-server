package org.safehaus.penrose.directory;

import org.safehaus.penrose.ldap.DN;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.partition.PartitionContext;
import org.safehaus.penrose.partition.PartitionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author Endi Sukma Dewata
 */
public class Directory implements Cloneable {

    public Logger log = LoggerFactory.getLogger(getClass());
    boolean debug = log.isDebugEnabled();

    public final static Collection<Entry> EMPTY_ENTRIES = new ArrayList<Entry>();

    protected Partition partition;

    protected DirectoryConfig directoryConfig;

    protected Map<String,Entry> entriesById = new LinkedHashMap<String,Entry>();

    public Directory(Partition partition) throws Exception {
        this.partition = partition;

        PartitionConfig partitionConfig = partition.getPartitionConfig();
        directoryConfig = partitionConfig.getDirectoryConfig();
    }

    public void init() throws Exception {

        for (EntryConfig entryConfig : directoryConfig.getEntryConfigs()) {
            if (!entryConfig.isEnabled()) continue;

            createEntry(entryConfig);
        }
    }

    public void destroy() throws Exception {
        for (String id : getRootIds()) {
            destroy(id);
        }
    }

    public void destroy(String id) throws Exception {

        Entry entry = getEntry(id);

        for (Entry child : entry.getChildren()) {
            if (child.getPartition() != partition) continue;
            destroy(child.getId());
        }

        entry.destroy();
    }

    public Entry createEntry(EntryConfig entryConfig) throws Exception {

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

        entriesById.put(entry.getId(), entry);

        return entry;
    }

    public Entry removeEntry(String id) throws Exception {

        Entry entry = entriesById.get(id);

        DN dn = entry.getDn();
        if (debug) log.debug("Removing entry \""+ dn +"\".");

        Entry parent = entry.getParent();

        if (parent != null) {
            if (debug) log.debug("Detaching from \""+parent.getDn()+"\".");
            parent.removeChild(entry);
            return entry;
        }

        return entry;
    }

    public Entry findParent(EntryConfig entryConfig) throws Exception {

        if (debug) log.debug("Searching parent of \""+entryConfig.getDn()+"\":");

        String parentId = entryConfig.getParentId();

        if (parentId != null) {
            if (debug) log.debug(" - Parent ID: "+parentId);
            Entry parent = getEntry(parentId);

            if (parent != null) {
                if (debug) log.debug("Found parent \""+parent.getDn()+"\".");
                return parent;
            }
        }

        DN parentDn = entryConfig.getParentDn();

        if (entryConfig.isAttached() && !parentDn.isEmpty()) {

            if (debug) log.debug(" - Parent DN: "+parentDn);
            Entry parent = getEntry(parentDn);

            if (parent != null) {
                if (debug) log.debug("Found parent \""+parent.getDn()+"\".");
                return parent;
            }

            if (debug) log.debug(" - Searching other partitions.");
            PartitionContext partitionContext = partition.getPartitionContext();
            PartitionManager partitionManager = partitionContext.getPartitionManager();
            parent = partitionManager.getEntry(parentDn);

            if (parent != null) {
                if (debug) log.debug("Found parent \""+parent.getDn()+"\" in partition "+parent.getPartition().getName()+".");
                return parent;
            }
        }

        if (debug) log.debug("Parent not found.");
        return null;
    }

    public Entry getEntry(String id) {
        return entriesById.get(id);
    }

    public Entry getEntry(DN dn) throws Exception {
        Collection<Entry> entries = getEntries(dn);
        if (entries.isEmpty()) return null;
        return entries.iterator().next();
    }

    public Collection<Entry> getEntries() {
        Collection<Entry> results = new ArrayList<Entry>();
        results.addAll(entriesById.values());
        return results;
    }
    
    public Collection<Entry> getEntries(Collection<String> ids) {

        if (ids == null) return EMPTY_ENTRIES;

        Collection<Entry> results = new ArrayList<Entry>();
        for (String id : ids) {
            Entry entry = entriesById.get(id);
            results.add(entry);
        }
        return results;
    }

    public Collection<Entry> getEntries(DN dn) throws Exception {

        if (dn == null) return EMPTY_ENTRIES;

        Collection<String> ids = directoryConfig.getEntryIdsByDn(dn);
        return getEntries(ids);
    }

    public Collection<Entry> findEntries(DN dn) throws Exception {

        if (debug) log.debug("Searching for \""+dn+"\" in \""+partition.getName()+"\".");

        Collection<Entry> results = new ArrayList<Entry>();

        for (String id : getRootIds()) {
            Entry entry = entriesById.get(id);
            Collection<Entry> list = entry.findEntries(dn);
            results.addAll(list);
        }

        return results;
    }
/*
    public Collection<Entry> findEntries(DN dn) throws Exception {
        if (dn == null) return EMPTY_ENTRIES;
        //log.debug("Finding entries \""+dn+"\" in partition "+getName());

        // search for static mappings
        Collection<Entry> results = entriesByDn.get(dn.getNormalizedDn());
        if (results != null) {
            //log.debug("Found "+results.size()+" mapping(s).");
            return results;
        }

        // can't find exact match -> search for parent mappings

        DN parentDn = dn.getParentDn();

        results = new ArrayList<Entry>();
        Collection<Entry> list;

        // if dn has no parent, check against root entries
        if (parentDn.isEmpty()) {
            //log.debug("Check root mappings");
            list = rootEntries;

        } else {
            if (debug) log.debug("Search parents for \""+parentDn+"\".");
            Collection<Entry> parents = findEntries(parentDn);

            // if no parent mappings found, the entry doesn't exist in this partition
            if (parents == null || parents.isEmpty()) {
            	if (debug) log.debug("Entry \""+parentDn+"\" not found.");
                return EMPTY_ENTRIES;
            }

            list = new ArrayList<Entry>();

            // for each parent mapping found
            for (Entry parent : parents) {
                if (debug) log.debug("Found parent \"" + parent.getDn()+"\".");

                String handlerName = parent.getHandlerName();
                if ("PROXY".equals(handlerName)) { // if parent is proxy, include it in results
                    results.add(parent);

                } else { // otherwise check for matching siblings
                    Collection<Entry> children = parent.getChildren();
                    list.addAll(children);
                }
            }
        }

        // check against each mapping in the list
        for (Entry entry : list) {

            if (debug) {
                log.debug("Checking DN pattern:");
                log.debug(" - " + dn);
                log.debug(" - " + entry.getDn());
            }
            if (!dn.matches(entry.getDn())) continue;

            if (debug) log.debug("Found " + entry.getDn());
            results.add(entry);
        }

        return results;
    }
*/
    public DirectoryConfig getDirectoryConfig() {
        return directoryConfig;
    }

    public void setDirectoryConfig(DirectoryConfig directoryConfig) {
        this.directoryConfig = directoryConfig;
    }

    public Partition getPartition() {
        return partition;
    }

    public String getRootId() {
        return directoryConfig.getRootId();
    }

    public Collection<String> getRootIds() {
        return directoryConfig.getRootIds();
    }

    public Entry getRootEntry() {
        return getEntry(getRootId());
    }

    public Collection<Entry> getRootEntries() {
        return getEntries(getRootIds());
    }

    public DN getSuffix() {
        return directoryConfig.getSuffix();
    }

    public Collection<DN> getSuffixes() {
        return directoryConfig.getSuffixes();
    }

    public Collection<Entry> getEntriesBySourceName(String sourceName) {
        Collection<String> ids = directoryConfig.getEntryIdsBySource(sourceName);
        return getEntries(ids);
    }
}
