package org.safehaus.penrose.directory;

import org.safehaus.penrose.ldap.DN;
import org.safehaus.penrose.ldap.LDAP;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionContext;
import org.safehaus.penrose.partition.PartitionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author Endi Sukma Dewata
 */
public class Directory implements Cloneable {

    public Logger log = LoggerFactory.getLogger(getClass());

    public final static Collection<Entry> EMPTY_ENTRIES     = new ArrayList<Entry>();

    protected DirectoryConfig directoryConfig;
    protected DirectoryContext directoryContext;

    protected Map<String,Entry> entriesById                     = new LinkedHashMap<String,Entry>();
    protected Map<String,Collection<Entry>> entriesByDn         = new LinkedHashMap<String,Collection<Entry>>();
    protected Map<String,Collection<Entry>> entriesBySourceName = new LinkedHashMap<String,Collection<Entry>>();

    protected Collection<DN> suffixes       = new ArrayList<DN>();
    protected Collection<Entry> rootEntries = new ArrayList<Entry>();

    public void init(DirectoryConfig directoryConfig, DirectoryContext directoryContext) throws Exception {
        this.directoryConfig = directoryConfig;
        this.directoryContext = directoryContext;

        initEntries();
    }

    public void initEntries() throws Exception {
        for (EntryConfig entryConfig : directoryConfig.getEntryConfigs()) {
            if (!entryConfig.isEnabled()) continue;

            createEntry(entryConfig);
        }
    }

    public Entry createEntry(EntryConfig entryConfig) throws Exception {

        log.debug("Creating entry \""+ entryConfig.getDn()+"\".");

        Partition partition = directoryContext.getPartition();

        EntryContext entryContext = new EntryContext();
        entryContext.setPartition(partition);
        entryContext.setDirectory(this);

        PartitionContext partitionContext = partition.getPartitionContext();
        ClassLoader cl = partitionContext.getClassLoader();

        String entryClass = entryConfig.getEntryClass();
        if (entryClass == null) entryClass = Entry.class.getName();

        Class clazz = cl.loadClass(entryClass);

        Entry entry = (Entry)clazz.newInstance();
        entry.init(entryConfig, entryContext);

        addEntry(entry);

        return entry;
    }

    public Entry removeEntry(String id) throws Exception {

        boolean debug = log.isDebugEnabled();

        Entry entry = entriesById.get(id);

        DN dn = entry.getDn();
        if (debug) log.debug("Removing entry \""+ dn +"\".");

        String normalizedDn = dn.getNormalizedDn();

        Collection<Entry> c = entriesByDn.get(normalizedDn);
        if (c != null) {
            c.remove(entry);
            if (c.isEmpty()) {
                entriesByDn.remove(normalizedDn);
            }
        }

        Collection<SourceRef> sourceRefs = entry.getSourceRefs();
        for (SourceRef sourceRef : sourceRefs) {
            String sourceName = sourceRef.getSource().getName();

            c = entriesBySourceName.get(sourceName);
            if (c != null) {
                c.remove(entry);
                if (c.isEmpty()) {
                    entriesBySourceName.remove(sourceName);
                }
            }
        }

        Entry parent = entry.getParent();

        if (parent != null) {
            parent.removeChild(entry);
            return entry;
        }

        rootEntries.remove(entry);
        suffixes.remove(entry.getDn());

        return entry;
    }

    public void addEntry(Entry entry) throws Exception {

        boolean debug = log.isDebugEnabled();

        DN dn = entry.getDn();
        if (debug) log.debug("Adding entry \""+ dn +"\".");

        String id = entry.getId();
        if (debug) log.debug(" - ID: "+id);

        // index by id
        entriesById.put(id, entry);

        // index by dn
        String normalizedDn = dn.getNormalizedDn();
        Collection<Entry> c = entriesByDn.get(normalizedDn);
        if (c == null) {
            c = new ArrayList<Entry>();
            entriesByDn.put(normalizedDn, c);
        }
        c.add(entry);

        // index by source
        Collection<SourceRef> sourceRefs = entry.getSourceRefs();
        for (SourceRef sourceRef : sourceRefs) {
            String sourceName = sourceRef.getSource().getName();
            c = entriesBySourceName.get(sourceName);
            if (c == null) {
                c = new ArrayList<Entry>();
                entriesBySourceName.put(sourceName, c);
            }
            c.add(entry);
        }

        String parentId = entry.getParentId();

        if (parentId != null) {
            if (debug) log.debug(" - Searching parent with id "+parentId);
            Entry parent = getEntry(parentId);

            if (parent != null) {
                if (debug) log.debug(" - Found parent \""+parent.getDn()+"\".");
                parent.addChild(entry);
                return;
            }
        }

        DN parentDn = dn.getParentDn();

        if (entry.getEntryConfig().isAttached() && !parentDn.isEmpty()) {

            if (debug) log.debug(" - Searching local parent with dn \""+parentDn+"\".");
            Collection<Entry> parents = getEntries(parentDn);

            if (!parents.isEmpty()) {
                Entry parent = parents.iterator().next();
                if (debug) log.debug(" - Found parent \""+parent.getDn()+"\".");
                parent.addChild(entry);
                return;
            }

            if (debug) log.debug(" - Local parent not found, searching external parent with dn \""+parentDn+"\"");
            Partition partition = directoryContext.getPartition();
            PartitionContext partitionContext = partition.getPartitionContext();
            PenroseContext penroseContext = partitionContext.getPenroseContext();

            PartitionManager partitionManager = penroseContext.getPartitionManager();
            Collection<Entry> entries = partitionManager.findEntries(parentDn);

            if (entries.isEmpty()) {
                if (debug) log.debug(" - External parent not found.");
/*
            } else if (entries.size() > 1) {
                log.debug(" - Found external parents:");
                for (Entry e : entries) log.debug("   - "+e.getDn());
                throw LDAP.createException(LDAP.OPERATIONS_ERROR, "Too many parents found.");
*/
            } else {
                Entry parent = entries.iterator().next();
                if (debug) log.debug(" - Found external parent \""+parent.getDn()+"\".");

                Partition p = parent.getPartition();
                if (debug) log.debug(" - Partition: "+p.getName());

                parent.addChild(entry);
            }
        }

        if (debug) log.debug(" - Add suffix \""+dn+"\"");
        rootEntries.add(entry);
        suffixes.add(entry.getDn());
    }

    public Entry getEntry(String id) {
        return entriesById.get(id);
    }

    public Collection<Entry> getEntries() {
        return entriesById.values();
    }
    
    public Collection<Entry> getEntries(DN dn) throws Exception {
        if (dn == null) return EMPTY_ENTRIES;

        Collection<Entry> list = entriesByDn.get(dn.getNormalizedDn());
        if (list == null) return EMPTY_ENTRIES;

        return new ArrayList<Entry>(list);
    }

    public Collection<Entry> findEntries(String targetDn) throws Exception {
        if (targetDn == null) return null;
        return findEntries(new DN(targetDn));
    }

    public Collection<Entry> findEntries(DN dn) throws Exception {

        boolean debug = log.isDebugEnabled();

        Collection<Entry> results = new ArrayList<Entry>();

        for (Entry entry : rootEntries) {
            if (debug) log.debug("Searching under "+entry.getDn());
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

    public DirectoryContext getDirectoryContext() {
        return directoryContext;
    }

    public void setDirectoryContext(DirectoryContext directoryContext) {
        this.directoryContext = directoryContext;
    }

    public Partition getPartition() {
        return directoryContext.getPartition();
    }

    public Collection<Entry> getRootEntries() {
        return rootEntries;
    }

    public Object clone() throws CloneNotSupportedException {
        Directory directory = (Directory)super.clone();

        directory.directoryConfig = (DirectoryConfig)directoryConfig.clone();
        directory.directoryContext = directoryContext;

        directory.entriesById     = new LinkedHashMap<String,Entry>();
        directory.entriesByDn     = new LinkedHashMap<String,Collection<Entry>>();
        directory.entriesBySourceName = new LinkedHashMap<String,Collection<Entry>>();

        directory.suffixes        = new ArrayList<DN>();
        directory.rootEntries     = new ArrayList<Entry>();

        try {
            directory.initEntries();

        } catch (Exception e) {
            throw new CloneNotSupportedException(e.getMessage());
        }

        return directory;
    }

    public Collection<Entry> getEntriesBySourceName(String sourceName) {
        return entriesBySourceName.get(sourceName);
    }
}
