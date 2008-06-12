package org.safehaus.penrose.directory;

import org.safehaus.penrose.ldap.DN;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionContext;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.partition.PartitionConfig;
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
    boolean debug = log.isDebugEnabled();

    public final static Collection<Entry> EMPTY_ENTRIES = new ArrayList<Entry>();

    protected Partition partition;

    protected DirectoryConfig directoryConfig;

    protected Map<String,Entry> entries = new LinkedHashMap<String,Entry>();

    protected Map<String,Collection<String>> entriesByDn         = new LinkedHashMap<String,Collection<String>>();
    protected Map<String,Collection<String>> entriesBySourceName = new LinkedHashMap<String,Collection<String>>();

    protected Collection<String> rootIds                  = new ArrayList<String>();
    protected Map<String,String> parentById               = new LinkedHashMap<String,String>();
    protected Map<String,Collection<String>> childrenById = new LinkedHashMap<String,Collection<String>>();

    protected Collection<DN> suffixes         = new ArrayList<DN>();

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
        for (String id : rootIds) {
            destroy(id);
        }
    }

    public void destroy(String id) throws Exception {

        Collection<String> ids = childrenById.get(id);

        if (ids != null) {
            for (String childId : ids) {
                destroy(childId);
            }
        }

        Entry entry = entries.get(id);
        entry.destroy();
    }

    public Entry createEntry(EntryConfig entryConfig) throws Exception {

        log.debug("Creating entry \""+ entryConfig.getDn()+"\".");

        EntryContext entryContext = new EntryContext();
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

        Entry entry = entries.get(id);

        DN dn = entry.getDn();
        if (debug) log.debug("Removing entry \""+ dn +"\".");

        String normalizedDn = dn.getNormalizedDn();

        Collection<String> c1 = entriesByDn.get(normalizedDn);
        if (c1 != null) {
            c1.remove(id);
            if (c1.isEmpty()) {
                entriesByDn.remove(normalizedDn);
            }
        }

        Collection<SourceRef> sourceRefs = entry.getSourceRefs();
        for (SourceRef sourceRef : sourceRefs) {
            String sourceName = sourceRef.getSource().getName();

            Collection<String> c2 = entriesBySourceName.get(sourceName);
            if (c2 != null) {
                c2.remove(id);
                if (c2.isEmpty()) {
                    entriesBySourceName.remove(sourceName);
                }
            }
        }

        Entry parent = entry.getParent();

        if (parent != null) {
            if (debug) log.debug("Detaching from \""+parent.getDn()+"\".");
            parent.removeChild(entry);
            return entry;
        }

        rootIds.remove(id);
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
        entries.put(id, entry);

        // index by dn
        String normalizedDn = dn.getNormalizedDn();
        Collection<String> c1 = entriesByDn.get(normalizedDn);
        if (c1 == null) {
            c1 = new ArrayList<String>();
            entriesByDn.put(normalizedDn, c1);
        }
        c1.add(id);

        // index by source
        Collection<SourceRef> sourceRefs = entry.getSourceRefs();
        for (SourceRef sourceRef : sourceRefs) {
            String sourceName = sourceRef.getSource().getName();
            Collection<String> c2 = entriesBySourceName.get(sourceName);
            if (c2 == null) {
                c2 = new ArrayList<String>();
                entriesBySourceName.put(sourceName, c2);
            }
            c2.add(id);
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
        rootIds.add(id);
        suffixes.add(entry.getDn());
    }

    public Entry getEntry(String id) {
        return entries.get(id);
    }

    public Collection<Entry> getEntries() {
        Collection<Entry> results = new ArrayList<Entry>();
        results.addAll(entries.values());
        return results;
    }
    
    public Collection<Entry> getEntries(Collection<String> ids) {

        if (ids == null) return EMPTY_ENTRIES;

        Collection<Entry> results = new ArrayList<Entry>();
        for (String id : ids) {
            Entry entry = entries.get(id);
            results.add(entry);
        }
        return results;
    }

    public Collection<Entry> getEntries(DN dn) throws Exception {

        if (dn == null) return EMPTY_ENTRIES;

        Collection<String> ids = entriesByDn.get(dn.getNormalizedDn());
        return getEntries(ids);
    }

    public Collection<Entry> findEntries(String targetDn) throws Exception {
        if (targetDn == null) return null;
        return findEntries(new DN(targetDn));
    }

    public Collection<Entry> findEntries(DN dn) throws Exception {

        Collection<Entry> results = new ArrayList<Entry>();

        for (String id : rootIds) {
            Entry entry = entries.get(id);
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

    public Entry getRootEntry() {
        if (rootIds.isEmpty()) return null;
        String id = rootIds.iterator().next();
        return entries.get(id);
    }

    public Collection<Entry> getRootEntries() {
        return getEntries(rootIds);
    }

    public DN getSuffix() {
        if (suffixes.isEmpty()) return null;
        return suffixes.iterator().next();
    }

    public Collection<DN> getSuffixes() {
        return suffixes;
    }

    public Entry getParent(Entry entry) {
        return getParent(entry.getId());
    }
    
    public Entry getParent(String id) {
        String parentId = parentById.get(id);
        return entries.get(parentId);
    }

    public void setParent(Entry entry, Entry parent) {
        setParent(entry.getId(), parent);
    }

    public void setParent(String id, Entry parent) {
        parentById.put(id, parent.getId());
    }

    public Collection<Entry> getChildren(Entry parent) {
        return getChildren(parent.getId());
    }

    public Collection<Entry> getChildren(String id) {
        Collection<String> childIds = childrenById.get(id);
        return getEntries(childIds);
    }

    public void addChild(Entry parent, Entry child) {
        addChild(parent.getId(), child);
    }
    
    public void addChild(String id, Entry child) {
        Collection<String> childIds = childrenById.get(id);
        if (childIds == null) {
            childIds = new ArrayList<String>();
            childrenById.put(id, childIds);
        }
        childIds.add(child.getId());

        parentById.put(child.getId(), id);
    }

    public void removeChild(Entry parent, Entry child) {
        removeChild(parent.getId(), child);
    }
    
    public void removeChild(String id, Entry child) {
        Collection<String> childIds = childrenById.get(id);
        if (childIds == null) return;

        childIds.remove(child.getId());
        if (childIds.isEmpty()) childrenById.remove(id);

        parentById.remove(child.getId());
    }

    public void removeChildren(Entry entry) {
        removeChildren(entry.getId());
    }

    public void removeChildren(String id) {
        childrenById.remove(id);
    }

    public Collection<Entry> getEntriesBySourceName(String sourceName) {
        Collection<String> ids = entriesBySourceName.get(sourceName);
        return getEntries(ids);
    }
}
