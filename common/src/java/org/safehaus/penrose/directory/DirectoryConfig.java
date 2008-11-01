package org.safehaus.penrose.directory;

import org.safehaus.penrose.ldap.DN;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.io.Serializable;

/**
 * @author Endi Sukma Dewata
 */
public class DirectoryConfig implements Serializable, Cloneable {

    static {
        log = LoggerFactory.getLogger(DirectoryConfig.class);
    }

    public static transient Logger log;
    public static boolean debug = log.isDebugEnabled();

    public final static Collection<String> EMPTY_IDS  = new ArrayList<String>();
    public final static Collection<EntryConfig> EMPTY = new ArrayList<EntryConfig>();

    protected Map<String,EntryConfig> entryConfigsById            = new LinkedHashMap<String,EntryConfig>();
    protected Map<String,Collection<String>> entryConfigsByDn     = new LinkedHashMap<String,Collection<String>>();
    protected Map<String,Collection<String>> entryConfigsBySource = new LinkedHashMap<String,Collection<String>>();

    protected Collection<String> rootIds                  = new ArrayList<String>();
    protected Map<String,String> parentById               = new LinkedHashMap<String,String>();
    protected Map<String,Collection<String>> childrenById = new LinkedHashMap<String,Collection<String>>();

    public DirectoryConfig() {
    }

    public void addEntryConfig(EntryConfig entryConfig) throws Exception {

        DN dn = entryConfig.getDn();
        if (debug) log.debug("Adding entry \""+dn+"\".");

        String id = entryConfig.getId();
        if (id == null) {
            int counter = 0;
            id = "entry"+counter;
            while (entryConfigsById.containsKey(id)) {
                counter++;
                id = "entry"+counter;
            }
            entryConfig.setId(id);
        }
        if (debug) log.debug(" - ID: "+id);

        // index by id
        entryConfigsById.put(id, entryConfig);

        // index by dn
        String normalizedDn = dn.getNormalizedDn();
        Collection<String> c1 = entryConfigsByDn.get(normalizedDn);
        if (c1 == null) {
            c1 = new ArrayList<String>();
            entryConfigsByDn.put(normalizedDn, c1);
        }
        c1.add(id);

        // index by source
        Collection<EntrySourceConfig> sourceConfigs = entryConfig.getSourceConfigs();
        for (EntrySourceConfig sourceConfig : sourceConfigs) {
            String sourceName = sourceConfig.getSourceName();
            Collection<String> c2 = entryConfigsBySource.get(sourceName);
            if (c2 == null) {
                c2 = new ArrayList<String>();
                entryConfigsBySource.put(sourceName, c2);
            }
            c2.add(entryConfig.getId());
        }

        String parentId = entryConfig.getParentId();

        if (parentId != null) {
            if (debug) log.debug(" - Searching parent with id "+parentId);
            EntryConfig parent = getEntryConfig(parentId);

            if (parent != null) {
                if (debug) log.debug(" - Found parent \""+parent.getDn()+"\".");
                addChild(parentId, entryConfig.getId());
                return;
            }
        }

        DN parentDn = entryConfig.getParentDn();

        if (!parentDn.isEmpty()) {

            if (debug) log.debug(" - Searching parent with dn \""+parentDn+"\".");
            Collection<EntryConfig> parents = getEntryConfigsByDn(parentDn);

            if (!parents.isEmpty()) {
                EntryConfig parent = parents.iterator().next();
                if (debug) log.debug(" - Found parent \""+parent.getDn()+"\".");
                addChild(parent.getId(), entryConfig.getId());
                return;
            }
        }

        if (debug) log.debug(" - Add suffix \""+dn+"\"");
        rootIds.add(id);
    }

    public boolean contains(EntryConfig entryConfig) {
        return entryConfigsById.containsKey(entryConfig.getId());
    }

    public EntryConfig getEntryConfig(String id) {
        return entryConfigsById.get(id);
    }

    public String getParentId(String entryId) {
        return parentById.get(entryId);
    }
    
    public EntryConfig getParent(EntryConfig entryConfig) {
        if (entryConfig == null) return null;

        String parentId = parentById.get(entryConfig.getId());
        return entryConfigsById.get(parentId);
    }

    public void updateEntryConfig(String id, EntryConfig entryConfig) throws Exception {

        EntryConfig oldEntryConfig = entryConfigsById.get(id);

        if (!oldEntryConfig.getDn().equals(entryConfig.getDn())) {
            renameEntryConfig(oldEntryConfig, entryConfig.getDn());
        }

        oldEntryConfig.copy(entryConfig);
        oldEntryConfig.setId(id);
    }

    public void removeEntryConfig(String id) throws Exception {
        EntryConfig entryConfig = getEntryConfig(id);
        if (entryConfig == null) return;
        removeEntryConfig(entryConfig);
    }

    public void removeEntryConfig(EntryConfig entryConfig) throws Exception {
        entryConfigsById.remove(entryConfig.getId());

        EntryConfig parent = getParent(entryConfig);
        if (parent == null) {
            rootIds.remove(entryConfig.getId());

        } else {
            removeChild(parent.getId(), entryConfig.getId());
        }

        Collection<String> c = entryConfigsByDn.get(entryConfig.getDn().getNormalizedDn());
        if (c == null) return;

        c.remove(entryConfig.getId());
        if (c.isEmpty()) {
            entryConfigsByDn.remove(entryConfig.getDn().getNormalizedDn());
        }
    }

    public Collection<String> getEntryIds() {
        return entryConfigsById.keySet();
    }
    
    public Collection<String> getEntryIdsByDn(DN dn) throws Exception {
        if (dn == null) return EMPTY_IDS;

        Collection<String> entryIds = entryConfigsByDn.get(dn.getNormalizedDn());
        if (entryIds == null) return EMPTY_IDS;

        return entryIds;
    }

    public Collection<String> getEntryIdsBySource(String sourceName) {
        Collection<String> entryIds = entryConfigsBySource.get(sourceName);
        if (entryIds == null) return EMPTY_IDS;
        return entryIds;
    }

    public Collection<EntryConfig> getEntryConfigs() {
        Collection<EntryConfig> list = new ArrayList<EntryConfig>();
        list.addAll(entryConfigsById.values());
        return list;
    }

    public Collection<EntryConfig> getEntryConfigs(Collection<String> entryIds) {
        Collection<EntryConfig> entryConfigs = new ArrayList<EntryConfig>();
        for (String entryId : entryIds) {
            EntryConfig entryConfig = getEntryConfig(entryId);
            if (entryConfig == null) continue;
            entryConfigs.add(entryConfig);
        }
        return entryConfigs;
    }

    public Collection<EntryConfig> getEntryConfigsBySource(String sourceName) {
        Collection<String> entryIds = getEntryIdsBySource(sourceName);
        return getEntryConfigs(entryIds);
    }

    public Collection<EntryConfig> getEntryConfigsByDn(DN dn) throws Exception {
        Collection<String> entryIds = getEntryIdsByDn(dn);
        return getEntryConfigs(entryIds);
    }

    public void renameChildren(EntryConfig entryConfig, String newDn) throws Exception {
        if (entryConfig == null) return;
        if (newDn.equals(entryConfig.getDn().toString())) return;

        DN oldDn = entryConfig.getDn();
        if (debug) log.debug("Renaming "+oldDn+" to "+newDn);

        Collection c = getEntryConfigsByDn(oldDn);
        if (c == null) return;

        c.remove(entryConfig);
        if (c.isEmpty()) {
        	if (debug) log.debug("Last "+oldDn);
            entryConfigsByDn.remove(oldDn.getNormalizedDn());
        }

        entryConfig.setStringDn(newDn);
        Collection<String> newList = entryConfigsByDn.get(newDn.toLowerCase());
        if (newList == null) {
        	if (debug) log.debug("First "+newDn);
            newList = new ArrayList<String>();
            entryConfigsByDn.put(newDn.toLowerCase(), newList);
        }
        newList.add(entryConfig.getId());

        Collection<EntryConfig> children = getChildren(entryConfig);

        if (children != null) {
            //addChildren(newDn, children);

            for (EntryConfig child : children) {
                String childNewDn = child.getRdn() + "," + newDn;
                //System.out.println(" - renaming child "+child.getDn()+" to "+childNewDn);

                renameChildren(child, childNewDn);
            }

            //removeChildren(oldDn);
        }
    }

    public void renameEntryConfig(EntryConfig entryConfig, DN newDn) throws Exception {

        EntryConfig oldParent = getParent(entryConfig);
        DN oldDn = entryConfig.getDn();

        if (debug) log.debug("Renaming "+oldDn+" to "+newDn);

        Collection<String> c = entryConfigsByDn.get(oldDn.getNormalizedDn());
        if (c == null) {
        	if (debug) log.debug("Entry "+oldDn+" not found.");
            return;
        }

        c.remove(entryConfig.getId());
        if (c.isEmpty()) {
        	if (debug) log.debug("Last "+oldDn);
            entryConfigsByDn.remove(oldDn.getNormalizedDn());
        }

        entryConfig.setDn(newDn);
        Collection<String> newList = entryConfigsByDn.get(newDn.getNormalizedDn());
        if (newList == null) {
        	if (debug) log.debug("First "+newDn);
            newList = new ArrayList<String>();
            entryConfigsByDn.put(newDn.getNormalizedDn(), newList);
        }
        newList.add(entryConfig.getId());

        EntryConfig newParent = getParent(entryConfig);
        if (debug) log.debug("New parent "+(newParent == null ? null : newParent.getDn()));

        if (newParent != null) {
            addChild(newParent.getId(), entryConfig.getId());
        }

        Collection<EntryConfig> children = getChildren(entryConfig);

        if (children != null) {
            //addChildren(newDn, children);

            for (EntryConfig child : children) {
                String childNewDn = child.getRdn() + "," + newDn;
                //System.out.println(" - renaming child "+child.getDn()+" to "+childNewDn);

                renameChildren(child, childNewDn);
            }

            //removeChildren(oldDn);
        }

        if (oldParent != null) {
            Collection oldSiblings = getChildren(oldParent);
            if (oldSiblings != null) oldSiblings.remove(entryConfig);
        }

    }

    public Collection<String> getChildIds(String parentId) {
        Collection<String> children = childrenById.get(parentId);
        if (children == null) return EMPTY_IDS;
        return children;
    }

    public void addChild(String parentId, String childId) {
        Collection<String> children = childrenById.get(parentId);
        if (children == null) {
            children = new ArrayList<String>();
            childrenById.put(parentId, children);
        }

        children.add(childId);
        parentById.put(childId, parentId);
    }

    public void removeChild(String parentId, String childId) throws Exception {
        parentById.remove(childId);

        Collection<String> children = childrenById.get(parentId);
        if (children == null) return;

        children.remove(childId);
        if (children.isEmpty()) childrenById.remove(parentId);
    }

    public Collection<EntryConfig> getChildren(EntryConfig parentConfig) {
        return getChildren(parentConfig.getId());
    }

    public Collection<EntryConfig> getChildren(String parentId) {
        Collection<String> children = childrenById.get(parentId);
        if (children == null) return EMPTY;
        return getEntryConfigs(children);
    }

    public void removeChildren(EntryConfig parentConfig) {
        Collection<String> childIds = childrenById.remove(parentConfig.getId());
        for (String childId : childIds) {
            parentById.remove(childId);
        }
    }

    public DN getSuffix() {
        String rootId = rootIds.iterator().next();
        EntryConfig rootEntry = getEntryConfig(rootId);
        return rootEntry.getDn();
    }
    
    public Collection<DN> getSuffixes() {
        Collection<DN> list = new ArrayList<DN>();
        for (EntryConfig entryConfig : getRootEntryConfigs()) {
            DN suffix = entryConfig.getDn();
            list.add(suffix);
        }
        return list;
    }

    public String getRootId() {
        if (rootIds.isEmpty()) return null;
        return rootIds.iterator().next();
    }
    
    public Collection<String> getRootIds() {
        return rootIds;
    }

    public Collection<EntryConfig> getRootEntryConfigs() {
        return getEntryConfigs(getRootIds());
    }

    public boolean contains(DN dn) throws Exception {
        for (DN suffix : getSuffixes()) {

            if (suffix.isEmpty() && dn.isEmpty() // Root DSE
                    || dn.endsWith(suffix)) {
                return true;
            }
        }

        return false;
    }

    public Collection<EntryConfig> getEntryConfigs(String dn) throws Exception {
        return getEntryConfigsByDn(new DN(dn));
    }

    public Object clone() throws CloneNotSupportedException {
        DirectoryConfig directoryConfig = (DirectoryConfig)super.clone();

        directoryConfig.entryConfigsById     = new LinkedHashMap<String,EntryConfig>();
        directoryConfig.entryConfigsByDn     = new LinkedHashMap<String,Collection<String>>();
        directoryConfig.entryConfigsBySource = new LinkedHashMap<String,Collection<String>>();

        directoryConfig.rootIds              = new ArrayList<String>();
        directoryConfig.parentById           = new LinkedHashMap<String,String>();
        directoryConfig.childrenById         = new LinkedHashMap<String,Collection<String>>();

        for (EntryConfig entryConfig : getEntryConfigs()) {
            try {
                directoryConfig.addEntryConfig((EntryConfig) entryConfig.clone());
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }

        return directoryConfig;
    }
}
