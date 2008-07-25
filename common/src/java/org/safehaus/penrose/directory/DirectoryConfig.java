package org.safehaus.penrose.directory;

import org.safehaus.penrose.ldap.DN;
import org.safehaus.penrose.source.SourceConfig;
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

    public final static Collection<EntryConfig> EMPTY = new ArrayList<EntryConfig>();

    private Map<String,EntryConfig> entryConfigsById                   = new LinkedHashMap<String,EntryConfig>();
    private Map<String,Collection<EntryConfig>> entryConfigsByDn       = new LinkedHashMap<String,Collection<EntryConfig>>();
    private Map<String,Collection<EntryConfig>> entryConfigsBySource   = new LinkedHashMap<String,Collection<EntryConfig>>();
    private Map<String,Collection<EntryConfig>> entryConfigsByParentId = new LinkedHashMap<String,Collection<EntryConfig>>();

    private Collection<DN> suffixes = new ArrayList<DN>();
    private Collection<EntryConfig> rootEntryConfigs = new ArrayList<EntryConfig>();

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
        Collection<EntryConfig> c = entryConfigsByDn.get(normalizedDn);
        if (c == null) {
            c = new ArrayList<EntryConfig>();
            entryConfigsByDn.put(normalizedDn, c);
        }
        c.add(entryConfig);

        // index by source
        Collection<EntrySourceConfig> sourceConfigs = entryConfig.getSourceConfigs();
        for (EntrySourceConfig sourceConfig : sourceConfigs) {
            String sourceName = sourceConfig.getSourceName();
            c = entryConfigsBySource.get(sourceName);
            if (c == null) {
                c = new ArrayList<EntryConfig>();
                entryConfigsBySource.put(sourceName, c);
            }
            c.add(entryConfig);
        }

        String parentId = entryConfig.getParentId();

        if (parentId != null) {
            if (debug) log.debug(" - Searching parent with id "+parentId);
            EntryConfig parent = getEntryConfig(parentId);

            if (parent != null) {
                if (debug) log.debug(" - Found parent \""+parent.getDn()+"\".");
                parentId = parent.getId();
                entryConfig.setParentId(parentId);
                addChildren(parent, entryConfig);
                return;
            }
        }

        DN parentDn = entryConfig.getParentDn();

        if (!parentDn.isEmpty()) {

            if (debug) log.debug(" - Searching parent with dn \""+parentDn+"\".");
            Collection<EntryConfig> parents = getEntryConfigs(parentDn);

            if (!parents.isEmpty()) {
                EntryConfig parent = parents.iterator().next();
                if (debug) log.debug(" - Found parent \""+parent.getDn()+"\".");
                parentId = parent.getId();
                entryConfig.setParentId(parentId);
                addChildren(parent, entryConfig);
                return;
            }
        }

        if (debug) log.debug(" - Add suffix \""+dn+"\"");
        rootEntryConfigs.add(entryConfig);
        suffixes.add(entryConfig.getDn());
    }

    public boolean contains(EntryConfig entryConfig) {
        return entryConfigsById.containsKey(entryConfig.getId());
    }

    public EntryConfig getEntryConfig(String id) {
        return entryConfigsById.get(id);
    }

    public EntryConfig getParent(EntryConfig entryConfig) {
        if (entryConfig == null) return null;

        return entryConfigsById.get(entryConfig.getParentId());
    }

    public void updateEntryConfig(String id, EntryConfig entryConfig) throws Exception {

        EntryConfig oldEntryConfig = entryConfigsById.get(id);

        if (!oldEntryConfig.getDn().equals(entryConfig.getDn())) {
            renameEntryConfig(oldEntryConfig, entryConfig.getDn());
        }

        oldEntryConfig.copy(entryConfig);
        oldEntryConfig.setId(id);
    }

    public void removeEntryConfig(EntryConfig entryConfig) throws Exception {
        entryConfigsById.remove(entryConfig.getId());

        EntryConfig parent = getParent(entryConfig);
        if (parent == null) {
            rootEntryConfigs.remove(entryConfig);
            suffixes.remove(entryConfig.getDn());

        } else {
            Collection<EntryConfig> children = getChildren(parent);
            if (children != null) children.remove(entryConfig);
        }

        Collection<EntryConfig> c = entryConfigsByDn.get(entryConfig.getDn().getNormalizedDn());
        if (c == null) return;

        c.remove(entryConfig);
        if (c.isEmpty()) {
            entryConfigsByDn.remove(entryConfig.getDn().getNormalizedDn());
        }
    }

    public void removeEntryConfig(String id) throws Exception {
        EntryConfig entryConfig = entryConfigsById.remove(id);

        EntryConfig parent = getParent(entryConfig);
        if (parent == null) {
            rootEntryConfigs.remove(entryConfig);
            suffixes.remove(entryConfig.getDn());

        } else {
            Collection<EntryConfig> children = getChildren(parent);
            if (children != null) children.remove(entryConfig);
        }

        Collection<EntryConfig> c = entryConfigsByDn.get(entryConfig.getDn().getNormalizedDn());
        if (c == null) return;

        c.remove(entryConfig);
        if (c.isEmpty()) {
            entryConfigsByDn.remove(entryConfig.getDn().getNormalizedDn());
        }
    }

    public Collection<String> getEntryIds() {
        return entryConfigsById.keySet();
    }
    
    public Collection<EntryConfig> getEntryConfigs() {
        Collection<EntryConfig> list = new ArrayList<EntryConfig>();
        for (Collection<EntryConfig> c : entryConfigsByDn.values()) {
            list.addAll(c);
        }
        return list;
    }

    public Collection<EntryConfig> getEntryConfigs(DN dn) throws Exception {
        if (dn == null) return EMPTY;

        Collection<EntryConfig> list = entryConfigsByDn.get(dn.getNormalizedDn());
        if (list == null) return EMPTY;

        return new ArrayList<EntryConfig>(list);
    }

    public void renameChildren(EntryConfig entryConfig, String newDn) throws Exception {
        if (entryConfig == null) return;
        if (newDn.equals(entryConfig.getDn().toString())) return;

        DN oldDn = entryConfig.getDn();
        if (debug) log.debug("Renaming "+oldDn+" to "+newDn);

        Collection c = getEntryConfigs(oldDn);
        if (c == null) return;

        c.remove(entryConfig);
        if (c.isEmpty()) {
        	if (debug) log.debug("Last "+oldDn);
            entryConfigsByDn.remove(oldDn.getNormalizedDn());
        }

        entryConfig.setStringDn(newDn);
        Collection<EntryConfig> newList = entryConfigsByDn.get(newDn.toLowerCase());
        if (newList == null) {
        	if (debug) log.debug("First "+newDn);
            newList = new ArrayList<EntryConfig>();
            entryConfigsByDn.put(newDn.toLowerCase(), newList);
        }
        newList.add(entryConfig);

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

        Collection<EntryConfig> c = entryConfigsByDn.get(oldDn.getNormalizedDn());
        if (c == null) {
        	if (debug) log.debug("Entry "+oldDn+" not found.");
            return;
        }

        c.remove(entryConfig);
        if (c.isEmpty()) {
        	if (debug) log.debug("Last "+oldDn);
            entryConfigsByDn.remove(oldDn.getNormalizedDn());
        }

        entryConfig.setDn(newDn);
        Collection<EntryConfig> newList = entryConfigsByDn.get(newDn.getNormalizedDn());
        if (newList == null) {
        	if (debug) log.debug("First "+newDn);
            newList = new ArrayList<EntryConfig>();
            entryConfigsByDn.put(newDn.getNormalizedDn(), newList);
        }
        newList.add(entryConfig);

        EntryConfig newParent = getParent(entryConfig);
        if (debug) log.debug("New parent "+(newParent == null ? null : newParent.getDn()));

        if (newParent != null) {
            addChildren(newParent, entryConfig);
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

    public Collection<EntryConfig> getEntryConfigs(SourceConfig sourceConfig) {
        String sourceName = sourceConfig.getName();
        Collection<EntryConfig> list = entryConfigsBySource.get(sourceName);
        if (list == null) return EMPTY;
        return list;
    }

    public void addChildren(EntryConfig parentConfig, Collection<EntryConfig> newChildren) {
        Collection<EntryConfig> children = entryConfigsByParentId.get(parentConfig.getId());
        if (children == null) {
            children = new ArrayList<EntryConfig>();
            entryConfigsByParentId.put(parentConfig.getId(), children);
        }
        children.addAll(newChildren);
    }

    public void addChildren(EntryConfig parentConfig, EntryConfig entryConfig) {
    	//if (debug) log.debug("Adding "+entryConfig.getDn()+" under "+parentConfig.getDn());
        Collection<EntryConfig> children = entryConfigsByParentId.get(parentConfig.getId());
        if (children == null) {
            children = new ArrayList<EntryConfig>();
            entryConfigsByParentId.put(parentConfig.getId(), children);
        }
        children.add(entryConfig);
    }

    public Collection<EntryConfig> getChildren(EntryConfig parentConfig) {
        return getChildren(parentConfig.getId());
    }

    public Collection<EntryConfig> getChildren(String parentId) {
        Collection<EntryConfig> children = entryConfigsByParentId.get(parentId);
        if (children == null) return EMPTY;
        return children;
    }

    public Collection<EntryConfig> removeChildren(EntryConfig parentConfig) {
        return entryConfigsByParentId.remove(parentConfig.getId());
    }

    public Collection<DN> getSuffixes() {
        return suffixes;
    }

    public void setRootEntryConfigs(Collection<EntryConfig> rootEntryConfigs) {
        this.rootEntryConfigs = rootEntryConfigs;
    }

    public Collection<EntryConfig> getRootEntryConfigs() {
        return rootEntryConfigs;
    }

    public boolean contains(DN dn) throws Exception {
        for (EntryConfig rootEntryConfig : rootEntryConfigs) {
            DN suffix = rootEntryConfig.getDn();

            if (suffix.isEmpty() && dn.isEmpty() // Root DSE
                    || dn.endsWith(suffix)) {
                return true;
            }
        }

        return false;
    }

    public Collection<EntryConfig> getEntryConfigs(String dn) throws Exception {
        return getEntryConfigs(new DN(dn));
    }

    public Object clone() throws CloneNotSupportedException {
        DirectoryConfig directoryConfig = (DirectoryConfig)super.clone();

        directoryConfig.entryConfigsById = new LinkedHashMap<String, EntryConfig>();
        directoryConfig.entryConfigsByDn = new LinkedHashMap<String,Collection<EntryConfig>>();
        directoryConfig.entryConfigsBySource = new LinkedHashMap<String,Collection<EntryConfig>>();
        directoryConfig.entryConfigsByParentId = new LinkedHashMap<String,Collection<EntryConfig>>();

        directoryConfig.suffixes = new ArrayList<DN>();
        directoryConfig.rootEntryConfigs = new ArrayList<EntryConfig>();

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
