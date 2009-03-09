package org.safehaus.penrose.directory;

import org.safehaus.penrose.ldap.DN;
import org.safehaus.penrose.ldap.RDN;
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

    public final static Collection<String> EMPTY_IDS  = new ArrayList<String>();
    public final static Collection<EntryConfig> EMPTY = new ArrayList<EntryConfig>();

    protected List<EntryConfig> entryConfigs                           = new ArrayList<EntryConfig>();
    protected Map<String,EntryConfig> entryConfigByName                = new LinkedHashMap<String,EntryConfig>();
    protected Map<String,Collection<EntryConfig>> entryConfigsByDn     = new LinkedHashMap<String,Collection<EntryConfig>>();
    protected Map<String,Collection<EntryConfig>> entryConfigsBySource = new LinkedHashMap<String,Collection<EntryConfig>>();

    protected Collection<String> rootNames                  = new ArrayList<String>();
    protected Map<String,String> parentByName               = new LinkedHashMap<String,String>();
    protected Map<String,Collection<String>> childrenByName = new LinkedHashMap<String,Collection<String>>();

    public DirectoryConfig() {
    }

    public void addEntryConfig(EntryConfig entryConfig) throws Exception {

        boolean debug = log.isDebugEnabled();
        DN dn = entryConfig.getDn();
        if (debug) log.debug("Adding entry \""+dn+"\".");

        entryConfigs.add(entryConfig);

        String name = entryConfig.getName();
        if (name == null) {
            int counter = 0;
            name = "entry"+counter;
            while (entryConfigByName.containsKey(name)) {
                counter++;
                name = "entry"+counter;
            }
            entryConfig.setName(name);
        }
        if (debug) log.debug(" - ID: "+name);

        // index by name
        entryConfigByName.put(name, entryConfig);

        // index by dn
        String normalizedDn = dn.getNormalizedDn();
        Collection<EntryConfig> c1 = entryConfigsByDn.get(normalizedDn);
        if (c1 == null) {
            c1 = new LinkedHashSet<EntryConfig>();
            entryConfigsByDn.put(normalizedDn, c1);
        }
        c1.add(entryConfig);

        // index by source
        for (EntrySourceConfig sourceConfig : entryConfig.getSourceConfigs()) {
            String sourceName = sourceConfig.getSourceName();
            Collection<EntryConfig> c2 = entryConfigsBySource.get(sourceName);
            if (c2 == null) {
                c2 = new LinkedHashSet<EntryConfig>();
                entryConfigsBySource.put(sourceName, c2);
            }
            c2.add(entryConfig);
        }

        String parentName = entryConfig.getParentName();

        if (parentName != null) {
            if (debug) log.debug(" - Searching parent with name "+parentName);
            EntryConfig parent = getEntryConfig(parentName);

            if (parent != null) {
                if (debug) log.debug(" - Found parent \""+parent.getDn()+"\".");
                addChild(parentName, entryConfig.getName());
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
                addChild(parent.getName(), entryConfig.getName());
                return;
            }
        }

        if (debug) log.debug(" - Add suffix \""+dn+"\"");
        rootNames.add(name);
    }

    public boolean contains(EntryConfig entryConfig) {
        return entryConfigByName.containsKey(entryConfig.getName());
    }

    public EntryConfig getEntryConfig(String name) {
        return entryConfigByName.get(name);
    }

    public String getParentName(String name) {
        return parentByName.get(name);
    }
    
    public EntryConfig getParent(EntryConfig entryConfig) {
        if (entryConfig == null) return null;

        String parentName = parentByName.get(entryConfig.getName());
        return entryConfigByName.get(parentName);
    }

    public void updateEntryConfig(String name, EntryConfig entryConfig) throws Exception {

        removeEntryConfig(name);
        addEntryConfig(entryConfig);
/*
        EntryConfig oldEntryConfig = entryConfigsById.get(name);

        DN oldParentDn = oldEntryConfig.getParentDn();
        DN parentDn = entryConfig.getParentDn();

        if (!oldParentDn.equals(parentDn)) {
            throw new Exception("Modify DN operation is not supported.");
        }

        RDN oldRdn = oldEntryConfig.getRdn();
        RDN rdn = entryConfig.getRdn();

        if (!oldRdn.equals(rdn)) {
            renameEntryConfig(oldEntryConfig, rdn);
        }

        oldEntryConfig.copy(entryConfig);
*/
    }

    public void removeEntryConfig(String name) throws Exception {
        
        boolean debug = log.isDebugEnabled();
        for (String childName : getChildNames(name)) {
            removeEntryConfig(childName);
        }

        EntryConfig entryConfig = entryConfigByName.remove(name);
        if (entryConfig == null) return;

        entryConfigs.remove(entryConfig);

        String parentName = parentByName.get(name);
        if (parentName == null) {
            if (debug) log.debug("Removing root entry "+name+".");
            rootNames.remove(name);

        } else {
            if (debug) log.debug("Removing entry "+name+" from parent "+parentName+".");
            parentByName.remove(name);

            Collection<String> children = childrenByName.get(parentName);
            if (children != null) {
                children.remove(name);
                if (children.isEmpty()) childrenByName.remove(parentName);
            }
        }

        String normalizedDn = entryConfig.getDn().getNormalizedDn();
        Collection<EntryConfig> c = entryConfigsByDn.get(normalizedDn);
        if (c != null) {
            c.remove(entryConfig);
            if (c.isEmpty()) entryConfigsByDn.remove(normalizedDn);
        }

        for (String sourceName : entryConfig.getSourceNames()) {
            Collection<EntryConfig> c2 = entryConfigsBySource.get(sourceName);
            if (c2 != null) {
                c2.remove(entryConfig);
                if (c2.isEmpty()) entryConfigsBySource.remove(sourceName);
            }
        }
    }

    public Collection<EntryConfig> getEntryConfigs() {
        return entryConfigs;
    }

    public Collection<EntryConfig> getEntryConfigs(Collection<String> names) {
        Collection<EntryConfig> entryConfigs = new LinkedHashSet<EntryConfig>();
        for (String name : names) {
            EntryConfig entryConfig = entryConfigByName.get(name);
            if (entryConfig == null) continue;
            entryConfigs.add(entryConfig);
        }
        return entryConfigs;
    }

    public Collection<String> getEntryNames() {
        return entryConfigByName.keySet();
    }
    
    public Collection<String> getEntryNames(Collection<EntryConfig> entryConfigs) throws Exception {
        Collection<String> names = new LinkedHashSet<String>();
        for (EntryConfig entryConfig : entryConfigs) {
            names.add(entryConfig.getName());
        }
        return names;
    }

    public Collection<String> getEntryNamesByDn(DN dn) throws Exception {
        if (dn == null) return EMPTY_IDS;

        Collection<EntryConfig> list = entryConfigsByDn.get(dn.getNormalizedDn());
        if (list == null) return EMPTY_IDS;

        return getEntryNames(list);
    }

    public String getEntryNameByDn(DN dn) throws Exception {
        if (dn == null) return null;

        Collection<EntryConfig> list = entryConfigsByDn.get(dn.getNormalizedDn());
        if (list == null) return null;
        if (list.isEmpty()) return null;

        return list.iterator().next().getName();
    }

    public Collection<String> getEntryNamesBySource(String sourceName) throws Exception {
        Collection<EntryConfig> list = entryConfigsBySource.get(sourceName);
        if (list == null) return EMPTY_IDS;

        return getEntryNames(list);
    }

    public Collection<EntryConfig> getEntryConfigsBySource(String sourceName) throws Exception {
        Collection<EntryConfig> list = entryConfigsBySource.get(sourceName);
        if (list == null) return EMPTY;

        return list;
    }

    public Collection<EntryConfig> getEntryConfigsByDn(DN dn) throws Exception {
        if (dn == null) return EMPTY;

        Collection<EntryConfig> list = entryConfigsByDn.get(dn.getNormalizedDn());
        if (list == null) return EMPTY;

        return list;
    }

    public void renameChildren(EntryConfig entryConfig, String newDn) throws Exception {
        boolean debug = log.isDebugEnabled();
        if (entryConfig == null) return;
        if (newDn.equals(entryConfig.getDn().toString())) return;

        DN oldDn = entryConfig.getDn();
        if (debug) log.debug("Renaming "+oldDn+" to "+newDn);

        Collection<EntryConfig> c = getEntryConfigsByDn(oldDn);
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
            newList = new LinkedHashSet<EntryConfig>();
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

        boolean debug = log.isDebugEnabled();
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
            newList = new LinkedHashSet<EntryConfig>();
            entryConfigsByDn.put(newDn.getNormalizedDn(), newList);
        }
        newList.add(entryConfig);

        EntryConfig newParent = getParent(entryConfig);
        if (debug) log.debug("New parent "+(newParent == null ? null : newParent.getDn()));

        if (newParent != null) {
            addChild(newParent.getName(), entryConfig.getName());
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

    public void renameEntryConfig(EntryConfig entryConfig, RDN newRdn) throws Exception {

        boolean debug = log.isDebugEnabled();
        EntryConfig oldParent = getParent(entryConfig);
        DN oldDn = entryConfig.getDn();

        if (debug) log.debug("Renaming "+oldDn+" to "+newRdn+".");

        Collection<EntryConfig> c = entryConfigsByDn.get(oldDn.getNormalizedDn());
        if (c == null) {
        	if (debug) log.debug("Entry "+oldDn+" not found.");
            return;
        }

        c.remove(entryConfig);
        if (c.isEmpty()) {
        	if (debug) log.debug("Last "+oldDn+".");
            entryConfigsByDn.remove(oldDn.getNormalizedDn());
        }

        DN newDn = newRdn.append(oldParent.getDn());
        entryConfig.setDn(newDn);
        Collection<EntryConfig> newList = entryConfigsByDn.get(newDn.getNormalizedDn());
        if (newList == null) {
        	if (debug) log.debug("First "+newDn+".");
            newList = new LinkedHashSet<EntryConfig>();
            entryConfigsByDn.put(newDn.getNormalizedDn(), newList);
        }
        newList.add(entryConfig);
    }

    public Collection<String> getChildNames(String parentName) {
        Collection<String> children = childrenByName.get(parentName);
        if (children == null) return EMPTY_IDS;
        return children;
    }

    public void addChild(String parentName, String childName) {

        boolean debug = log.isDebugEnabled();
        if (debug) log.debug("Adding child "+childName+" to parent "+parentName+".");

        Collection<String> children = childrenByName.get(parentName);
        if (children == null) {
            children = new LinkedHashSet<String>();
            childrenByName.put(parentName, children);
        }

        children.add(childName);
        parentByName.put(childName, parentName);
    }

    public void removeChild(String parentName, String childName) throws Exception {

        boolean debug = log.isDebugEnabled();
        if (debug) log.debug("Removing child "+childName+" from parent "+parentName+".");

        parentByName.remove(childName);

        Collection<String> children = childrenByName.get(parentName);
        if (children == null) return;

        children.remove(childName);
        if (children.isEmpty()) childrenByName.remove(parentName);
    }

    public Collection<EntryConfig> getChildren(EntryConfig parentConfig) {
        return getChildren(parentConfig.getName());
    }

    public Collection<EntryConfig> getChildren(String name) {
        Collection<String> children = childrenByName.get(name);
        if (children == null) return EMPTY;
        return getEntryConfigs(children);
    }

    public void removeChildren(EntryConfig parentConfig) {
        Collection<String> names = childrenByName.remove(parentConfig.getName());
        for (String name : names) {
            parentByName.remove(name);
        }
    }

    public DN getSuffix() {
        String name = rootNames.iterator().next();
        EntryConfig rootEntry = getEntryConfig(name);
        return rootEntry.getDn();
    }
    
    public Collection<DN> getSuffixes() {
        Collection<DN> list = new LinkedHashSet<DN>();
        for (EntryConfig entryConfig : getRootEntryConfigs()) {
            DN suffix = entryConfig.getDn();
            list.add(suffix);
        }
        return list;
    }

    public String getRootName() {
        if (rootNames.isEmpty()) return null;
        return rootNames.iterator().next();
    }
    
    public Collection<String> getRootNames() {
        return rootNames;
    }

    public Collection<EntryConfig> getRootEntryConfigs() {
        return getEntryConfigs(getRootNames());
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

        directoryConfig.entryConfigs         = new ArrayList<EntryConfig>();
        directoryConfig.entryConfigByName    = new LinkedHashMap<String,EntryConfig>();
        directoryConfig.entryConfigsByDn     = new LinkedHashMap<String,Collection<EntryConfig>>();
        directoryConfig.entryConfigsBySource = new LinkedHashMap<String,Collection<EntryConfig>>();

        directoryConfig.rootNames = new ArrayList<String>();
        directoryConfig.parentByName = new LinkedHashMap<String,String>();
        directoryConfig.childrenByName = new LinkedHashMap<String,Collection<String>>();

        for (EntryConfig entryConfig : entryConfigs) {
            try {
                directoryConfig.addEntryConfig((EntryConfig) entryConfig.clone());
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }

        return directoryConfig;
    }
}
