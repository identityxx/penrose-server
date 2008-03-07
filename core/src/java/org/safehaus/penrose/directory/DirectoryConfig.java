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

    public final static Collection<EntryMapping> EMPTY = new ArrayList<EntryMapping>();

    private Map<String,EntryMapping> entryMappingsById = new LinkedHashMap<String,EntryMapping>();
    private Map<String,Collection<EntryMapping>> entryMappingsByDn = new LinkedHashMap<String,Collection<EntryMapping>>();
    private Map<String,Collection<EntryMapping>> entryMappingsBySource = new LinkedHashMap<String,Collection<EntryMapping>>();
    private Map<String,Collection<EntryMapping>> entryMappingsByParentId = new LinkedHashMap<String,Collection<EntryMapping>>();

    private Collection<DN> suffixes = new ArrayList<DN>();
    private Collection<EntryMapping> rootEntryMappings = new ArrayList<EntryMapping>();

    public void addEntryMapping(EntryMapping entryMapping) throws Exception {

        String dn = entryMapping.getDn().getNormalizedDn();
        //if (debug) log.debug("Adding entry \""+dn+"\".");

        String id = entryMapping.getId();
        if (id == null) {
            id = ""+ entryMappingsById.size();
            entryMapping.setId(id);
        }
        //if (debug) log.debug("ID: "+id);

        // lookup by id
        entryMappingsById.put(id, entryMapping);

        // lookup by dn
        Collection<EntryMapping> c = entryMappingsByDn.get(dn);
        if (c == null) {
            c = new ArrayList<EntryMapping>();
            entryMappingsByDn.put(dn, c);
        }
        c.add(entryMapping);

        // lookup by source
        Collection<SourceMapping> sourceMappings = entryMapping.getSourceMappings();
        for (SourceMapping sourceMapping : sourceMappings) {
            String sourceName = sourceMapping.getSourceName();
            c = entryMappingsBySource.get(sourceName);
            if (c == null) {
                c = new ArrayList<EntryMapping>();
                entryMappingsBySource.put(sourceName, c);
            }
            c.add(entryMapping);
        }

        EntryMapping parent = null;

        String parentId = entryMapping.getParentId();
        if (parentId == null) {
            DN parentDn = entryMapping.getParentDn();
            if (!parentDn.isEmpty()) {
                c = getEntryMappings(parentDn);
                if (c != null && !c.isEmpty()) {
                    parent = c.iterator().next();
                    parentId = parent.getId();
                    entryMapping.setParentId(parentId);
                }
            }

        } else {
            parent = getEntryMappingById(parentId);
        }

        if (parent == null) {
        	//if (debug) log.debug("Suffix: "+dn);
            rootEntryMappings.add(entryMapping);
            suffixes.add(entryMapping.getDn());
        } else {
        	//if (debug) log.debug("Parent ID: "+parentId);
            addChildren(parent, entryMapping);
        }
    }

    public boolean contains(EntryMapping entryMapping) {
        return entryMappingsById.containsKey(entryMapping.getId());
    }

    public EntryMapping getEntryMappingById(String id) {
        return entryMappingsById.get(id);
    }

    public EntryMapping getParent(EntryMapping entryMapping) {
        if (entryMapping == null) return null;

        return entryMappingsById.get(entryMapping.getParentId());
    }

    public void removeEntryMapping(EntryMapping entryMapping) throws Exception {
        entryMappingsById.remove(entryMapping.getId());

        EntryMapping parent = getParent(entryMapping);
        if (parent == null) {
            rootEntryMappings.remove(entryMapping);
            suffixes.remove(entryMapping.getDn());

        } else {
            Collection children = getChildren(parent);
            if (children != null) children.remove(entryMapping);
        }

        Collection<EntryMapping> c = entryMappingsByDn.get(entryMapping.getDn().getNormalizedDn());
        if (c == null) return;

        c.remove(entryMapping);
        if (c.isEmpty()) {
            entryMappingsByDn.remove(entryMapping.getDn().getNormalizedDn());
        }
    }

    public Collection<EntryMapping> getEntryMappings() {
        Collection<EntryMapping> list = new ArrayList<EntryMapping>();
        for (Collection<EntryMapping> c : entryMappingsByDn.values()) {
            list.addAll(c);
        }
        return list;
    }

    public Collection<EntryMapping> getEntryMappings(DN dn) throws Exception {
        if (dn == null) return EMPTY;

        Collection<EntryMapping> list = entryMappingsByDn.get(dn.getNormalizedDn());
        if (list == null) return EMPTY;

        return new ArrayList<EntryMapping>(list);
    }

    public void renameChildren(EntryMapping entryMapping, String newDn) throws Exception {
        if (entryMapping == null) return;
        if (newDn.equals(entryMapping.getDn())) return;

        DN oldDn = entryMapping.getDn();
        if (debug) log.debug("Renaming "+oldDn+" to "+newDn);

        Collection c = getEntryMappings(oldDn);
        if (c == null) return;

        c.remove(entryMapping);
        if (c.isEmpty()) {
        	if (debug) log.debug("Last "+oldDn);
            entryMappingsByDn.remove(oldDn.getNormalizedDn());
        }

        entryMapping.setStringDn(newDn);
        Collection<EntryMapping> newList = entryMappingsByDn.get(newDn.toLowerCase());
        if (newList == null) {
        	if (debug) log.debug("First "+newDn);
            newList = new ArrayList<EntryMapping>();
            entryMappingsByDn.put(newDn.toLowerCase(), newList);
        }
        newList.add(entryMapping);

        Collection<EntryMapping> children = getChildren(entryMapping);

        if (children != null) {
            //addChildren(newDn, children);

            for (EntryMapping child : children) {
                String childNewDn = child.getRdn() + "," + newDn;
                //System.out.println(" - renaming child "+child.getDn()+" to "+childNewDn);

                renameChildren(child, childNewDn);
            }

            //removeChildren(oldDn);
        }
    }

    public void renameEntryMapping(EntryMapping entryMapping, DN newDn) throws Exception {
        if (entryMapping == null) return;
        if (entryMapping.getDn().matches(newDn)) return;

        EntryMapping oldParent = getParent(entryMapping);
        DN oldDn = entryMapping.getDn();

        if (debug) log.debug("Renaming "+oldDn+" to "+newDn);

        Collection<EntryMapping> c = entryMappingsByDn.get(oldDn.getNormalizedDn());
        if (c == null) {
        	if (debug) log.debug("Entry "+oldDn+" not found.");
            return;
        }

        c.remove(entryMapping);
        if (c.isEmpty()) {
        	if (debug) log.debug("Last "+oldDn);
            entryMappingsByDn.remove(oldDn.getNormalizedDn());
        }

        entryMapping.setDn(newDn);
        Collection<EntryMapping> newList = entryMappingsByDn.get(newDn.getNormalizedDn());
        if (newList == null) {
        	if (debug) log.debug("First "+newDn);
            newList = new ArrayList<EntryMapping>();
            entryMappingsByDn.put(newDn.getNormalizedDn(), newList);
        }
        newList.add(entryMapping);

        EntryMapping newParent = getParent(entryMapping);
        if (debug) log.debug("New parent "+(newParent == null ? null : newParent.getDn()));

        if (newParent != null) {
            addChildren(newParent, entryMapping);
        }

        Collection<EntryMapping> children = getChildren(entryMapping);

        if (children != null) {
            //addChildren(newDn, children);

            for (EntryMapping child : children) {
                String childNewDn = child.getRdn() + "," + newDn;
                //System.out.println(" - renaming child "+child.getDn()+" to "+childNewDn);

                renameChildren(child, childNewDn);
            }

            //removeChildren(oldDn);
        }

        if (oldParent != null) {
            Collection oldSiblings = getChildren(oldParent);
            if (oldSiblings != null) oldSiblings.remove(entryMapping);
        }

    }

    public Collection<EntryMapping> getEntryMappings(SourceConfig sourceConfig) {
        String sourceName = sourceConfig.getName();
        Collection<EntryMapping> list = entryMappingsBySource.get(sourceName);
        if (list == null) return EMPTY;
        return list;
    }

    public void addChildren(EntryMapping parentMapping, Collection<EntryMapping> newChildren) {
        Collection<EntryMapping> children = entryMappingsByParentId.get(parentMapping.getId());
        if (children == null) {
            children = new ArrayList<EntryMapping>();
            entryMappingsByParentId.put(parentMapping.getId(), children);
        }
        children.addAll(newChildren);
    }

    public void addChildren(EntryMapping parentMapping, EntryMapping entryMapping) {
    	//if (debug) log.debug("Adding "+entryMapping.getDn()+" under "+parentMapping.getDn());
        Collection<EntryMapping> children = entryMappingsByParentId.get(parentMapping.getId());
        if (children == null) {
            children = new ArrayList<EntryMapping>();
            entryMappingsByParentId.put(parentMapping.getId(), children);
        }
        children.add(entryMapping);
    }

    public Collection<EntryMapping> getChildren(EntryMapping parentMapping) {
        Collection<EntryMapping> children = entryMappingsByParentId.get(parentMapping.getId());
        if (children == null) return EMPTY;
        return children;
    }

    public Collection<EntryMapping> removeChildren(EntryMapping parentMapping) {
        return entryMappingsByParentId.remove(parentMapping.getId());
    }

    public Collection<DN> getSuffixes() {
        return suffixes;
    }

    public void setRootEntryMappings(Collection<EntryMapping> rootEntryMappings) {
        this.rootEntryMappings = rootEntryMappings;
    }

    public Collection<EntryMapping> getRootEntryMappings() {
        return rootEntryMappings;
    }

    public boolean contains(DN dn) throws Exception {
        for (EntryMapping rootEntryMapping : rootEntryMappings) {
            DN suffix = rootEntryMapping.getDn();

            if (suffix.isEmpty() && dn.isEmpty() // Root DSE
                    || dn.endsWith(suffix)) {
                return true;
            }
        }

        return false;
    }

    public Collection<EntryMapping> getEntryMappings(String dn) throws Exception {
        return getEntryMappings(new DN(dn));
    }

    public Object clone() throws CloneNotSupportedException {
        DirectoryConfig mappings = (DirectoryConfig)super.clone();

        mappings.entryMappingsById = new LinkedHashMap<String,EntryMapping>();
        mappings.entryMappingsByDn = new LinkedHashMap<String,Collection<EntryMapping>>();
        mappings.entryMappingsBySource = new LinkedHashMap<String,Collection<EntryMapping>>();
        mappings.entryMappingsByParentId = new LinkedHashMap<String,Collection<EntryMapping>>();

        mappings.suffixes = new ArrayList<DN>();
        mappings.rootEntryMappings = new ArrayList<EntryMapping>();

        for (EntryMapping entryMapping : getEntryMappings()) {
            try {
                mappings.addEntryMapping((EntryMapping)entryMapping.clone());
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }

        return mappings;
    }
}
