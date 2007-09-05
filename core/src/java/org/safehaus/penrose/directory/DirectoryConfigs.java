package org.safehaus.penrose.directory;

import org.safehaus.penrose.ldap.DN;
import org.safehaus.penrose.source.SourceConfig;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.mapping.SourceMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.io.Serializable;

/**
 * @author Endi Sukma Dewata
 */
public class DirectoryConfigs implements Serializable, Cloneable {

    static {
        log = LoggerFactory.getLogger(DirectoryConfigs.class);
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

    public void addEntryMapping(EntryMapping entryMapping) {

        String dn = entryMapping.getDn().getNormalizedDn();
        if (debug) log.debug("Adding entry "+dn+".");

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

    public void removeEntryMapping(EntryMapping entryMapping) {
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

    public Collection<EntryMapping> findEntryMappings(DN dn) throws Exception {
        if (dn == null) return EMPTY;
        //log.debug("Finding entry mappings \""+dn+"\" in partition "+getName());

        // search for static mappings
        Collection<EntryMapping> results = entryMappingsByDn.get(dn.getNormalizedDn());
        if (results != null) {
            //log.debug("Found "+results.size()+" mapping(s).");
            return results;
        }

        // can't find exact match -> search for parent mappings

        DN parentDn = dn.getParentDn();

        results = new ArrayList<EntryMapping>();
        Collection<EntryMapping> list;

        // if dn has no parent, check against root entries
        if (parentDn.isEmpty()) {
            //log.debug("Check root mappings");
            list = rootEntryMappings;

        } else {
            if (debug) log.debug("Search parent mappings for \""+parentDn+"\"");
            Collection<EntryMapping> parentMappings = findEntryMappings(parentDn);

            // if no parent mappings found, the entry doesn't exist in this partition
            if (parentMappings == null || parentMappings.isEmpty()) {
            	if (debug) log.debug("Entry mapping \""+parentDn+"\" not found");
                return EMPTY;
            }

            list = new ArrayList<EntryMapping>();

            // for each parent mapping found
            for (EntryMapping parentMapping : parentMappings) {
                if (debug) log.debug("Found parent " + parentMapping.getDn());

                String handlerName = parentMapping.getHandlerName();
                if ("PROXY".equals(handlerName)) { // if parent is proxy, include it in results
                    results.add(parentMapping);

                } else { // otherwise check for matching siblings
                    Collection<EntryMapping> children = getChildren(parentMapping);
                    list.addAll(children);
                }
            }
        }

        // check against each mapping in the list
        for (EntryMapping entryMapping : list) {

            if (debug) {
                log.debug("Checking DN pattern:");
                log.debug(" - " + dn);
                log.debug(" - " + entryMapping.getDn());
            }
            if (!dn.matches(entryMapping.getDn())) continue;

            if (debug) log.debug("Found " + entryMapping.getDn());
            results.add(entryMapping);
        }

        return results;
    }

    public Collection<EntryMapping> getEntryMappings() {
        Collection<EntryMapping> list = new ArrayList<EntryMapping>();
        for (Collection<EntryMapping> c : entryMappingsByDn.values()) {
            list.addAll(c);
        }
        return list;
    }

    public Collection<EntryMapping> getEntryMappings(DN dn) {
        if (dn == null) return EMPTY;

        Collection<EntryMapping> list = entryMappingsByDn.get(dn.getNormalizedDn());
        if (list == null) return EMPTY;

        return new ArrayList<EntryMapping>(list);
    }

    public void renameChildren(EntryMapping entryMapping, String newDn) {
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

    public void renameEntryMapping(EntryMapping entryMapping, DN newDn) {
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

    public boolean contains(DN dn) {
        for (EntryMapping rootEntryMapping : rootEntryMappings) {
            DN suffix = rootEntryMapping.getDn();

            if (suffix.isEmpty() && dn.isEmpty() // Root DSE
                    || dn.endsWith(suffix)) {
                return true;
            }
        }

        return false;
    }

    public Collection<EntryMapping> getEntryMappings(String dn) {
        return getEntryMappings(new DN(dn));
    }

    public SourceMapping getEffectiveSourceMapping(EntryMapping entryMapping, String name) {
        SourceMapping sourceMapping = entryMapping.getSourceMapping(name);
        if (sourceMapping != null) return sourceMapping;

        EntryMapping parent = getParent(entryMapping);
        if (parent != null) return getEffectiveSourceMapping(parent, name);

        return null;
    }

    public Collection<SourceMapping> getEffectiveSourceMappings(EntryMapping entryMapping) {
         Collection<SourceMapping> list = new ArrayList<SourceMapping>();
         list.addAll(entryMapping.getSourceMappings());

         EntryMapping parent = getParent(entryMapping);
         if (parent != null) list.addAll(getEffectiveSourceMappings(parent));

         return list;
     }

    public Collection<EntryMapping> findEntryMappings(String targetDn) throws Exception {
        if (targetDn == null) return null;
        return findEntryMappings(new DN(targetDn));
    }

    public List<EntryMapping> getPath(EntryMapping entryMapping) {
        List<EntryMapping> path = new ArrayList<EntryMapping>();

        while (entryMapping != null) {
            path.add(0, entryMapping);
            entryMapping = getParent(entryMapping);
        }

        return path;
    }

    public List<EntryMapping> getRelativePath(EntryMapping baseMapping, EntryMapping entryMapping) {
        List<EntryMapping> path = new ArrayList<EntryMapping>();

        while (entryMapping != null) {
            path.add(0, entryMapping);
            if (entryMapping == baseMapping) break;

            entryMapping = getParent(entryMapping);
        }

        return path;
    }

    public boolean isDynamic(EntryMapping entryMapping) {

        boolean dynamic = entryMapping.isDynamic();

        //log.debug("Mapping "+entryMapping.getDn()+" is "+(dynamic ? "dynamic" : "not dynamic"));
        if (dynamic) return true;

        EntryMapping parentMapping = getParent(entryMapping);
        if (parentMapping == null) return false;

        return isDynamic(parentMapping);
    }

    public Object clone() throws CloneNotSupportedException {
        DirectoryConfigs mappings = (DirectoryConfigs)super.clone();

        mappings.entryMappingsById = new LinkedHashMap<String,EntryMapping>();
        mappings.entryMappingsByDn = new LinkedHashMap<String,Collection<EntryMapping>>();
        mappings.entryMappingsBySource = new LinkedHashMap<String,Collection<EntryMapping>>();
        mappings.entryMappingsByParentId = new LinkedHashMap<String,Collection<EntryMapping>>();

        mappings.suffixes = new ArrayList<DN>();
        mappings.rootEntryMappings = new ArrayList<EntryMapping>();

        for (EntryMapping entryMapping : getEntryMappings()) {
            mappings.addEntryMapping((EntryMapping)entryMapping.clone());
        }

        return mappings;
    }
}
