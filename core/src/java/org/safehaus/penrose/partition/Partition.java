/**
 * Copyright (c) 2000-2006, Identyx Corporation.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.safehaus.penrose.partition;

import java.util.*;

import org.safehaus.penrose.module.ModuleMapping;
import org.safehaus.penrose.module.ModuleConfig;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.ldap.DN;
import org.safehaus.penrose.source.Sources;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/**
 * @author Endi S. Dewata
 */
public class Partition {

    Logger log = LoggerFactory.getLogger(getClass());

    public final static Collection<EntryMapping> EMPTY = new ArrayList<EntryMapping>();

    private PartitionConfig partitionConfig;

    private Map<String,EntryMapping> entryMappingsById = new LinkedHashMap<String,EntryMapping>();
    private Map<String,Collection<EntryMapping>> entryMappingsByDn = new LinkedHashMap<String,Collection<EntryMapping>>();
    private Map<String,Collection<EntryMapping>> entryMappingsBySource = new LinkedHashMap<String,Collection<EntryMapping>>();
    private Map<String,Collection<EntryMapping>> entryMappingsByParentId = new LinkedHashMap<String,Collection<EntryMapping>>();

    private Collection<DN> suffixes = new ArrayList<DN>();
    private Collection<EntryMapping> rootEntryMappings = new ArrayList<EntryMapping>();

    private Map<String,ConnectionConfig> connectionConfigs = new LinkedHashMap<String,ConnectionConfig>();
    private Sources sources = new Sources();

    private Map<String,ModuleConfig> moduleConfigs = new LinkedHashMap<String,ModuleConfig>();
    private Map<String,Collection<ModuleMapping>> moduleMappings = new LinkedHashMap<String,Collection<ModuleMapping>>();

    public Partition(PartitionConfig partitionConfig) {
        this.partitionConfig = partitionConfig;
    }

    public String getName() {
        return partitionConfig.getName();
    }

    public String getHandlerName() {
        return partitionConfig.getHandlerName();
    }

    public String getEngineName() {
        return partitionConfig.getEngineName();
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

    public boolean contains(EntryMapping entryMapping) {
        return entryMappingsById.containsKey(entryMapping.getId());
    }

    public Collection<EntryMapping> getEntryMappings(String dn) {
        return getEntryMappings(new DN(dn));
    }

    public Collection<EntryMapping> getEntryMappings(DN dn) {
        if (dn == null) return EMPTY;

        Collection<EntryMapping> list = entryMappingsByDn.get(dn.getNormalizedDn());
        if (list == null) return EMPTY;

        return new ArrayList<EntryMapping>(list);
    }

    public Collection<EntryMapping> getEntryMappings(SourceConfig sourceConfig) {
        String sourceName = sourceConfig.getName();
        Collection<EntryMapping> list = entryMappingsBySource.get(sourceName);
        if (list == null) return EMPTY;
        return list;
    }

    public void addEntryMapping(EntryMapping entryMapping) throws Exception {

        String dn = entryMapping.getDn().getNormalizedDn();
        //if (log.isDebugEnabled()) log.debug("Adding entry "+dn);

        String id = entryMapping.getId();
        if (id == null) {
            id = ""+ entryMappingsById.size();
            entryMapping.setId(id);
        }
        //if (log.isDebugEnabled()) log.debug("ID: "+id);

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
        	//if (log.isDebugEnabled()) log.debug("Suffix: "+dn);
            rootEntryMappings.add(entryMapping);
            suffixes.add(entryMapping.getDn());
        } else {
        	//if (log.isDebugEnabled()) log.debug("Parent ID: "+parentId);
            addChildren(parent, entryMapping);
        }
    }

    public EntryMapping getEntryMappingById(String id) {
        return entryMappingsById.get(id);
    }

    public void modifyEntryMapping(EntryMapping oldEntry, EntryMapping newEntry) {
        oldEntry.copy(newEntry);
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

    public void renameEntryMapping(EntryMapping entryMapping, DN newDn) {
        if (entryMapping == null) return;
        if (entryMapping.getDn().equals(newDn)) return;

        EntryMapping oldParent = getParent(entryMapping);
        DN oldDn = entryMapping.getDn();

        if (log.isDebugEnabled()) log.debug("Renaming "+oldDn+" to "+newDn);

        Collection<EntryMapping> c = entryMappingsByDn.get(oldDn.getNormalizedDn());
        if (c == null) {
        	if (log.isDebugEnabled()) log.debug("Entry "+oldDn+" not found.");
            return;
        }

        c.remove(entryMapping);
        if (c.isEmpty()) {
        	if (log.isDebugEnabled()) log.debug("Last "+oldDn);
            entryMappingsByDn.remove(oldDn.getNormalizedDn());
        }

        entryMapping.setDn(newDn);
        Collection<EntryMapping> newList = entryMappingsByDn.get(newDn.getNormalizedDn());
        if (newList == null) {
        	if (log.isDebugEnabled()) log.debug("First "+newDn);
            newList = new ArrayList<EntryMapping>();
            entryMappingsByDn.put(newDn.getNormalizedDn(), newList);
        }
        newList.add(entryMapping);

        EntryMapping newParent = getParent(entryMapping);
        if (log.isDebugEnabled()) log.debug("New parent "+(newParent == null ? null : newParent.getDn()));

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

    public void renameChildren(EntryMapping entryMapping, String newDn) {
        if (entryMapping == null) return;
        if (newDn.equals(entryMapping.getDn())) return;

        DN oldDn = entryMapping.getDn();
        if (log.isDebugEnabled()) log.debug("Renaming "+oldDn+" to "+newDn);

        Collection c = getEntryMappings(oldDn);
        if (c == null) return;

        c.remove(entryMapping);
        if (c.isEmpty()) {
        	if (log.isDebugEnabled()) log.debug("Last "+oldDn);
            entryMappingsByDn.remove(oldDn.getNormalizedDn());
        }

        entryMapping.setStringDn(newDn);
        Collection<EntryMapping> newList = entryMappingsByDn.get(newDn.toLowerCase());
        if (newList == null) {
        	if (log.isDebugEnabled()) log.debug("First "+newDn);
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

    public List<EntryMapping> getPath(EntryMapping entryMapping) {
        List<EntryMapping> path = new ArrayList<EntryMapping>();
        
        while (entryMapping != null) {
            path.add(0, entryMapping);
            entryMapping = getParent(entryMapping);
        }

        return path;
    }

    public EntryMapping getParent(EntryMapping entryMapping) {
        if (entryMapping == null) return null;

        return entryMappingsById.get(entryMapping.getParentId());
    }

    public Collection<EntryMapping> getChildren(EntryMapping parentMapping) {
        Collection<EntryMapping> children = entryMappingsByParentId.get(parentMapping.getId());
        if (children == null) return EMPTY;
        return children;
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
    	//if (log.isDebugEnabled()) log.debug("Adding "+entryMapping.getDn()+" under "+parentMapping.getDn());
        Collection<EntryMapping> children = entryMappingsByParentId.get(parentMapping.getId());
        if (children == null) {
            children = new ArrayList<EntryMapping>();
            entryMappingsByParentId.put(parentMapping.getId(), children);
        }
        children.add(entryMapping);
    }

    public Collection<EntryMapping> removeChildren(EntryMapping parentMapping) {
        return entryMappingsByParentId.remove(parentMapping.getId());
    }

   public Collection<SourceMapping> getEffectiveSourceMappings(EntryMapping entryMapping) {
        Collection<SourceMapping> list = new ArrayList<SourceMapping>();
        list.addAll(entryMapping.getSourceMappings());

        EntryMapping parent = getParent(entryMapping);
        if (parent != null) list.addAll(getEffectiveSourceMappings(parent));

        return list;
    }

    public SourceMapping getEffectiveSourceMapping(EntryMapping entryMapping, String name) {
        SourceMapping sourceMapping = entryMapping.getSourceMapping(name);
        if (sourceMapping != null) return sourceMapping;

        EntryMapping parent = getParent(entryMapping);
        if (parent != null) return getEffectiveSourceMapping(parent, name);

        return null;
    }

    public void addModuleConfig(ModuleConfig moduleConfig) throws Exception {
        moduleConfigs.put(moduleConfig.getName(), moduleConfig);
    }

    public ModuleConfig getModuleConfig(String name) {
        return moduleConfigs.get(name);
    }

    public Collection<ModuleMapping> getModuleMappings(String name) {
        return moduleMappings.get(name);
    }

    public void addModuleMapping(ModuleMapping mapping) throws Exception {
        Collection<ModuleMapping> c = moduleMappings.get(mapping.getModuleName());
        if (c == null) {
            c = new ArrayList<ModuleMapping>();
            moduleMappings.put(mapping.getModuleName(), c);
        }
        c.add(mapping);

        String moduleName = mapping.getModuleName();
        if (moduleName == null) throw new Exception("Missing module name");

        ModuleConfig moduleConfig = getModuleConfig(moduleName);
        if (moduleConfig == null) throw new Exception("Undefined module "+moduleName);

        mapping.setModuleConfig(moduleConfig);
    }

    public void addConnectionConfig(ConnectionConfig connectionConfig) {
        connectionConfigs.put(connectionConfig.getName(), connectionConfig);
    }

    public void renameConnectionConfig(ConnectionConfig connectionConfig, String newName) {
        if (connectionConfig == null) return;
        if (connectionConfig.getName().equals(newName)) return;

        connectionConfigs.remove(connectionConfig.getName());
        connectionConfigs.put(newName, connectionConfig);
    }

    public void modifyConnectionConfig(String name, ConnectionConfig newConnectionConfig) {
        ConnectionConfig connectionConfig = connectionConfigs.get(name);
        connectionConfig.copy(newConnectionConfig);
    }

    public ConnectionConfig removeConnectionConfig(String connectionName) {
        return connectionConfigs.remove(connectionName);
    }

    public ConnectionConfig getConnectionConfig(String name) {
        return connectionConfigs.get(name);
    }

    public Collection<EntryMapping> getEntryMappings() {
        Collection<EntryMapping> list = new ArrayList<EntryMapping>();
        for (Collection<EntryMapping> c : entryMappingsByDn.values()) {
            list.addAll(c);
        }
        return list;
    }

    public Collection<EntryMapping> findEntryMappings(String targetDn) throws Exception {
        if (targetDn == null) return null;
        return findEntryMappings(new DN(targetDn));
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
            if (log.isDebugEnabled()) log.debug("Search parent mappings for \""+parentDn+"\"");
            Collection<EntryMapping> parentMappings = findEntryMappings(parentDn);

            // if no parent mappings found, the entry doesn't exist in this partition
            if (parentMappings == null || parentMappings.isEmpty()) {
            	if (log.isDebugEnabled()) log.debug("Entry mapping \""+parentDn+"\" not found");
                return null;
            }

            list = new ArrayList<EntryMapping>();

            // for each parent mapping found
            for (EntryMapping parentMapping : parentMappings) {
                if (log.isDebugEnabled()) log.debug("Found parent " + parentMapping.getDn());

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

            if (log.isDebugEnabled()) {
                log.debug("Checking DN pattern:");
                log.debug(" - " + dn);
                log.debug(" - " + entryMapping.getDn());
            }
            if (!dn.matches(entryMapping.getDn())) continue;

            if (log.isDebugEnabled()) log.debug("Found " + entryMapping.getDn());
            results.add(entryMapping);
        }

        return results;
    }

    public Collection<ConnectionConfig> getConnectionConfigs() {
        return connectionConfigs.values();
    }

    public void setConnectionConfigs(Map<String,ConnectionConfig> connectionConfigs) {
        this.connectionConfigs = connectionConfigs;
    }

    public Collection<Collection<ModuleMapping>> getModuleMappings() {
        return moduleMappings.values();
    }
    public Collection<EntryMapping> getRootEntryMappings() {
        return rootEntryMappings;
    }

    public Collection getSuffixes() {
        return suffixes;
    }
    
    public ModuleConfig removeModuleConfig(String moduleName) {
        return moduleConfigs.remove(moduleName);
    }

    public Collection<ModuleConfig> getModuleConfigs() {
        return moduleConfigs.values();
    }

    public Collection<ModuleMapping> removeModuleMapping(String moduleName) {
        return moduleMappings.remove(moduleName);
    }

    public void removeModuleMapping(ModuleMapping mapping) {
        if (mapping == null) return;
        if (mapping.getModuleName() == null) return;

        Collection<ModuleMapping> c = moduleMappings.get(mapping.getModuleName());
        if (c != null) c.remove(mapping);
    }

    public void setModuleConfigs(Map<String,ModuleConfig> moduleConfigs) {
        this.moduleConfigs = moduleConfigs;
    }

    public void setModuleMappings(Map<String,Collection<ModuleMapping>> moduleMappings) {
        this.moduleMappings = moduleMappings;
    }

    public void setRootEntryMappings(Collection<EntryMapping> rootEntryMappings) {
        this.rootEntryMappings = rootEntryMappings;
    }

    public Collection<FieldMapping> getSearchableFields(SourceMapping sourceMapping) {
        SourceConfig sourceConfig = sources.getSourceConfig(sourceMapping.getSourceName());

        Collection<FieldMapping> results = new ArrayList<FieldMapping>();
        for (FieldMapping fieldMapping : sourceMapping.getFieldMappings()) {
            FieldConfig fieldConfig = sourceConfig.getFieldConfig(fieldMapping.getName());
            if (fieldConfig == null) continue;
            if (!fieldConfig.isSearchable()) continue;
            results.add(fieldMapping);
        }

        return results;
    }

    public boolean isDynamic(EntryMapping entryMapping) {

        boolean dynamic = entryMapping.isDynamic();

        //log.debug("Mapping "+entryMapping.getDn()+" is "+(dynamic ? "dynamic" : "not dynamic"));
        if (dynamic) return true;

        EntryMapping parentMapping = getParent(entryMapping);
        if (parentMapping == null) return false;

        return isDynamic(parentMapping);
    }

    public PartitionConfig getPartitionConfig() {
        return partitionConfig;
    }

    public void setPartitionConfig(PartitionConfig partitionConfig) {
        this.partitionConfig = partitionConfig;
    }

    public String toString() {
        return partitionConfig.getName();
    }

    public Sources getSources() {
        return sources;
    }

    public void setSources(Sources sources) {
        this.sources = sources;
    }
}
