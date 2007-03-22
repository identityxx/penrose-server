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
import org.safehaus.penrose.entry.DN;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/**
 * @author Endi S. Dewata
 */
public class Partition {

    Logger log = LoggerFactory.getLogger(getClass());

    private PartitionConfig partitionConfig;

    private Map entryMappingsById = new LinkedHashMap();
    private Map entryMappingsByDn = new LinkedHashMap();
    private Map entryMappingsBySource = new LinkedHashMap();
    private Map entryMappingsByParentId = new LinkedHashMap();

    private Collection suffixes = new ArrayList();
    private Collection rootEntryMappings = new ArrayList();

    private Map connectionConfigs = new LinkedHashMap();
    private Map sourceConfigs = new LinkedHashMap();

    private Map moduleConfigs = new LinkedHashMap();
    private Map moduleMappings = new LinkedHashMap();

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
        for (Iterator i=rootEntryMappings.iterator(); i.hasNext(); ) {
            EntryMapping entryMapping = (EntryMapping)i.next();
            DN suffix = entryMapping.getDn();

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

    public Collection getEntryMappings(String dn) {
        return getEntryMappings(new DN(dn));
    }

    public Collection getEntryMappings(DN dn) {
        if (dn == null) return null;

        Collection list = (Collection)entryMappingsByDn.get(dn.getNormalizedDn());
        if (list == null) return null;

        return new ArrayList(list);
    }

    public Collection getEntryMappings(SourceConfig sourceConfig) {
        String sourceName = sourceConfig.getName();
        Collection list = (Collection)entryMappingsBySource.get(sourceName);
        if (list == null) return new ArrayList();
        return list;
    }

    public void addEntryMapping(EntryMapping entryMapping) throws Exception {

        String dn = entryMapping.getDn().getNormalizedDn();
        if (log.isDebugEnabled()) log.debug("Adding entry "+dn);

        String id = entryMapping.getId();
        if (id == null) {
            id = ""+ entryMappingsById.size();
            entryMapping.setId(id);
        }
        if (log.isDebugEnabled()) log.debug("ID: "+id);

        // lookup by id
        entryMappingsById.put(id, entryMapping);

        // lookup by dn
        Collection c = (Collection) entryMappingsByDn.get(dn);
        if (c == null) {
            c = new ArrayList();
            entryMappingsByDn.put(dn, c);
        }
        c.add(entryMapping);

        // lookup by source
        Collection sourceMappings = entryMapping.getSourceMappings();
        for (Iterator i = sourceMappings.iterator(); i.hasNext(); ) {
            SourceMapping sourceMapping = (SourceMapping)i.next();
            String sourceName = sourceMapping.getSourceName();
            c = (Collection)entryMappingsBySource.get(sourceName);
            if (c == null) {
                c = new ArrayList();
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
                    parent = (EntryMapping)c.iterator().next();
                    parentId = parent.getId();
                    entryMapping.setParentId(parentId);
                }
            }

        } else {
            parent = getEntryMappingById(parentId);
        }

        if (parent == null) {
        	if (log.isDebugEnabled()) log.debug("Suffix: "+dn);
            rootEntryMappings.add(entryMapping);
            suffixes.add(entryMapping.getDn());
        } else {
        	if (log.isDebugEnabled()) log.debug("Parent ID: "+parentId);
            addChildren(parent, entryMapping);
        }
    }

    public EntryMapping getEntryMappingById(String id) {
        return (EntryMapping) entryMappingsById.get(id);
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

        Collection c = (Collection)entryMappingsByDn.get(entryMapping.getDn().getNormalizedDn());
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

        Collection c = (Collection) entryMappingsByDn.get(oldDn.getNormalizedDn());
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
        Collection newList = (Collection) entryMappingsByDn.get(newDn.getNormalizedDn());
        if (newList == null) {
        	if (log.isDebugEnabled()) log.debug("First "+newDn);
            newList = new ArrayList();
            entryMappingsByDn.put(newDn.getNormalizedDn(), newList);
        }
        newList.add(entryMapping);

        EntryMapping newParent = getParent(entryMapping);
        if (log.isDebugEnabled()) log.debug("New parent "+(newParent == null ? null : newParent.getDn()));

        if (newParent != null) {
            addChildren(newParent, entryMapping);
        }

        Collection children = getChildren(entryMapping);

        if (children != null) {
            //addChildren(newDn, children);

            for (Iterator i=children.iterator(); i.hasNext(); ) {
                EntryMapping child = (EntryMapping)i.next();
                String childNewDn = child.getRdn()+","+newDn;
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
        Collection newList = (Collection) entryMappingsByDn.get(newDn.toLowerCase());
        if (newList == null) {
        	if (log.isDebugEnabled()) log.debug("First "+newDn);
            newList = new ArrayList();
            entryMappingsByDn.put(newDn.toLowerCase(), newList);
        }
        newList.add(entryMapping);

        Collection children = getChildren(entryMapping);

        if (children != null) {
            //addChildren(newDn, children);

            for (Iterator i=children.iterator(); i.hasNext(); ) {
                EntryMapping child = (EntryMapping)i.next();
                String childNewDn = child.getRdn()+","+newDn;
                //System.out.println(" - renaming child "+child.getDn()+" to "+childNewDn);

                renameChildren(child, childNewDn);
            }

            //removeChildren(oldDn);
        }
    }

    public EntryMapping getParent(EntryMapping entryMapping) {
        if (entryMapping == null) return null;

        return (EntryMapping)entryMappingsById.get(entryMapping.getParentId());
    }

    public Collection getChildren(EntryMapping parentMapping) {
        Collection children = (Collection) entryMappingsByParentId.get(parentMapping.getId());
        if (children == null) return new ArrayList();
        return children;
    }

    public void addChildren(EntryMapping parentMapping, Collection newChildren) {
        Collection children = (Collection)entryMappingsByParentId.get(parentMapping.getId());
        if (children == null) {
            children = new ArrayList();
            entryMappingsByParentId.put(parentMapping.getId(), children);
        }
        children.addAll(newChildren);
    }

    public void addChildren(EntryMapping parentMapping, EntryMapping entryMapping) {
    	if (log.isDebugEnabled()) log.debug("Adding "+entryMapping.getDn()+" under "+parentMapping.getDn());
        Collection children = (Collection) entryMappingsByParentId.get(parentMapping.getId());
        if (children == null) {
            children = new ArrayList();
            entryMappingsByParentId.put(parentMapping.getId(), children);
        }
        children.add(entryMapping);
    }

    public Collection removeChildren(EntryMapping parentMapping) {
        return (Collection) entryMappingsByParentId.remove(parentMapping.getId());
    }

   public Collection getEffectiveSourceMappings(EntryMapping entryMapping) {
        Collection list = new ArrayList();
        list.addAll(entryMapping.getSourceMappings());

        EntryMapping parent = getParent(entryMapping);
        if (parent != null) list.addAll(getEffectiveSourceMappings(parent));

        return list;
    }

    public SourceMapping getEffectiveSourceMapping(EntryMapping entryMapping, String name) {
        SourceMapping sourceMapping = (SourceMapping)entryMapping.getSourceMapping(name);
        if (sourceMapping != null) return sourceMapping;

        EntryMapping parent = getParent(entryMapping);
        if (parent != null) return getEffectiveSourceMapping(parent, name);

        return null;
    }

    public void addModuleConfig(ModuleConfig moduleConfig) throws Exception {
        moduleConfigs.put(moduleConfig.getName(), moduleConfig);
    }

    public ModuleConfig getModuleConfig(String name) {
        return (ModuleConfig)moduleConfigs.get(name);
    }

    public Collection getModuleMappings(String name) {
        return (Collection)moduleMappings.get(name);
    }

    public void addModuleMapping(ModuleMapping mapping) throws Exception {
        Collection c = (Collection)moduleMappings.get(mapping.getModuleName());
        if (c == null) {
            c = new ArrayList();
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
        ConnectionConfig connectionConfig = (ConnectionConfig)connectionConfigs.get(name);
        connectionConfig.copy(newConnectionConfig);
    }

    public ConnectionConfig removeConnectionConfig(String connectionName) {
        return (ConnectionConfig)connectionConfigs.remove(connectionName);
    }

    public ConnectionConfig getConnectionConfig(String name) {
        return (ConnectionConfig)connectionConfigs.get(name);
    }

    public void addSourceConfig(SourceConfig sourceConfig) {
    	if (log.isDebugEnabled()) log.debug("Adding source "+sourceConfig.getName());
        sourceConfigs.put(sourceConfig.getName(), sourceConfig);
    }

    public SourceConfig removeSourceConfig(String name) {
        return (SourceConfig)sourceConfigs.remove(name);
    }

    public SourceConfig getSourceConfig(String name) {
        return (SourceConfig)sourceConfigs.get(name);
    }

    public SourceConfig getSourceConfig(SourceMapping sourceMapping) {
        return getSourceConfig(sourceMapping.getSourceName());
    }
    
    public Collection getSourceConfigs() {
        return sourceConfigs.values();
    }

    public void renameSourceConfig(SourceConfig sourceConfig, String newName) {
        if (sourceConfig == null) return;
        if (sourceConfig.getName().equals(newName)) return;

        sourceConfigs.remove(sourceConfig.getName());
        sourceConfigs.put(newName, sourceConfig);
    }

    public void modifySourceConfig(String name, SourceConfig newSourceConfig) {
        SourceConfig sourceConfig = (SourceConfig)sourceConfigs.get(name);
        sourceConfig.copy(newSourceConfig);
    }

    public Collection getEntryMappings() {
        Collection list = new ArrayList();
        for (Iterator i=entryMappingsByDn.values().iterator(); i.hasNext(); ) {
            Collection c = (Collection)i.next();
            list.addAll(c);
        }
        return list;
    }

    public Collection findEntryMappings(String targetDn) throws Exception {
        if (targetDn == null) return null;
        return findEntryMappings(new DN(targetDn));
    }

    public Collection findEntryMappings(DN dn) throws Exception {
        if (dn == null) return new ArrayList();
        //log.debug("Finding entry mappings \""+dn+"\" in partition "+getName());

        // search for static mappings
        Collection results = (Collection) entryMappingsByDn.get(dn.getNormalizedDn());
        if (results != null) {
            //log.debug("Found "+results.size()+" mapping(s).");
            return results;
        }

        // can't find exact match -> search for parent mappings

        DN parentDn = dn.getParentDn();

        results = new ArrayList();
        Collection list;

        // if dn has no parent, check against root entries
        if (parentDn.isEmpty()) {
            //log.debug("Check root mappings");
            list = rootEntryMappings;

        } else {
            if (log.isDebugEnabled()) log.debug("Search parent mappings for \""+parentDn+"\"");
            Collection parentMappings = findEntryMappings(parentDn);

            // if no parent mappings found, the entry doesn't exist in this partition
            if (parentMappings == null || parentMappings.isEmpty()) {
            	if (log.isDebugEnabled()) log.debug("Entry mapping \""+parentDn+"\" not found");
                return null;
            }

            list = new ArrayList();

            // for each parent mapping found
            for (Iterator i=parentMappings.iterator(); i.hasNext(); ) {
                EntryMapping parentMapping = (EntryMapping)i.next();
                if (log.isDebugEnabled()) log.debug("Found parent "+parentMapping.getDn());

                String handlerName = parentMapping.getHandlerName();
                if ("PROXY".equals(handlerName)) { // if parent is proxy, include it in results
                    results.add(parentMapping);

                } else { // otherwise check for matching siblings
                    Collection children = getChildren(parentMapping);
                    list.addAll(children);
                }
            }
        }

        // check against each mapping in the list
        for (Iterator iterator = list.iterator(); iterator.hasNext(); ) {
            EntryMapping entryMapping = (EntryMapping) iterator.next();

            if (log.isDebugEnabled()) 
        	{
            	log.debug("Checking DN pattern:");
	            log.debug(" - "+dn);
	            log.debug(" - "+entryMapping.getDn());
        	}
            if (!dn.matches(entryMapping.getDn())) continue;

            if (log.isDebugEnabled()) log.debug("Found "+entryMapping.getDn());
            results.add(entryMapping);
        }

        return results;
    }

    public Collection getConnectionConfigs() {
        return connectionConfigs.values();
    }

    public void setConnectionConfigs(Map connectionConfigs) {
        this.connectionConfigs = connectionConfigs;
    }

    public Collection getModuleMappings() {
        return moduleMappings.values();
    }
    public Collection getRootEntryMappings() {
        return rootEntryMappings;
    }

    public Collection getSuffixes() {
        return suffixes;
    }
    
    public ModuleConfig removeModuleConfig(String moduleName) {
        return (ModuleConfig)moduleConfigs.remove(moduleName);
    }

    public Collection getModuleConfigs() {
        return moduleConfigs.values();
    }

    public Collection removeModuleMapping(String moduleName) {
        return (Collection)moduleMappings.remove(moduleName);
    }

    public void removeModuleMapping(ModuleMapping mapping) {
        if (mapping == null) return;
        if (mapping.getModuleName() == null) return;

        Collection c = (Collection)moduleMappings.get(mapping.getModuleName());
        if (c != null) c.remove(mapping);
    }

    public void setModuleConfigs(Map moduleConfigs) {
        this.moduleConfigs = moduleConfigs;
    }

    public void setModuleMappings(Map moduleMappings) {
        this.moduleMappings = moduleMappings;
    }

    public void setRootEntryMappings(Collection rootEntryMappings) {
        this.rootEntryMappings = rootEntryMappings;
    }

    public Collection getSearchableFields(SourceMapping sourceMapping) {
        SourceConfig sourceConfig = getSourceConfig(sourceMapping.getSourceName());

        Collection results = new ArrayList();
        for (Iterator i=sourceMapping.getFieldMappings().iterator(); i.hasNext(); ) {
            FieldMapping fieldMapping = (FieldMapping)i.next();
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
}
