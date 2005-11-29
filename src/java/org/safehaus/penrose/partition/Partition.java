/**
 * Copyright (c) 2000-2005, Identyx Corporation.
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

import org.apache.log4j.Logger;
import org.safehaus.penrose.module.ModuleMapping;
import org.safehaus.penrose.module.ModuleConfig;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.connector.ConnectionConfig;

public class Partition {

    Logger log = Logger.getLogger(getClass());

    private Map entryMappings = new TreeMap();
    private Collection rootEntryMappings = new ArrayList();
    private Map childrenMap = new TreeMap();

    private Map connectionConfigs = new LinkedHashMap();

    private Map moduleConfigs = new LinkedHashMap();
    private Map moduleMappings = new LinkedHashMap();

    public Partition() {
    }

	public void addEntryMapping(EntryMapping entry) throws Exception {

        String dn = entry.getDn();

        if (entryMappings.get(dn) != null) throw new Exception("Entry "+dn+" already exists.");

        //System.out.println("Adding "+dn+".");

        EntryMapping parent = getParent(entry);

        if (parent != null) { // parent found
            //System.out.println("Found parent "+parentDn+".");

            Collection children = getChildren(parent);
            if (children == null) {
                children = new ArrayList();
                setChildren(parent, children);
            }
            children.add(entry);
        }

        entryMappings.put(dn, entry);

        if (parent == null) {
        	rootEntryMappings.add(entry);
        }
/*
        for (Iterator j=entry.getSourceMappings().iterator(); j.hasNext(); ) {
            Source source = (Source)j.next();

            String sourceName = source.getSourceName();
            String connectionName = source.getConnectionName();

            ConnectionConfig connection = getConnectionConfig(connectionName);
            if (connection == null) throw new Exception("Connection "+connectionName+" undefined.");

            SourceDefinition sourceDefinition = connection.getSourceDefinition(sourceName);
            if (sourceDefinition == null) throw new Exception("Source "+sourceName+" undefined.");

            Collection fieldConfigs = sourceDefinition.getFieldDefinitions();

            for (Iterator k=fieldConfigs.iterator(); k.hasNext(); ) {
                FieldDefinition fieldConfig = (FieldDefinition)k.next();
                String fieldName = fieldConfig.getName();

                // define any missing fields
                Field field = (Field)source.getFieldMapping(fieldName);
                if (field != null) continue;

                field = new Field();
                field.setName(fieldName);
                source.addFieldMapping(field);
            }
        }
*/
    }

    public void modifyEntryMapping(String dn, EntryMapping newEntry) {
        EntryMapping entry = getEntryMapping(dn);
        entry.copy(newEntry);
    }

    public EntryMapping removeEntryMapping(EntryMapping entry) {
        EntryMapping parent = getParent(entry);
        if (parent == null) {
            rootEntryMappings.remove(entry);

        } else {
            Collection children = getChildren(parent);
            if (children != null) children.remove(entry);
        }

        return (EntryMapping)entryMappings.remove(entry.getDn());
    }

    public void renameEntryMapping(EntryMapping entry, String newDn) {
    	if (entry == null) return;
    	if (entry.getDn().equals(newDn)) return;

        EntryMapping oldParent = getParent(entry);
    	String oldDn = entry.getDn();

    	entry.setDn(newDn);
        entryMappings.put(newDn, entry);

        EntryMapping newParent = getParent(entry);

        if (newParent != null) {
            Collection newSiblings = getChildren(newParent);
            if (newSiblings == null) {
                newSiblings = new ArrayList();
                setChildren(newParent, newSiblings);
            }
            newSiblings.add(entry);
        }

        Collection children = getChildren(oldDn);

        if (children != null) {
            setChildren(newDn, children);

            for (Iterator i=children.iterator(); i.hasNext(); ) {
                EntryMapping child = (EntryMapping)i.next();
                String childNewDn = child.getRdn()+","+newDn;
                //System.out.println(" - renaming child "+child.getDn()+" to "+childNewDn);

                renameChildren(child, childNewDn);
            }

            removeChildren(oldDn);
        }

        entryMappings.remove(oldDn);

        if (oldParent != null) {
            Collection oldSiblings = getChildren(oldParent);
            if (oldSiblings != null) oldSiblings.remove(entry);
        }

    }

    public void renameChildren(EntryMapping entry, String newDn) {
    	if (entry == null) return;

    	if (newDn.equals(entry.getDn())) return;

        String oldDn = entry.getDn();

        entry.setDn(newDn);
        entryMappings.put(newDn, entry);

        Collection children = getChildren(oldDn);

        if (children != null) {
            setChildren(newDn, children);

            for (Iterator i=children.iterator(); i.hasNext(); ) {
                EntryMapping child = (EntryMapping)i.next();
                String childNewDn = child.getRdn()+","+newDn;
                //System.out.println(" - renaming child "+child.getDn()+" to "+childNewDn);

                renameChildren(child, childNewDn);
            }

            removeChildren(oldDn);
        }

        entryMappings.remove(oldDn);
    }

    public EntryMapping getParent(EntryMapping entryMapping) {
        if (entryMapping == null) return null;
        String parentDn = entryMapping.getParentDn();
        return getEntryMapping(parentDn);
    }

    public Collection getChildren(EntryMapping entryMapping) {
        return getChildren(entryMapping.getDn());
    }

    public Collection getChildren(String dn) {
        return (Collection)childrenMap.get(dn);
    }

    public void setChildren(EntryMapping entryMapping, Collection children) {
        setChildren(entryMapping.getDn(), children);
    }

    public void setChildren(String dn, Collection children) {
        childrenMap.put(dn, children);
    }

    public Collection removeChildren(EntryMapping entry) {
        return removeChildren(entry.getDn());
    }

    public Collection removeChildren(String dn) {
        return (Collection)childrenMap.remove(dn);
    }

    public Collection getEffectiveSources(EntryMapping entry) {
        Collection list = new ArrayList();
        list.addAll(entry.getSourceMappings());

        EntryMapping parent = getParent(entry);
        if (parent != null) list.addAll(getEffectiveSources(parent));

        return list;
    }

    public SourceMapping getEffectiveSource(EntryMapping entryMapping, String name) {
        SourceMapping sourceMapping = (SourceMapping)entryMapping.getSourceMapping(name);
        if (sourceMapping != null) return sourceMapping;

        EntryMapping parent = getParent(entryMapping);
        if (parent != null) return getEffectiveSource(parent, name);

        return null;
    }

    public Collection getEffectiveRelationships(EntryMapping entryMapping) {
        Collection relationships = new ArrayList();
        relationships.addAll(entryMapping.getRelationships());

        EntryMapping parent = getParent(entryMapping);
        if (parent != null) relationships.addAll(getEffectiveRelationships(parent));

        return relationships;
    }

    public void addModuleConfig(ModuleConfig moduleConfig) throws Exception {
        moduleConfigs.put(moduleConfig.getModuleName(), moduleConfig);
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

	/**
	 * Add connection object to this configuration
	 * 
	 * @param connectionConfig
	 */
	public void addConnectionConfig(ConnectionConfig connectionConfig) throws Exception {
		connectionConfigs.put(connectionConfig.getConnectionName(), connectionConfig);
	}
	
	public ConnectionConfig removeConnectionConfig(String connectionName) {
		return (ConnectionConfig)connectionConfigs.remove(connectionName);
	}

    public ConnectionConfig getConnectionConfig(String name) {
        return (ConnectionConfig)connectionConfigs.get(name);
    }

	/**
	 * @return Returns the root.
	 */
	public Collection getEntryMappings() {
		return entryMappings.values();
	}

	/**
	 * @param root
	 *            The root to set.
	 */
	public void setEntryMappings(Map root) {
		this.entryMappings = root;
	}

    public EntryMapping getEntryMapping(String dn) {
        if (dn == null) return null;
        return (EntryMapping)entryMappings.get(dn);
    }

    public EntryMapping findEntryMapping(String dn) throws Exception {

        EntryMapping result = null;

        try {
            log.debug("Finding "+dn);

            if (dn == null || "".equals(dn)) {
                return result;
            }
            
            EntryMapping entryMapping = (EntryMapping)this.entryMappings.get(dn);
            if (entryMapping != null) {
                result = entryMapping;
                return result;
            }

            Row rdn = Entry.getRdn(dn);
            String parentDn = Entry.getParentDn(dn);

            Collection list;

            if (parentDn == null) {
                list = rootEntryMappings;

            } else {
                EntryMapping parentMapping = findEntryMapping(parentDn);
                if (parentMapping == null) {
                    return result;
                }

                list = getChildren(parentMapping);
                if (list == null) return result;
            }

            for (Iterator iterator = list.iterator(); iterator.hasNext(); ) {
                EntryMapping childMapping = (EntryMapping) iterator.next();
                Row childRdn = Entry.getRdn(childMapping.getRdn());

                if (!rdn.getNames().equals(childRdn.getNames())) continue;

                if (childMapping.isRdnDynamic()) {
                    result = childMapping;
                    return result;
                }

                if (!rdn.equals(childRdn)) continue;

                return childMapping;
            }

            return result;

        } finally {
            //log.debug("result: "+result);
        }
    }

	/**
	 * @return Returns the connections.
	 */
	public Collection getConnectionConfigs() {
		return connectionConfigs.values();
	}
	/**
	 * @param connectionConfigs
	 *            The connections to set.
	 */
	public void setConnectionConfigs(Map connectionConfigs) {
		this.connectionConfigs = connectionConfigs;
	}

	public String toString() {

		String nl = System.getProperty("line.separator");
		StringBuffer sb = new StringBuffer();
		
        sb.append(nl);
        sb.append(nl);

        sb.append("CONNECTIONS:");
        sb.append(nl);
        sb.append(nl);

		for (Iterator i = connectionConfigs.keySet().iterator(); i.hasNext();) {
			String connectionName = (String) i.next();
			ConnectionConfig connection = (ConnectionConfig) connectionConfigs.get(connectionName);
			sb.append(connectionName + " (" + connection.getConnectionType() + ")" + nl);
			sb.append("Parameters:" + nl);
			for (Iterator j = connection.getParameterNames().iterator(); j.hasNext();) {
				String name = (String) j.next();
				String value = connection.getParameter(name);
				sb.append("- " + name + ": " + value + nl);
			}
			sb.append(nl);

            for (Iterator j = connection.getSourceDefinitions().iterator(); j.hasNext();) {
                SourceDefinition sourceConfig = (SourceDefinition)j.next();
                sb.append("Source "+sourceConfig.getName()+nl);
                for (Iterator k = sourceConfig.getFieldDefinitions().iterator(); k.hasNext(); ) {
                    FieldDefinition field = (FieldDefinition)k.next();
                    sb.append("- field: "+field.getName()+" "+(field.isPrimaryKey() ? "(primary key)" : "") + nl);
                }
                for (Iterator k = sourceConfig.getParameterNames().iterator(); k.hasNext(); ) {
                    String name = (String)k.next();
                    sb.append("- "+name+": "+sourceConfig.getParameter(name) + nl);
                }
                sb.append(nl);
            }
        }

		sb.append("ENTRIES:");
        sb.append(nl);
        sb.append(nl);

		for (Iterator i = rootEntryMappings.iterator(); i.hasNext();) {
			EntryMapping entry = (EntryMapping)i.next();
			sb.append(toString(entry));
		}

        sb.append("MODULES:");
        sb.append(nl);
        sb.append(nl);
        for (Iterator i = moduleConfigs.values().iterator(); i.hasNext(); ) {
            ModuleConfig moduleConfig = (ModuleConfig)i.next();
            sb.append(moduleConfig.getModuleName()+" ("+moduleConfig.getModuleClass() + ")" + nl);
            for (Iterator j = moduleConfig.getParameterNames().iterator(); j.hasNext(); ) {
                String name = (String)j.next();
                sb.append("- "+name+": "+moduleConfig.getParameter(name) + nl);
            }
            sb.append(nl);
        }

        sb.append("MODULE MAPPINGS:");
        sb.append(nl);
        sb.append(nl);
        for (Iterator i = moduleMappings.values().iterator(); i.hasNext(); ) {
            Collection c = (Collection)i.next();
            for (Iterator j = c.iterator(); j.hasNext(); ) {
                ModuleMapping moduleMapping = (ModuleMapping)j.next();
                sb.append(moduleMapping.getModuleName()+" -> "+ moduleMapping.getBaseDn() + nl);
            }
        }

		return sb.toString();
	}

	public String toString(EntryMapping entry) {
		
		String nl = System.getProperty("line.separator");
		StringBuffer sb = new StringBuffer("dn: " + entry.getDn() + nl);
		Collection oc = entry.getObjectClasses();
		for (Iterator i = oc.iterator(); i.hasNext(); ) {
			String value = (String) i.next();
			sb.append("objectClass: " + value + nl);
		}

		Collection attributes = entry.getAttributeMappings();
		for (Iterator i = attributes.iterator(); i.hasNext(); ) {
			AttributeMapping attribute = (AttributeMapping) i.next();
			if (attribute.getName().equals("objectClass"))
				continue;

			sb.append(attribute.getName() + ": "
					+ attribute.getExpression() + nl);
		}

		Collection childDefinitions = entry.getChildMappings();
		for (Iterator i = childDefinitions.iterator(); i.hasNext();) {
			MappingRule child = (MappingRule) i.next();
			sb.append("=> "+child.getFile() + nl);
		}

        sb.append(nl);

        Collection children = getChildren(entry);
        if (children != null) {
            for (Iterator i = children.iterator(); i.hasNext();) {
                EntryMapping child = (EntryMapping) i.next();
                sb.append(toString(child));
            }
        }

		return sb.toString();
	}

	public Collection getModuleMappings() {
		return moduleMappings.values();
	}
	public Collection getRootEntryMappings() {
		return rootEntryMappings;
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
        ConnectionConfig connectionConfig = getConnectionConfig(sourceMapping.getConnectionName());
        SourceDefinition sourceDefinition = connectionConfig.getSourceDefinition(sourceMapping.getSourceName());

        Collection results = new ArrayList();
        for (Iterator i=sourceMapping.getFieldMappings().iterator(); i.hasNext(); ) {
            FieldMapping fieldMapping = (FieldMapping)i.next();
            FieldDefinition fieldDefinition = sourceDefinition.getFieldDefinition(fieldMapping.getName());
            if (!fieldDefinition.isSearchable()) continue;
            results.add(fieldMapping);
        }

        return results;
    }

    public boolean isDynamic(EntryMapping entryMapping) {
        if (!entryMapping.isDynamic()) return false;

        EntryMapping parentMapping = getParent(entryMapping);
        if (parentMapping == null) return true;

        return isDynamic(parentMapping);
    }
}
