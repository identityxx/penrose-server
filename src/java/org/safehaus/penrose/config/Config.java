/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.config;

import java.io.Serializable;
import java.util.*;

import org.apache.log4j.Logger;
import org.safehaus.penrose.module.ModuleConfig;
import org.safehaus.penrose.module.ModuleMapping;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.cache.CacheConfig;
import org.safehaus.penrose.engine.EngineConfig;
import org.safehaus.penrose.interpreter.InterpreterConfig;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.connection.*;


/**
 * @author Endi S. Dewata
 * @author Adison Wongkar 
 */
public class Config implements Serializable {

    public Logger log = Logger.getLogger(Penrose.CONFIG_LOGGER);

    private int debug = 0;

    private Map entryDefinitions = new LinkedHashMap();
    private Collection rootEntryDefinitions = new ArrayList();

    private Collection schemaFiles = new ArrayList();

    private String rootDn;
    private String rootPassword;
	
    private Map interpreterConfigs = new LinkedHashMap();
    private Map cacheConfigs = new LinkedHashMap();
    private Map engineConfigs = new LinkedHashMap();
    private Map adapterConfigs = new LinkedHashMap();
    private Map connectionConfigs = new LinkedHashMap();
    private Map sourceConfigs = new LinkedHashMap();

    private Map moduleConfigs = new LinkedHashMap();
    private Map moduleMappings = new LinkedHashMap();

    public Config() {
    }

	public void setDebug(int debug) {
		this.debug = debug;
	}

	public void addEntryDefinition(EntryDefinition entry) throws Exception {

        String dn = entry.getDn();

        if (entryDefinitions.get(dn) != null) throw new Exception("Entry already exists");

        int i = dn.indexOf(",");

        if (i >= 0) { // entry has parent

            String parentDn = dn.substring(i+1);
            EntryDefinition parent = (EntryDefinition)entryDefinitions.get(parentDn);

            if (parent != null) { // parent found
                parent.addChild(entry);
            }
        }

        entryDefinitions.put(dn, entry);

        if (entry.getParent() == null) {
        	log.debug("Adding "+dn+" to rootEntryDefinitions");
        	rootEntryDefinitions.add(entry);
        }
	}

    public void removeEntry(EntryDefinition entry) {
        if (entry.getParent() == null) {
            rootEntryDefinitions.remove(entry);
        } else {
            Collection siblings = entry.getParent().getChildren();
            siblings.remove(entry);
        }
        entryDefinitions.remove(entry.getDn());
    }
    
    public void renameEntry(EntryDefinition entry, String newDn) {
    	log.debug("renameEntry: "+entry.getDn()+" to "+newDn);
    	if (entry == null) return;
    	if (newDn.equals(entry.getDn())) return;
    	log.debug("renameEntry: "+entry.getDn()+" to "+newDn);
    	String oldDn = entry.getDn();
    	entry.setDn(newDn);
        entryDefinitions.remove(oldDn);
        entryDefinitions.put(newDn, entry);
        Collection children = entry.getChildren();
        if (children != null) {
        	Object[] cs = children.toArray();
        	for (int i=0; i<cs.length; i++) {
        		EntryDefinition child = (EntryDefinition) cs[i];
                String childNewDn = child.getRdn()+","+newDn;
                renameEntry(child, childNewDn);
        	}
        }
		entryDefinitions.remove(oldDn);
    }

    public void addSourceDefinition(SourceDefinition sourceConfig) throws Exception {
        sourceConfigs.put(sourceConfig.getName(), sourceConfig);
    }
    
    public void removeSourceConfig(String sourceName) {
    	if (this.sourceConfigs != null) {
    		this.sourceConfigs.remove(sourceName);
    	}
    }
    
    public void addModuleConfig(ModuleConfig moduleConfig) throws Exception {
        moduleConfigs.put(moduleConfig.getModuleName(), moduleConfig);
    }

    public void addModuleMapping(ModuleMapping mapping) {
        moduleMappings.put(mapping.getModuleName(), mapping);
    }

	/**
	 * Add connection object to this configuration
	 * 
	 * @param connection
	 */
	public void addConnectionConfig(ConnectionConfig connection) {
		connectionConfigs.put(connection.getConnectionName(), connection);
	}
	
	public void removeConnection(String connectionName) {
		connectionConfigs.remove(connectionName);
	}

    public ConnectionConfig getConnectionConfig(String name) {
        return (ConnectionConfig)connectionConfigs.get(name);
    }

    public void addAdapterConfig(AdapterConfig adapter) {
        adapterConfigs.put(adapter.getAdapterName(), adapter);
    }

	/**
	 * @return Returns the root.
	 */
	public Collection getEntryDefinitions() {
		return entryDefinitions.values();
	}

	/**
	 * @param root
	 *            The root to set.
	 */
	public void setEntryDefinitions(Map root) {
		this.entryDefinitions = root;
	}

    public EntryDefinition getEntryDefinition(String dn) {
        return (EntryDefinition)entryDefinitions.get(dn);
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
	/**
	 * @return Returns the rootDn.
	 */
	public String getRootDn() {
		return rootDn;
	}
	/**
	 * @param rootDn
	 *            The rootDn to set.
	 */
	public void setRootDn(String rootDn) {
		this.rootDn = rootDn;
	}
	/**
	 * @return Returns the rootPassword.
	 */
	public String getRootPassword() {
		return rootPassword;
	}
	/**
	 * @param rootPassword
	 *            The rootPassword to set.
	 */
	public void setRootPassword(String rootPassword) {
		this.rootPassword = rootPassword;
	}

	public String toString() {

		String nl = System.getProperty("line.separator");
		StringBuffer sb = new StringBuffer();
		
        sb.append(nl);
        sb.append(nl);
        sb.append("CACHE:");
        sb.append(cacheConfigs);
        sb.append(nl);
        sb.append(nl);

        sb.append("CONNECTIONS:");
        sb.append(nl);
        sb.append(nl);

		for (Iterator i = connectionConfigs.keySet().iterator(); i.hasNext();) {
			String connectionName = (String) i.next();
			ConnectionConfig connection = (ConnectionConfig) connectionConfigs.get(connectionName);
			sb.append(connectionName + " (" + connection.getAdapterName() + ")" + nl);
			sb.append("Parameters:" + nl);
			for (Iterator j = connection.getParameterNames().iterator(); j.hasNext();) {
				String name = (String) j.next();
				String value = connection.getParameter(name);
				sb.append("- " + name + ": " + value + nl);
			}
			sb.append(nl);
		}

        sb.append("SOURCES:");
        sb.append(nl);
        sb.append(nl);

        for (Iterator i = sourceConfigs.values().iterator(); i.hasNext();) {
            SourceDefinition sourceConfig = (SourceDefinition)i.next();
            sb.append(sourceConfig.getName()+" ("+sourceConfig.getConnectionName() + ")" + nl);
            for (Iterator j = sourceConfig.getFields().iterator(); j.hasNext(); ) {
                FieldDefinition field = (FieldDefinition)j.next();
                sb.append("- field: "+field.getName()+" "+(field.isPrimaryKey() ? "(primary key)" : "") + nl);
            }
            for (Iterator j = sourceConfig.getParameterNames().iterator(); j.hasNext(); ) {
                String name = (String)j.next();
                sb.append("- "+name+": "+sourceConfig.getParameter(name) + nl);
            }
            sb.append(nl);
        }

		sb.append("ROOT ENTRIES:");
        sb.append(nl);
        sb.append(nl);

		for (Iterator i = rootEntryDefinitions.iterator(); i.hasNext();) {
			EntryDefinition entry = (EntryDefinition)i.next();
            String dn = entry.getDn();
			//if (entry.getParent() != null) continue;
			sb.append("DN: "+dn + nl);
			sb.append(toString(entry));
		}

		sb.append("ENTRIES:");
        sb.append(nl);
        sb.append(nl);

		for (Iterator i = entryDefinitions.keySet().iterator(); i.hasNext();) {
			String dn = (String) i.next();
			EntryDefinition entry = (EntryDefinition) entryDefinitions.get(dn);
			//if (entry.getParent() != null) continue;
			sb.append("DN: "+dn + nl);
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
            ModuleMapping moduleMapping = (ModuleMapping)i.next();
            sb.append(moduleMapping.getModuleName()+" -> "+ moduleMapping.getBaseDn() + nl);
        }

		return sb.toString();
	}

	public String toString(EntryDefinition entry) {
		
		String nl = System.getProperty("line.separator");
		StringBuffer sb = new StringBuffer("dn: " + entry.getDn() + nl);
		
		sb.append("parentDn: "+entry.getParentDn() + nl);
		sb.append("parent: "+(entry.getParent()==null?"null":"not null") + nl);

		Collection oc = entry.getObjectClasses();
		for (Iterator i = oc.iterator(); i.hasNext(); ) {
			String value = (String) i.next();
			sb.append("objectClass: " + value + nl);
		}

		Map attributes = entry.getAttributes();
		for (Iterator i = attributes.values().iterator(); i.hasNext(); ) {
			AttributeDefinition attribute = (AttributeDefinition) i.next();
			if (attribute.getName().equals("objectClass"))
				continue;

			sb.append(attribute.getName() + ": "
					+ attribute.getExpression() + nl);
		}

        sb.append(nl);

		Collection children = entry.getChildren();
		for (Iterator i = children.iterator(); i.hasNext();) {
			EntryDefinition child = (EntryDefinition) i.next();
			sb.append(toString(child));
		}

		return sb.toString();
	}

    public Collection getAdapterConfigs() {
        return adapterConfigs.values();
    }

    public AdapterConfig getAdapterConfig(String name) {
        return (AdapterConfig)adapterConfigs.get(name);
    }

    public Collection getSchemaFiles() {
        return schemaFiles;
    }

    public void setSchemaFiles(List schemaFiles) {
        this.schemaFiles = schemaFiles;
    }
    
    public void addCacheConfig(CacheConfig cacheConfig) {
    	cacheConfigs.put(cacheConfig.getCacheName(), cacheConfig);
    }
    
    public Collection getCacheConfigs() {
    	return cacheConfigs.values();
    }

	public Collection getModuleMappings() {
		return moduleMappings.values();
	}
	public Collection getRootEntryDefinitions() {
		return rootEntryDefinitions;
	}

    public Collection getSourceConfigs() {
		return sourceConfigs.values();
	}

    public SourceDefinition getSourceConfig(Source source) {
        return (SourceDefinition)sourceConfigs.get(source.getName());
    }

    public void removeModuleConfig(String moduleName) {
    	moduleConfigs.remove(moduleName);
    }
    
    public Collection getModuleConfigs() {
    	return moduleConfigs.values();
    }
    
    public void removeModuleMapping(String moduleName) {
    	moduleMappings.remove(moduleName);
    }

    public Collection getEngineConfigs() {
        return engineConfigs.values();
    }

    public void addEngineConfig(EngineConfig engineConfig) {
        engineConfigs.put(engineConfig.getEngineName(), engineConfig);
    }

    public void addInterpreterConfig(InterpreterConfig interpreterConfig) {
        interpreterConfigs.put(interpreterConfig.getInterpreterName(), interpreterConfig);
    }

    public Collection getInterpreterConfigs() {
        return interpreterConfigs.values();
    }

    public InterpreterConfig getInterpreterConfig(String name) {
        return (InterpreterConfig)interpreterConfigs.get(name);
    }

    public void setModuleConfigs(Map moduleConfigs) {
        this.moduleConfigs = moduleConfigs;
    }

    public void setModuleMappings(Map moduleMappings) {
        this.moduleMappings = moduleMappings;
    }

    public void setSourceConfigs(Map sourceConfigs) {
        this.sourceConfigs = sourceConfigs;
    }

    public void setAdapterConfigs(Map adapterConfigs) {
        this.adapterConfigs = adapterConfigs;
    }

    public void setCacheConfigs(Map cacheConfigs) {
        this.cacheConfigs = cacheConfigs;
    }

    public void setInterpreterConfigs(Map interpreterConfigs) {
        this.interpreterConfigs = interpreterConfigs;
    }

    public int getDebug() {
        return debug;
    }

    public void setRootEntryDefinitions(Collection rootEntryDefinitions) {
        this.rootEntryDefinitions = rootEntryDefinitions;
    }

}
