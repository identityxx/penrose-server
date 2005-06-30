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
import org.safehaus.penrose.module.GenericModuleMapping;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.graph.Graph;
import org.safehaus.penrose.cache.CacheConfig;
import org.safehaus.penrose.engine.EngineConfig;
import org.safehaus.penrose.graph.Graph;
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

    private Map entryDefinitions = new TreeMap();
    private Collection rootEntryDefinitions = new ArrayList();

    private Map graphs = new HashMap();
    private Map primarySources = new HashMap();

    private Collection schemaFiles = new ArrayList();

    private String rootDn;
    private String rootPassword;
	
    private Map interpreterConfigs = new LinkedHashMap();
    private Map cacheConfigs = new LinkedHashMap();
    private Map engineConfigs = new LinkedHashMap();
    private Map adapterConfigs = new LinkedHashMap();
    private Map connectionConfigs = new LinkedHashMap();
    private Map sourceDefinitions = new LinkedHashMap();

    private Map moduleConfigs = new LinkedHashMap();
    private Map moduleMappings = new LinkedHashMap();

    public Config() {
    }

	public void setDebug(int debug) {
		this.debug = debug;
	}

	public void addEntryDefinition(EntryDefinition entry) throws Exception {

        String dn = entry.getDn();

        if (entryDefinitions.get(dn) != null) throw new Exception("Entry "+dn+" already exists.");

        log.debug("Adding "+dn+".");

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
        	rootEntryDefinitions.add(entry);
        }

        // connecting all references to source and field definitions
        for (Iterator j=entry.getSources().iterator(); j.hasNext(); ) {
            Source source = (Source)j.next();

            SourceDefinition sourceConfig = getSourceDefinition(source);
            if (sourceConfig == null) throw new Exception("Source "+source.getName()+" undefined.");

            source.setSourceDefinition(sourceConfig);

            Collection fieldConfigs = sourceConfig.getFields();

            for (Iterator k=fieldConfigs.iterator(); k.hasNext(); ) {
                FieldDefinition fieldConfig = (FieldDefinition)k.next();
                String fieldName = fieldConfig.getName();

                // define any missing fields
                Field field = (Field)source.getField(fieldName);
                if (field == null) {
                    field = new Field();
                    field.setName(fieldName);
                    source.addField(field);
                }

                field.setFieldDefinition(fieldConfig);
                if (fieldConfig.isPrimaryKey()) source.addPrimaryKeyField(field);
            }
        }
    }

    public void analyze() throws Exception {

        for (Iterator i=rootEntryDefinitions.iterator(); i.hasNext(); ) {
            EntryDefinition entryDefinition = (EntryDefinition)i.next();
            analyze(entryDefinition);
        }
    }

    public void analyze(EntryDefinition entryDefinition) throws Exception {

        log.debug("Entry "+entryDefinition.getDn()+":");

        Source source = computePrimarySource(entryDefinition);
        primarySources.put(entryDefinition, source);
        log.debug(" - primary source: "+source);

        Graph graph = computeGraph(entryDefinition);
        graphs.put(entryDefinition, graph);
        log.debug(" - graph: "+graph);

        for (Iterator i=entryDefinition.getChildren().iterator(); i.hasNext(); ) {
            EntryDefinition childDefinition = (EntryDefinition)i.next();
            analyze(childDefinition);
        }
	}

    public Source getPrimarySource(EntryDefinition entryDefinition) {
        return (Source)primarySources.get(entryDefinition);
    }

    Source computePrimarySource(EntryDefinition entryDefinition) {

        Collection rdnAttributes = entryDefinition.getRdnAttributes();

        // TODO need to handle multiple rdn attributes
        AttributeDefinition rdnAttribute = (AttributeDefinition)rdnAttributes.iterator().next();
        String exp = rdnAttribute.getExpression();

        // TODO need to handle complex expression
        int index = exp.indexOf(".");
        if (index < 0) return null;

        String primarySourceName = exp.substring(0, index);

        for (Iterator i = entryDefinition.getSources().iterator(); i.hasNext();) {
            Source source = (Source) i.next();
            if (source.getName().equals(primarySourceName)) return source;
        }

        return null;
    }

    public Graph getGraph(EntryDefinition entryDefinition) throws Exception {

        return (Graph)graphs.get(entryDefinition);
    }

    Graph computeGraph(EntryDefinition entryDefinition) throws Exception {

        Graph graph = new Graph();

        Collection sources = entryDefinition.getEffectiveSources();
        if (sources.size() == 0) return null;

        for (Iterator i=sources.iterator(); i.hasNext(); ) {
            Source source = (Source)i.next();
            graph.addNode(source);
        }

        Collection relationships = entryDefinition.getRelationships();
        for (Iterator i=relationships.iterator(); i.hasNext(); ) {
            Relationship relationship = (Relationship)i.next();
            // System.out.println("Checking ["+relationship.getExpression()+"]");

            String lhs = relationship.getLhs();
            int li = lhs.indexOf(".");
            String lsourceName = lhs.substring(0, li);

            String rhs = relationship.getRhs();
            int ri = rhs.indexOf(".");
            String rsourceName = rhs.substring(0, ri);

            Source lsource = entryDefinition.getEffectiveSource(lsourceName);
            Source rsource = entryDefinition.getEffectiveSource(rsourceName);
            graph.addEdge(lsource, rsource, relationship);
        }

        // System.out.println("Graph: "+graph);

        return graph;
    }

    public EntryDefinition removeEntryDefinition(EntryDefinition entry) {
        if (entry.getParent() == null) {
            rootEntryDefinitions.remove(entry);

        } else {
            Collection siblings = entry.getParent().getChildren();
            siblings.remove(entry);
        }

        return (EntryDefinition)entryDefinitions.remove(entry.getDn());
    }
    
    public void renameEntryDefinition(EntryDefinition entry, String newDn) {
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
                renameEntryDefinition(child, childNewDn);
        	}
        }
		entryDefinitions.remove(oldDn);
    }

    public void addSourceDefinition(SourceDefinition sourceDefinition) throws Exception {
        sourceDefinitions.put(sourceDefinition.getName(), sourceDefinition);

        String connectionName = sourceDefinition.getConnectionName();
        if (connectionName == null) throw new Exception("Missing connection name");

        ConnectionConfig connectionConfig = getConnectionConfig(connectionName);
        if (connectionConfig == null) throw new Exception("Undefined connection "+connectionName);

        sourceDefinition.setConnectionConfig(connectionConfig);

        String adapterName = connectionConfig.getAdapterName();
        if (adapterName == null) throw new Exception("Missing adapter name");

        AdapterConfig adapterConfig = getAdapterConfig(adapterName);
        if (adapterConfig == null) throw new Exception("Undefined adapter "+adapterName);

        sourceDefinition.setAdapterConfig(adapterConfig);
    }
    
    public SourceDefinition removeSourceDefinition(String sourceName) {
    	return (SourceDefinition)sourceDefinitions.remove(sourceName);
    }
    
    public void addModuleConfig(ModuleConfig moduleConfig) throws Exception {
        moduleConfigs.put(moduleConfig.getModuleName(), moduleConfig);
    }

    public ModuleConfig getModuleConfig(String name) {
        return (ModuleConfig)moduleConfigs.get(name);
    }

    public void addModuleMapping(GenericModuleMapping mapping) throws Exception {
        moduleMappings.put(mapping.getModuleName(), mapping);

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

        String adapterName = connectionConfig.getAdapterName();
        if (adapterName == null) throw new Exception("Missing adapter name");

        AdapterConfig adapterConfig = getAdapterConfig(adapterName);
        if (adapterConfig == null) throw new Exception("Undefined adapter "+adapterName);

        connectionConfig.setAdapterConfig(adapterConfig);
	}
	
	public ConnectionConfig removeConnectionConfig(String connectionName) {
		return (ConnectionConfig)connectionConfigs.remove(connectionName);
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
        sb.append(nl);
        sb.append(nl);

        for (Iterator i = cacheConfigs.keySet().iterator(); i.hasNext();) {
            String cacheName = (String) i.next();
            CacheConfig sourceCache = (CacheConfig) cacheConfigs.get(cacheName);
            sb.append(cacheName + " (" + sourceCache.getCacheClass() + ")" + nl);
            sb.append("Parameters:" + nl);
            for (Iterator j = sourceCache.getParameterNames().iterator(); j.hasNext();) {
                String name = (String) j.next();
                String value = sourceCache.getParameter(name);
                sb.append("- " + name + ": " + value + nl);
            }
            sb.append(nl);
        }

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

        for (Iterator i = sourceDefinitions.values().iterator(); i.hasNext();) {
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

		sb.append("ENTRIES:");
        sb.append(nl);
        sb.append(nl);

		for (Iterator i = entryDefinitions.keySet().iterator(); i.hasNext();) {
			String dn = (String) i.next();
			EntryDefinition entry = (EntryDefinition) entryDefinitions.get(dn);
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

		Collection childDefinitions = entry.getChildDefinitions();
		for (Iterator i = childDefinitions.iterator(); i.hasNext();) {
			MappingRule child = (MappingRule) i.next();
			sb.append("=> "+child.getFile() + nl);
		}

        sb.append(nl);

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

    public CacheConfig getCacheConfig() {
        return (CacheConfig)cacheConfigs.get("DEFAULT");
    }

    public CacheConfig getCacheConfig(String name) {
        return (CacheConfig)cacheConfigs.get(name);
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

    public Collection getSourceDefinitions() {
		return sourceDefinitions.values();
	}

    public SourceDefinition getSourceDefinition(String name) {
        return (SourceDefinition)sourceDefinitions.get(name);
    }

    public SourceDefinition getSourceDefinition(Source source) {
        return getSourceDefinition(source.getSourceName());
    }

    public ModuleConfig removeModuleConfig(String moduleName) {
    	return (ModuleConfig)moduleConfigs.remove(moduleName);
    }
    
    public Collection getModuleConfigs() {
    	return moduleConfigs.values();
    }
    
    public ModuleMapping removeModuleMapping(String moduleName) {
    	return (ModuleMapping)moduleMappings.remove(moduleName);
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

    public void setSourceDefinitions(Map sourceDefinitions) {
        this.sourceDefinitions = sourceDefinitions;
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

    public Map getGraphs() {
        return graphs;
    }

    public void setGraphs(Map graphs) {
        this.graphs = graphs;
    }

    public Map getPrimarySources() {
        return primarySources;
    }

    public void setPrimarySources(Map primarySources) {
        this.primarySources = primarySources;
    }
}
