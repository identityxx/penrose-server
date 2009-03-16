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
package org.safehaus.penrose.directory;

import org.safehaus.penrose.acl.ACI;
import org.safehaus.penrose.ldap.DN;
import org.safehaus.penrose.ldap.DNBuilder;
import org.safehaus.penrose.ldap.RDN;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class EntryConfig implements Serializable, Cloneable {

    public final static long serialVersionUID = 1L;

    public boolean enabled = true;
    public boolean attached = true;

    public String name;
    public String parentName;

    public DN dn;

    public String entryClass;

    public Collection<String> objectClasses = new TreeSet<String>();

    public String description;

    public String mappingName;

    public Collection<EntryAttributeConfig> attributeConfigs = new LinkedHashSet<EntryAttributeConfig>();
    public Map<String,Collection<EntryAttributeConfig>> attributeConfigsByName = new TreeMap<String,Collection<EntryAttributeConfig>>();
    public Collection<EntryAttributeConfig> rdnAttributeConfigs = new LinkedHashSet<EntryAttributeConfig>();

    public List<EntrySourceConfig> sourceConfigs = new ArrayList<EntrySourceConfig>();

    public Collection<ACI> acl = new ArrayList<ACI>();

    public Map<String,String> parameters = new TreeMap<String,String>();

    public String initScript;
    public String addScript;
    public String bindScript;
    public String compareScript;
    public String deleteScript;
    public String modifyScript;
    public String modrdnScript;
    public String searchScript;
    public String unbindScript;
    public String destroyScript;

    public Map<String,EntrySearchConfig> searchConfigs = new LinkedHashMap<String,EntrySearchConfig>();

    public EntryConfig() {
        dn = new DN();
    }

    public EntryConfig(String dn) {
        this.dn = new DN(dn);
    }

    public EntryConfig(DN dn) {
        this.dn = dn;
    }

    public EntryConfig(String rdn, EntryConfig parent) throws Exception {
        DNBuilder db = new DNBuilder();
        db.set(rdn);
        db.append(parent.getDn());

        dn = db.toDn();
    }

    public EntryConfig(RDN rdn, EntryConfig parent) throws Exception {
        DNBuilder db = new DNBuilder();
        db.set(rdn);
        db.append(parent.getDn());

        dn = db.toDn();
    }

    public RDN getRdn() throws Exception {
        return dn.getRdn();
    }
    
    public DN getParentDn() throws Exception {
        return dn.getParentDn();
    }

    public boolean isDynamic() {
        for (EntryAttributeConfig entryAttributeConfig : attributeConfigs) {
            if (entryAttributeConfig.getConstant() == null) return true;
        }

        return false;
    }
    
    public Collection<EntryAttributeConfig> getRdnAttributeConfigs() {
        return rdnAttributeConfigs;
    }

    public Collection getNonRdnAttributeConfigs() {
        Collection<EntryAttributeConfig> results = new ArrayList<EntryAttributeConfig>();
        for (EntryAttributeConfig attributeConfig : attributeConfigs) {
            if (attributeConfig.isRdn()) continue;
            results.add(attributeConfig);
        }
        return results;
    }

    public DN getDn() {
        return dn;
    }

    public void setDn(DN dn) {
        this.dn = dn;
    }

    public void setDn(String dn) {
        this.dn = new DN(dn);
    }

    public String getStringDn() {
        return dn.toString();
    }

    public void setStringDn(String dn) {
        this.dn = new DN(dn);
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public boolean isAttached() {
        return attached;
    }

    public void setAttached(boolean attached) {
        this.attached = attached;
    }

    public Collection<EntrySourceConfig> getSourceConfigs() {
        return sourceConfigs;
    }

    public Collection<String> getSourceNames() {
        Collection<String> list = new ArrayList<String>();
        for (EntrySourceConfig sourceConfig : sourceConfigs) {
            list.add(sourceConfig.getSourceName());
        }
        return list;
    }

    public Collection<String> getObjectClasses() {
        return objectClasses;
    }

    public boolean containsObjectClass(String objectClass) {
        for (String oc : objectClasses) {
            if (oc.equalsIgnoreCase(objectClass)) return true;
        }
        return false;
    }

	public void addObjectClass(String oc) {
		objectClasses.add(oc);
	}

    public void addObjectClasses(Collection<String> list) {
        objectClasses.addAll(list);
    }

    public void setObjectClasses(Collection<String> list) {
        objectClasses.clear();
        objectClasses.addAll(list);
    }

    public void removeObjectClass(String oc) {
        objectClasses.remove(oc);
    }

    public void removeObjectClasses() {
        objectClasses.clear();
    }

    public void addSourceConfig(EntrySourceConfig entrySourceConfig) {
        sourceConfigs.add(entrySourceConfig);
    }

    public void addSourceConfigs(Collection<EntrySourceConfig> entrySourceConfigs) {
        sourceConfigs.addAll(entrySourceConfigs);
    }

    public void setSourceConfigs(Collection<EntrySourceConfig> entrySourceConfigs) {
        sourceConfigs.clear();
        sourceConfigs.addAll(entrySourceConfigs);
    }

    public int getSourceConfigIndex(EntrySourceConfig entrySourceConfig) {
        return sourceConfigs.indexOf(entrySourceConfig);
    }

    public void setSourceIndex(EntrySourceConfig sourceConfig, int index) {
        sourceConfigs.remove(sourceConfig);
        sourceConfigs.add(index, sourceConfig);
    }

    public void removeSourceConfigs() {
        sourceConfigs.clear();
    }

    public EntrySourceConfig getSourceConfig(String name) {
        for (EntrySourceConfig sourceConfig : sourceConfigs) {
            if (sourceConfig.getAlias().equals(name)) return sourceConfig;
        }
        return null;
    }

    public EntrySourceConfig getSourceConfig(int index) {
        try {
            return sourceConfigs.get(index);
        } catch (Exception e) {
            return null;
        }
    }

    public EntrySourceConfig removeSourceConfig(String name) {
        EntrySourceConfig sourceConfig = getSourceConfig(name);
        if (sourceConfig != null) {
            sourceConfigs.remove(sourceConfig);
        }
        return sourceConfig;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Attribute Configs
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public Collection<EntryAttributeConfig> getAttributeConfigs() {
        return attributeConfigs;
    }

    public void addAttributeConfigs(Collection<EntryAttributeConfig> entryAttributeConfigs) {
        for (EntryAttributeConfig attributeConfig : entryAttributeConfigs) {
            addAttributeConfig(attributeConfig);
        }
    }

    public void setAttributeConfigs(Collection<EntryAttributeConfig> entryAttributeConfigs) {
        removeAttributeConfigs();
        for (EntryAttributeConfig attributeConfig : entryAttributeConfigs) {
            addAttributeConfig(attributeConfig);
        }
    }

    public void removeAttributeConfigs() {
        attributeConfigs.clear();
        attributeConfigsByName.clear();
        rdnAttributeConfigs.clear();
    }

    public void addAttributesFromRdn() throws Exception {
        RDN rdn = dn.getRdn();
        for (String name : rdn.getNames()) {
            Object value = rdn.get(name);

            EntryAttributeConfig attributeConfig = new EntryAttributeConfig(name, value, true);
            addAttributeConfig(attributeConfig);
        }
    }

    public void addAttributeConfig(String name, Object value) {
        addAttributeConfig(new EntryAttributeConfig(name, value));
    }

    public void addAttributeConfig(EntryAttributeConfig attributeConfig) {
        String name = attributeConfig.getName().toLowerCase();
        //log.debug("Adding attribute "+name+" ("+attributeConfig.isRdn()+")");

        attributeConfigs.add(attributeConfig);

        Collection<EntryAttributeConfig> list = attributeConfigsByName.get(name);
        if (list == null) {
            list = new LinkedHashSet<EntryAttributeConfig>();
            attributeConfigsByName.put(name, list);
        }
        list.add(attributeConfig);

        if (attributeConfig.isRdn()) {
            rdnAttributeConfigs.add(attributeConfig);
        }
    }

    public EntryAttributeConfig getAttributeConfig(String name) {
        Collection<EntryAttributeConfig> list = attributeConfigsByName.get(name.toLowerCase());
        if (list == null) return null;

        Iterator i = list.iterator();
        if (!i.hasNext()) return null;

        return (EntryAttributeConfig)i.next();
    }

    public Collection<EntryAttributeConfig> getAttributeConfigs(String name) {
        return attributeConfigsByName.get(name.toLowerCase());
    }

    public Collection<EntryAttributeConfig> getAttributeConfigs(Collection<String> names) {
        if (names == null) return attributeConfigs;

        Collection<EntryAttributeConfig> results = new ArrayList<EntryAttributeConfig>();
        for (String name : names) {
            Collection<EntryAttributeConfig> list = attributeConfigsByName.get(name.toLowerCase());
            if (list == null) continue;
            results.addAll(list);
        }

        return results;
    }

    public void removeAttributeConfigs(String name) {
        String key = name.toLowerCase();
        Collection<EntryAttributeConfig> list = attributeConfigsByName.remove(key);
        for (EntryAttributeConfig attributeConfig : list) {
            attributeConfigs.remove(attributeConfig);
            rdnAttributeConfigs.remove(attributeConfig);
        }
        if (list.isEmpty()) attributeConfigsByName.remove(key);
    }

    public void removeAttributeConfig(EntryAttributeConfig attributeConfig) {

        String key = attributeConfig.getName().toLowerCase();

        attributeConfigs.remove(attributeConfig);
        rdnAttributeConfigs.remove(attributeConfig);

        Collection<EntryAttributeConfig> list = attributeConfigsByName.get(key);
        if (list == null) return;

        list.remove(attributeConfig);
        if (list.isEmpty()) attributeConfigsByName.remove(key);
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getMappingName() {
        return mappingName;
    }

    public void setMappingName(String mappingName) {
        this.mappingName = mappingName;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Access Control List
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void setACL(Collection<ACI> acl) {
        this.acl.clear();
        this.acl.addAll(acl);
    }

    public void addACI(ACI aci) {
        acl.add(aci);
    }

    public Collection<ACI> getACL() {
        return acl;
    }

    public void removeACI(ACI aci) {
        acl.remove(aci);
    }
    
    public void removeACL() {
        acl.clear();
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Parameters
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public String getParameter(String name) {
        return parameters.get(name);
    }

    public void setParameter(String name, String value) {
        parameters.put(name, value);
    }

    public void removeParameter(String name) {
        parameters.remove(name);
    }

    public void removeParameters() {
        parameters.clear();
    }

    public Collection<String> getParameterNames() {
        return parameters.keySet();
    }

    public Map<String,String> getParameters() {
        return parameters;
    }

    public void setParameters(Map<String,String> parameters) {
        if (parameters == this.parameters) return;
        this.parameters.clear();
        this.parameters.putAll(parameters);
    }

    public int hashCode() {
        return name == null ? 0 : name.hashCode();
    }

    boolean equals(Object o1, Object o2) {
        if (o1 == null && o2 == null) return true;
        if (o1 != null) return o1.equals(o2);
        return o2.equals(o1);
    }

    public boolean equals(Object object) {
        if (object == this) return true;
        if (object == null) return false;
        if (object.getClass() != this.getClass()) return false;

        EntryConfig entryConfig = (EntryConfig)object;
        if (enabled != entryConfig.enabled) return false;
        if (attached != entryConfig.attached) return false;

        if (!equals(name, entryConfig.name)) return false;
        if (!equals(parentName, entryConfig.parentName)) return false;

        if (!equals(dn, entryConfig.dn)) return false;

        if (!equals(entryClass, entryConfig.entryClass)) return false;
        if (!equals(description, entryConfig.description)) return false;
        if (!equals(mappingName, entryConfig.mappingName)) return false;

        if (!equals(objectClasses, entryConfig.objectClasses)) return false;
        if (!equals(attributeConfigs, entryConfig.attributeConfigs)) return false;

        if (!equals(sourceConfigs, entryConfig.sourceConfigs)) return false;
        if (!equals(acl, entryConfig.acl)) return false;
        if (!equals(parameters, entryConfig.parameters)) return false;

        if (!equals(initScript, entryConfig.initScript)) return false;
        if (!equals(addScript, entryConfig.addScript)) return false;
        if (!equals(bindScript, entryConfig.bindScript)) return false;
        if (!equals(compareScript, entryConfig.compareScript)) return false;
        if (!equals(deleteScript, entryConfig.deleteScript)) return false;
        if (!equals(modifyScript, entryConfig.modifyScript)) return false;
        if (!equals(modrdnScript, entryConfig.modrdnScript)) return false;
        if (!equals(searchScript, entryConfig.searchScript)) return false;
        if (!equals(unbindScript, entryConfig.unbindScript)) return false;
        if (!equals(destroyScript, entryConfig.destroyScript)) return false;

        if (!equals(searchConfigs, entryConfig.searchConfigs)) return false;

        return true;
    }

    public void copy(EntryConfig entryConfig) throws CloneNotSupportedException {
        enabled = entryConfig.enabled;
        attached = entryConfig.attached;

        name = entryConfig.name;
        parentName = entryConfig.parentName;

        dn = entryConfig.dn;

        entryClass = entryConfig.entryClass;
        description = entryConfig.description;
        mappingName = entryConfig.mappingName;

        objectClasses = new TreeSet<String>();
        for (String objectClass : entryConfig.objectClasses) {
            addObjectClass(objectClass);
        }

        attributeConfigs = new LinkedHashSet<EntryAttributeConfig>();
        attributeConfigsByName = new TreeMap<String,Collection<EntryAttributeConfig>>();
        rdnAttributeConfigs = new LinkedHashSet<EntryAttributeConfig>();

        for (EntryAttributeConfig attributeConfig : entryConfig.attributeConfigs) {
            addAttributeConfig((EntryAttributeConfig) attributeConfig.clone());
        }

        sourceConfigs = new ArrayList<EntrySourceConfig>();
        for (EntrySourceConfig sourceConfig : entryConfig.sourceConfigs) {
            addSourceConfig((EntrySourceConfig) sourceConfig.clone());
        }

        acl = new ArrayList<ACI>();
        for (ACI aci : entryConfig.acl) {
            addACI((ACI) aci.clone());
        }

        parameters = new TreeMap<String,String>();
        parameters.putAll(entryConfig.parameters);

        initScript = entryConfig.initScript;
        addScript = entryConfig.addScript;
        bindScript = entryConfig.bindScript;
        compareScript = entryConfig.compareScript;
        deleteScript = entryConfig.deleteScript;
        modifyScript = entryConfig.modifyScript;
        modrdnScript = entryConfig.modrdnScript;
        searchScript = entryConfig.searchScript;
        unbindScript = entryConfig.unbindScript;
        destroyScript = entryConfig.destroyScript;

        searchConfigs = new LinkedHashMap<String,EntrySearchConfig>();
        for (EntrySearchConfig searchConfig : entryConfig.searchConfigs.values()) {
            addSearchConfig((EntrySearchConfig) searchConfig.clone());
        }
    }

    public Object clone() throws CloneNotSupportedException {
        EntryConfig entryConfig = (EntryConfig)super.clone();
        entryConfig.copy(this);
        return entryConfig;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getParentName() {
        return parentName;
    }

    public void setParentName(String parentName) {
        this.parentName = parentName;
    }

    public String getPrimarySourceName() {
        if (sourceConfigs.size() == 0) return null;
        EntrySourceConfig sourceConfig = sourceConfigs.get(0);
        return sourceConfig.getAlias();
/*
        for (AttributeMapping rdnAttributeMapping : rdnAttributeMappings) {

            String variable = rdnAttributeMapping.getVariable();
            if (variable == null) continue;

            int i = variable.indexOf('.');
            if (i < 0) continue;

            return variable.substring(0, i);
        }

        return null;
*/
    }

    public String getEntryClass() {
        return entryClass;
    }

    public void setEntryClass(String entryClass) {
        this.entryClass = entryClass;
    }

    public String getInitScript() {
        return initScript;
    }

    public void setInitScript(String initScript) {
        this.initScript = initScript;
    }

    public String getAddScript() {
        return addScript;
    }

    public void setAddScript(String addScript) {
        this.addScript = addScript;
    }

    public String getBindScript() {
        return bindScript;
    }

    public void setBindScript(String bindScript) {
        this.bindScript = bindScript;
    }

    public String getCompareScript() {
        return compareScript;
    }

    public void setCompareScript(String compareScript) {
        this.compareScript = compareScript;
    }

    public String getDeleteScript() {
        return deleteScript;
    }

    public void setDeleteScript(String deleteScript) {
        this.deleteScript = deleteScript;
    }

    public String getModifyScript() {
        return modifyScript;
    }

    public void setModifyScript(String modifyScript) {
        this.modifyScript = modifyScript;
    }

    public String getModrdnScript() {
        return modrdnScript;
    }

    public void setModrdnScript(String modrdnScript) {
        this.modrdnScript = modrdnScript;
    }

    public String getSearchScript() {
        return searchScript;
    }

    public void setSearchScript(String searchScript) {
        this.searchScript = searchScript;
    }

    public String getDestroyScript() {
        return destroyScript;
    }

    public void setDestroyScript(String destroyScript) {
        this.destroyScript = destroyScript;
    }

    public String getUnbindScript() {
        return unbindScript;
    }

    public void setUnbindScript(String unbindScript) {
        this.unbindScript = unbindScript;
    }

    public void addSearchConfig(EntrySearchConfig searchConfig) {

        Logger log = LoggerFactory.getLogger(getClass());

        String name = searchConfig.getName();
        if (name == null) {
            int counter = 0;
            name = "search"+counter;
            while (searchConfigs.containsKey(name)) {
                counter++;
                name = "search"+counter;
            }
            searchConfig.setName(name);
        }

        log.debug("Adding search config "+name+".");
        searchConfigs.put(name, searchConfig);
    }

    public Collection<EntrySearchConfig> getSearchConfigs() {
        return searchConfigs.values();
    }
}
