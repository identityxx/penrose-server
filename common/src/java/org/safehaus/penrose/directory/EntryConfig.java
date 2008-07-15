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
import org.safehaus.penrose.ldap.RDN;
import org.safehaus.penrose.ldap.DNBuilder;

import java.util.*;
import java.io.Serializable;

/**
 * @author Endi S. Dewata
 */
public class EntryConfig implements Serializable, Cloneable {

    public final static String QUERY_CACHE_SIZE                = "queryCacheSize";
    public final static String QUERY_CACHE_EXPIRATION          = "queryCacheExpiration";

    public final static String DATA_CACHE_SIZE                 = "dataCacheSize";
    public final static String DATA_CACHE_EXPIRATION           = "dataCacheExpiration";

    public final static String BATCH_SIZE                      = "batchSize";

    public final static String CACHE                           = "cache";

    public final static int    DEFAULT_QUERY_CACHE_SIZE        = 100;
    public final static int    DEFAULT_QUERY_CACHE_EXPIRATION  = 5;

    public final static int    DEFAULT_DATA_CACHE_SIZE         = 100;
    public final static int    DEFAULT_DATA_CACHE_EXPIRATION   = 5;

    public final static int    DEFAULT_BATCH_SIZE              = 20;

    public final static String DEFAULT_CACHE                   = "DEFAULT";

    public boolean enabled = true;
    public boolean attached = true;

    public String id;
    public String parentId;

    public DN dn;

    public String entryClass;

    public Collection<String> objectClasses = new TreeSet<String>();

    public String description;

    public boolean staticRdn = true;

    public Collection<AttributeMapping> attributeMappings                   = new LinkedHashSet<AttributeMapping>();
    public Map<String,Collection<AttributeMapping>> attributeMappingsByName = new TreeMap<String,Collection<AttributeMapping>>();
    public Collection<AttributeMapping> rdnAttributeMappings                = new LinkedHashSet<AttributeMapping>();

    public List<SourceMapping> sourceMappings = new ArrayList<SourceMapping>();
    
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

    public EntryConfig() {
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

    public RDN getRdn() {
        return dn.getRdn();
    }
    
    public DN getParentDn() {
        return dn.getParentDn();
    }

    public boolean isStaticRdn() {
        return staticRdn;
    }

    public boolean isDynamic() {
        for (AttributeMapping attributeMapping : attributeMappings) {
            if (attributeMapping.getConstant() == null) return true;
        }

        return false;
    }
    
    public Collection<AttributeMapping> getRdnAttributeMappings() {
        return rdnAttributeMappings;
    }

    public Collection getNonRdnAttributeMappings() {
        Collection<AttributeMapping> results = new ArrayList<AttributeMapping>();
        for (AttributeMapping attributeMapping : attributeMappings) {
            if (attributeMapping.isRdn()) continue;
            results.add(attributeMapping);
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

    public Collection<AttributeMapping> getAttributeMappings() {
        return attributeMappings;
    }

    public void removeAttributeMappings() {
        attributeMappings.clear();
        attributeMappingsByName.clear();
        rdnAttributeMappings.clear();
    }

    public Collection<SourceMapping> getSourceMappings() {
        return sourceMappings;
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

    public void removeObjectClass(String oc) {
        objectClasses.remove(oc);
    }

    public void removeObjectClasses() {
        objectClasses.clear();
    }

    public void addSourceMapping(SourceMapping sourceMapping) {
        sourceMappings.add(sourceMapping);
    }

    public int getSourceMappingIndex(SourceMapping sourceMapping) {
        return sourceMappings.indexOf(sourceMapping);
    }

    public void setSourceIndex(SourceMapping sourceMapping, int index) {
        sourceMappings.remove(sourceMapping);
        sourceMappings.add(index, sourceMapping);
    }

    public void removeSourceMappings() {
        sourceMappings.clear();
    }

    public SourceMapping getSourceMapping(String name) {
        for (SourceMapping sourceMapping : sourceMappings) {
            if (sourceMapping.getName().equals(name)) return sourceMapping;
        }
        return null;
    }

    public SourceMapping getSourceMapping(int index) {
        try {
            return sourceMappings.get(index);
        } catch (Exception e) {
            return null;
        }
    }

    public SourceMapping removeSourceMapping(String name) {
        SourceMapping sourceMapping = getSourceMapping(name);
        if (sourceMapping != null) {
            sourceMappings.remove(sourceMapping);
        }
        return sourceMapping;
    }

    public void addAttributeMappings(Collection<AttributeMapping> attributeMappings) {
        for (AttributeMapping attributeMapping : attributeMappings) {
            addAttributeMapping(attributeMapping);
        }
    }

    public void addAttributeMappingsFromRdn() {
        RDN rdn = dn.getRdn();
        for (String name : rdn.getNames()) {
            Object value = rdn.get(name);

            AttributeMapping attributeMapping = new AttributeMapping(name, value, true);
            addAttributeMapping(attributeMapping);
        }
    }

    public void addAttributeMapping(String name, Object value) {
        addAttributeMapping(new AttributeMapping(name, value));
    }

    public void addAttributeMapping(AttributeMapping attributeMapping) {
        String name = attributeMapping.getName().toLowerCase();
        //log.debug("Adding attribute "+name+" ("+attributeMapping.isRdn()+")");

        attributeMappings.add(attributeMapping);

        Collection<AttributeMapping> list = attributeMappingsByName.get(name);
        if (list == null) {
            list = new LinkedHashSet<AttributeMapping>();
            attributeMappingsByName.put(name, list);
        }
        list.add(attributeMapping);

        if (attributeMapping.isRdn()) {
            rdnAttributeMappings.add(attributeMapping);
        }

        staticRdn &= attributeMapping.getConstant() != null;
    }

    public AttributeMapping getAttributeMapping(String name) {
        Collection<AttributeMapping> list = attributeMappingsByName.get(name.toLowerCase());
        if (list == null) return null;

        Iterator i = list.iterator();
        if (!i.hasNext()) return null;

        return (AttributeMapping)i.next();
    }

    public Collection<AttributeMapping> getAttributeMappings(String name) {
        return attributeMappingsByName.get(name.toLowerCase());
    }

    public Collection<AttributeMapping> getAttributeMappings(Collection<String> names) {
        if (names == null) return attributeMappings;

        Collection<AttributeMapping> results = new ArrayList<AttributeMapping>();
        for (String name : names) {
            Collection<AttributeMapping> list = attributeMappingsByName.get(name.toLowerCase());
            if (list == null) continue;
            results.addAll(list);
        }

        return results;
    }

    public void removeAttributeMappings(String name) {
        Collection<AttributeMapping> list = attributeMappingsByName.remove(name.toLowerCase());
        for (AttributeMapping attributeMapping : list) {
            attributeMappings.remove(attributeMapping);
            rdnAttributeMappings.remove(attributeMapping);
        }
    }

    public void removeAttributeMapping(AttributeMapping attributeMapping) {

        attributeMappings.remove(attributeMapping);
        rdnAttributeMappings.remove(attributeMapping);

        String key = attributeMapping.getName().toLowerCase();

        Collection<AttributeMapping> list = attributeMappingsByName.get(key);
        if (list == null) return;

        list.remove(attributeMapping);
        if (list.isEmpty()) attributeMappingsByName.remove(key);
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
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

    public String getParameter(String name) {
        return parameters.get(name);
    }

    public void setParameter(String name, String value) {
        parameters.put(name, value);
    }

    public void removeParameter(String name) {
        parameters.remove(name);
    }

    public Collection<String> getParameterNames() {
        return parameters.keySet();
    }

    public int hashCode() {
        return id == null ? 0 : id.hashCode();
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

        if (!equals(id, entryConfig.id)) return false;
        if (!equals(parentId, entryConfig.parentId)) return false;

        if (!equals(dn, entryConfig.dn)) return false;

        if (!equals(entryClass, entryConfig.entryClass)) return false;
        if (!equals(description, entryConfig.description)) return false;

        if (!equals(objectClasses, entryConfig.objectClasses)) return false;
        if (!equals(attributeMappings, entryConfig.attributeMappings)) return false;

        if (!equals(sourceMappings, entryConfig.sourceMappings)) return false;
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

        return true;
    }

    public void copy(EntryConfig entryConfig) throws CloneNotSupportedException {
        enabled = entryConfig.enabled;
        attached = entryConfig.attached;

        id = entryConfig.id;
        parentId = entryConfig.parentId;

        dn = entryConfig.dn;

        entryClass = entryConfig.entryClass;
        description = entryConfig.description;

        objectClasses = new TreeSet<String>();
        for (String objectClass : entryConfig.objectClasses) {
            addObjectClass(objectClass);
        }

        attributeMappings       = new LinkedHashSet<AttributeMapping>();
        attributeMappingsByName = new TreeMap<String,Collection<AttributeMapping>>();
        rdnAttributeMappings    = new LinkedHashSet<AttributeMapping>();

        for (AttributeMapping attributeMapping : entryConfig.attributeMappings) {
            addAttributeMapping((AttributeMapping) attributeMapping.clone());
        }

        sourceMappings = new ArrayList<SourceMapping>();
        for (SourceMapping sourceMapping : entryConfig.sourceMappings) {
            addSourceMapping((SourceMapping) sourceMapping.clone());
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
    }

    public Object clone() throws CloneNotSupportedException {
        EntryConfig entryConfig = (EntryConfig)super.clone();
        entryConfig.copy(this);
        return entryConfig;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getParentId() {
        return parentId;
    }

    public void setParentId(String parentId) {
        this.parentId = parentId;
    }

    public String getPrimarySourceName() {
        if (sourceMappings.size() == 0) return null;
        SourceMapping sourceMapping = sourceMappings.get(0);
        return sourceMapping.getName();
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
}
