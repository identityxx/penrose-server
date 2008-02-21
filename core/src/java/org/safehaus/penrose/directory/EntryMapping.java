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
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.*;
import java.io.Serializable;

/**
 * @author Endi S. Dewata
 */
public class EntryMapping implements Serializable, Cloneable {

    static {
        log = LoggerFactory.getLogger(EntryMapping.class);
    }

    public static transient Logger log;
    public static boolean debug = log.isDebugEnabled();

    public final static String QUERY_CACHE_SIZE        = "queryCacheSize";
    public final static String QUERY_CACHE_EXPIRATION  = "queryCacheExpiration";

    public final static String DATA_CACHE_SIZE         = "dataCacheSize";
    public final static String DATA_CACHE_EXPIRATION   = "dataCacheExpiration";

    public final static String BATCH_SIZE              = "batchSize";

    public final static String CACHE                   = "cache";

    public final static int    DEFAULT_QUERY_CACHE_SIZE        = 100;
    public final static int    DEFAULT_QUERY_CACHE_EXPIRATION  = 5;

    public final static int    DEFAULT_DATA_CACHE_SIZE         = 100;
    public final static int    DEFAULT_DATA_CACHE_EXPIRATION   = 5;

    public final static int    DEFAULT_BATCH_SIZE              = 20;

    public final static String DEFAULT_CACHE                   = "DEFAULT";

    private boolean enabled = true;
    private boolean attached = true;

    private String id;
    private String parentId;

    private DN dn;

    private String entryClass;

    private Collection<String> objectClasses = new TreeSet<String>();

    private String description;

    private boolean staticRdn = true;

    public Collection<AttributeMapping> attributeMappings = new ArrayList<AttributeMapping>();
    private Map<String,Collection<AttributeMapping>> attributeMappingsByName = new TreeMap<String,Collection<AttributeMapping>>();
    private Collection<AttributeMapping> rdnAttributeMappings = new ArrayList<AttributeMapping>();

    private List<SourceMapping> sourceMappings = new ArrayList<SourceMapping>();
    
    private String handlerName;
    private String engineName;

    private Collection<ACI> acl = new ArrayList<ACI>();

    private Map<String,String> parameters = new TreeMap<String,String>();

	public EntryMapping() {
	}

    public EntryMapping(String dn) {
        this.dn = new DN(dn);
    }

    public EntryMapping(DN dn) {
        this.dn = dn;
    }

    public EntryMapping(String rdn, EntryMapping parent) throws Exception {
        DNBuilder db = new DNBuilder();
        db.set(rdn);
        db.append(parent.getDn());

        dn = db.toDn();
    }

    public EntryMapping(RDN rdn, EntryMapping parent) throws Exception {
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

	public void addAttributeMapping(AttributeMapping attributeMapping) {
        String name = attributeMapping.getName().toLowerCase();
        //log.debug("Adding attribute "+name+" ("+attributeMapping.isRdn()+")");

        attributeMappings.add(attributeMapping);

        Collection<AttributeMapping> list = attributeMappingsByName.get(name);
        if (list == null) {
            list = new ArrayList<AttributeMapping>();
            attributeMappingsByName.put(name, list);
        }
        list.add(attributeMapping);

        if (attributeMapping.isRdn()) {
            rdnAttributeMappings.add(attributeMapping);
        }

        staticRdn &= attributeMapping.getConstant() != null;
    }

    public AttributeMapping getAttributeMapping(String name) {
        Collection<AttributeMapping> list = getAttributeMappings(name);
        if (list == null) return null;
        Iterator i = list.iterator();
        if (!i.hasNext()) return null;
        return (AttributeMapping)i.next();
    }

    public Collection<AttributeMapping> getAttributeMappings(String name) {
        return attributeMappingsByName.get(name.toLowerCase());
    }

    public Collection<AttributeMapping> getAttributeMappings(Collection<String> names) {
        if (names == null) return getAttributeMappings();

        Collection<AttributeMapping> results = new ArrayList<AttributeMapping>();
        for (String name : names) {
            Collection<AttributeMapping> list = getAttributeMappings(name);
            if (list == null) continue;
            results.addAll(list);
        }

        return results;
    }

    public void removeAttributeMappings(String name) {
        attributeMappingsByName.remove(name.toLowerCase());
    }

    public void removeAttributeMapping(AttributeMapping attributeMapping) {
        Collection<AttributeMapping> list = getAttributeMappings(attributeMapping.getName());
        if (list == null) return;

        list.remove(attributeMapping);
        if (list.isEmpty()) removeAttributeMappings(attributeMapping.getName());
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

        EntryMapping entryMapping = (EntryMapping)object;
        if (enabled != entryMapping.enabled) return false;
        if (attached != entryMapping.attached) return false;

        if (!equals(id, entryMapping.id)) return false;
        if (!equals(parentId, entryMapping.parentId)) return false;

        if (!equals(dn, entryMapping.dn)) return false;

        if (!equals(entryClass, entryMapping.entryClass)) return false;
        if (!equals(description, entryMapping.description)) return false;

        if (!equals(objectClasses, entryMapping.objectClasses)) return false;
        if (!equals(attributeMappings, entryMapping.attributeMappings)) return false;

        if (!equals(sourceMappings, entryMapping.sourceMappings)) return false;
        if (!equals(handlerName, entryMapping.handlerName)) return false;
        if (!equals(engineName, entryMapping.engineName)) return false;
        if (!equals(acl, entryMapping.acl)) return false;
        if (!equals(parameters, entryMapping.parameters)) return false;

        return true;
    }

    public void copy(EntryMapping entryMapping) throws CloneNotSupportedException {
        enabled = entryMapping.enabled;
        attached = entryMapping.attached;

        id = entryMapping.id;
        parentId = entryMapping.parentId;

        dn = entryMapping.dn;

        entryClass = entryMapping.entryClass;
        description = entryMapping.description;

        objectClasses = new TreeSet<String>();
        for (String objectClass : entryMapping.objectClasses) {
            addObjectClass(objectClass);
        }

        attributeMappings = new ArrayList<AttributeMapping>();
        attributeMappingsByName = new TreeMap<String,Collection<AttributeMapping>>();
        rdnAttributeMappings = new ArrayList<AttributeMapping>();
        for (AttributeMapping attributeMapping : entryMapping.attributeMappings) {
            addAttributeMapping((AttributeMapping) attributeMapping.clone());
        }

        sourceMappings = new ArrayList<SourceMapping>();
        for (SourceMapping sourceMapping : entryMapping.sourceMappings) {
            addSourceMapping((SourceMapping) sourceMapping.clone());
        }

        handlerName = entryMapping.handlerName;
        engineName = entryMapping.engineName;

        acl = new ArrayList<ACI>();
        for (ACI aci : entryMapping.acl) {
            addACI((ACI) aci.clone());
        }

        parameters = new TreeMap<String,String>();
        parameters.putAll(entryMapping.parameters);
    }

    public Object clone() throws CloneNotSupportedException {
        EntryMapping entryMapping = (EntryMapping)super.clone();
        entryMapping.copy(this);
        return entryMapping;
    }

    public String getHandlerName() {
        return handlerName;
    }

    public void setHandlerName(String handlerName) {
        this.handlerName = handlerName;
    }

    public String getEngineName() {
        return engineName;
    }

    public void setEngineName(String engineName) {
        this.engineName = engineName;
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
}
