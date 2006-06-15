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
package org.safehaus.penrose.mapping;

import org.safehaus.penrose.acl.ACI;
import org.safehaus.penrose.util.EntryUtil;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class EntryMapping implements Cloneable {

    Logger log = Logger.getLogger(getClass());

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

    /**
     * Distinguished name.
     */
    private String rdn = "";

    private String parentDn;

    private boolean enabled = true;

	/**
	 * Children. Each element is of type org.safehaus.penrose.mapping.EntryMapping.
	 */
    private Collection childMappings = new ArrayList();

    /**
     * Object classes. Each element is of type String.
     */
    private Collection objectClasses = new TreeSet();
    
    private String script;

    /**
     * Attributes. The keys are the attribute names (java.lang.String). Each value is of type org.safehaus.penrose.mapping.AttributeMapping.
     */
    private Map attributeMappings = new TreeMap();

    /**
     * Sources. Each element is of type org.safehaus.penrose.mapping.Source.
     */
    private List sourceMappings = new ArrayList();
    
    /**
     * Relationship. Each element is of type org.safehaus.penrose.mapping.Relationship.
     */
    private Collection relationships = new ArrayList();

    /**
     * Access Control Instruction. Each element is of type org.safehaus.penrose.acl.ACI.
     */
    private Collection acl = new ArrayList();

    private Map parameters = new TreeMap();

	public EntryMapping() {
	}

	public EntryMapping(String dn) {
        Row row = EntryUtil.getRdn(dn);
        rdn = row.toString();
        parentDn = EntryUtil.getParentDn(dn);
    }

    public EntryMapping(String rdn, EntryMapping parent) {
        this.rdn = rdn;
        this.parentDn = parent.getDn();
    }

    public String getRdn() {
        return rdn;
    }
    
    public Row getRdn(AttributeValues attributeValues) {
        Collection rdnAttributes = getRdnAttributes();
        Row row = new Row();

        for (Iterator i=rdnAttributes.iterator(); i.hasNext(); ) {
            AttributeMapping rdnAttribute = (AttributeMapping)i.next();
            String name = rdnAttribute.getName();
            Object value = attributeValues.getOne(name);
            row.set(name, value);
        }

        return row;
    }

    public String getParentDn() {
        return parentDn;
    }

    public boolean isRdnDynamic() {
        for (Iterator i=attributeMappings.values().iterator(); i.hasNext(); ) {
            Collection list = (Collection)i.next();

            for (Iterator j=list.iterator(); j.hasNext(); ) {
                AttributeMapping attributeMapping = (AttributeMapping)j.next();
                if (!attributeMapping.isRdn()) continue;
                if (attributeMapping.getConstant() == null) return true;
            }
        }

        return false;
    }

    public boolean isDynamic() {
        for (Iterator i=attributeMappings.values().iterator(); i.hasNext(); ) {
            Collection list = (Collection)i.next();

            for (Iterator j=list.iterator(); j.hasNext(); ) {
                AttributeMapping attributeMapping = (AttributeMapping)j.next();
                if (attributeMapping.getConstant() == null) return true;
            }
        }

        return false;
    }
    
    public Collection getRdnAttributes() {
        Collection results = new ArrayList();
        for (Iterator i=attributeMappings.values().iterator(); i.hasNext(); ) {
            Collection list = (Collection)i.next();

            for (Iterator j=list.iterator(); j.hasNext(); ) {
                AttributeMapping attributeMapping = (AttributeMapping)j.next();
                if (!attributeMapping.isRdn()) continue;
                results.add(attributeMapping);
            }
        }
        return results;
    }

    public Collection getNonRdnAttributes() {
        Collection results = new ArrayList();
        for (Iterator i=attributeMappings.values().iterator(); i.hasNext(); ) {
            Collection list = (Collection)i.next();

            for (Iterator j=list.iterator(); j.hasNext(); ) {
                AttributeMapping attributeMapping = (AttributeMapping)j.next();
                if (attributeMapping.isRdn()) continue;
                results.add(attributeMapping);
            }
        }
        return results;
    }

    public String getDn() {
        if (rdn == null) return null;
        if (parentDn == null) return rdn;
        return rdn+","+parentDn;
    }

    public void setDn(String dn) {
        Row row = EntryUtil.getRdn(dn);
        rdn = row.toString();
        parentDn = EntryUtil.getParentDn(dn);
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public Collection getAttributeMappings() {
        Collection results = new ArrayList();

        for (Iterator i=attributeMappings.values().iterator(); i.hasNext(); ) {
            Collection list = (Collection)i.next();
            results.addAll(list);
        }

        return results;
    }

    public void removeAttributeMappings() {
        attributeMappings.clear();
    }

    public Collection getRelationships() {
        return relationships;
    }

    public Collection getSourceMappings() {
        return sourceMappings;
    }

    public Collection getObjectClasses() {
        return objectClasses;
    }

    public boolean containsObjectClass(String objectClass) {
        for (Iterator i=objectClasses.iterator(); i.hasNext(); ) {
            String oc = (String)i.next();
            if (oc.equalsIgnoreCase(objectClass)) return true;
        }
        return false;
    }

	public void addObjectClass(String oc) {
		objectClasses.add(oc);
	}

    public void addObjectClasses(Collection list) {
        objectClasses.addAll(list);
    }

    public void removeObjectClass(String oc) {
        objectClasses.remove(oc);
    }

    public void removeObjectClasses() {
        objectClasses.clear();
    }

    public void addEntryMapping(MappingRule mappingRule) {
        childMappings.add(mappingRule);
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
        for (Iterator i=sourceMappings.iterator(); i.hasNext(); ) {
            SourceMapping sourceMapping = (SourceMapping)i.next();
            if (name.equals(sourceMapping.getName())) return sourceMapping;
        }
        return null;
    }

    public SourceMapping getSourceMapping(int index) {
        return (SourceMapping)sourceMappings.get(index);
    }

    public SourceMapping removeSourceMapping(String name) {
        SourceMapping sourceMapping = getSourceMapping(name);
        if (sourceMapping != null) {
            sourceMappings.remove(sourceMapping);
        }
        return sourceMapping;
    }

    public void addAttributeMappings(Collection attributeMappings) {
        for (Iterator i=attributeMappings.iterator(); i.hasNext(); ) {
            AttributeMapping attributeMapping = (AttributeMapping)i.next();
            addAttributeMapping(attributeMapping);
        }
    }

	public void addAttributeMapping(AttributeMapping attributeMapping) {
        String name = attributeMapping.getName().toLowerCase();
        Collection list = (Collection)attributeMappings.get(name);
        if (list == null) {
            list = new ArrayList();
            attributeMappings.put(name, list);
        }
        list.add(attributeMapping);
	}

    public AttributeMapping getAttributeMapping(String name) {
        Collection list = getAttributeMappings(name);
        if (list == null || list.isEmpty()) return null;
        return (AttributeMapping)list.iterator().next();
    }

    public Collection getAttributeMappings(String name) {
        return (Collection)attributeMappings.get(name.toLowerCase());
    }

    public Collection getAttributeMappings(Collection names) {
        if (names == null) return getAttributeMappings();

        Collection results = new ArrayList();
        for (Iterator i=names.iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Collection list = getAttributeMappings(name);
            if (list == null) continue;
            results.addAll(list);
        }

        return results;
    }

    public void removeAttributeMappings(String name) {
        attributeMappings.remove(name.toLowerCase());
    }

    public void removeAttributeMapping(AttributeMapping attributeMapping) {
        Collection list = getAttributeMappings(attributeMapping.getName());
        if (list == null) return;

        list.remove(attributeMapping);
        if (list.isEmpty()) removeAttributeMappings(attributeMapping.getName());
    }

	public void addRelationship(Relationship relationship) {
		relationships.add(relationship);
	}

    public void removeRelationship(Relationship relationship) {
        relationships.remove(relationship);
    }

    public void removeRelationships() {
        relationships.clear();
    }
    
    public String getScript() {
        return script;
    }

    public void setScript(String script) {
        this.script = script;
    }

    public void setRdn(String rdn) {
        this.rdn = rdn;
    }

    public void setParentDn(String parentDn) {
        this.parentDn = parentDn;
    }

    public Collection getChildMappings() {
        return childMappings;
    }

    public void setChildMappings(List childMappings) {
        this.childMappings = childMappings;
    }

    public void addACI(ACI aci) {
        acl.add(aci);
    }

    public Collection getACL() {
        return acl;
    }

    public void removeACI(ACI aci) {
        acl.remove(aci);
    }
    
    public void removeACL() {
        acl.clear();
    }

    public String getParameter(String name) {
        return (String)parameters.get(name);
    }

    public void setParameter(String name, String value) {
        parameters.put(name, value);
    }

    public void removeParameter(String name) {
        parameters.remove(name);
    }

    public Collection getParameterNames() {
        return parameters.keySet();
    }

    public int hashCode() {
        return (rdn == null ? 0 : rdn.hashCode()) +
                (parentDn == null ? 0 : parentDn.hashCode()) +
                (enabled ? 0 : 1) +
                (objectClasses == null ? 0 : objectClasses.hashCode()) +
                (script == null ? 0 : script.hashCode()) +
                (attributeMappings == null ? 0 : attributeMappings.hashCode()) +
                (sourceMappings == null ? 0 : sourceMappings.hashCode()) +
                (relationships == null ? 0 : relationships.hashCode()) +
                (acl == null ? 0 : acl.hashCode()) +
                (parameters == null ? 0 : parameters.hashCode());
    }

    boolean equals(Object o1, Object o2) {
        if (o1 == null && o2 == null) return true;
        if (o1 != null) return o1.equals(o2);
        return o2.equals(o1);
    }

    public boolean equals(Object object) {
        if (this == object) return true;
        if((object == null) || (object.getClass() != this.getClass())) return false;

        EntryMapping entryMapping = (EntryMapping)object;
        if (!equals(rdn, entryMapping.rdn)) return false;
        if (!equals(parentDn, entryMapping.parentDn)) return false;
        if (enabled != entryMapping.enabled) return false;
        if (!equals(objectClasses, entryMapping.objectClasses)) return false;
        if (!equals(script, entryMapping.script)) return false;
        if (!equals(attributeMappings, entryMapping.attributeMappings)) return false;
        if (!equals(sourceMappings, entryMapping.sourceMappings)) return false;
        if (!equals(relationships, entryMapping.relationships)) return false;
        if (!equals(acl, entryMapping.acl)) return false;
        if (!equals(parameters, entryMapping.parameters)) return false;

        return true;
    }

    public void copy(EntryMapping entryMapping) {
        rdn = entryMapping.rdn;
        parentDn = entryMapping.parentDn;
        enabled = entryMapping.enabled;
        script = entryMapping.script;

        removeObjectClasses();
        for (Iterator i=entryMapping.objectClasses.iterator(); i.hasNext(); ) {
            String objectClass = (String)i.next();
            addObjectClass(objectClass);
        }

        removeAttributeMappings();
        for (Iterator i=entryMapping.attributeMappings.values().iterator(); i.hasNext(); ) {
            Collection list = (Collection)i.next();
            for (Iterator j=list.iterator(); j.hasNext(); ) {
                AttributeMapping attribute = (AttributeMapping)j.next();
                addAttributeMapping((AttributeMapping)attribute.clone());
            }
        }

        removeSourceMappings();
        for (Iterator i=entryMapping.sourceMappings.iterator(); i.hasNext(); ) {
            SourceMapping sourceMapping = (SourceMapping)i.next();
            addSourceMapping((SourceMapping)sourceMapping.clone());
        }

        removeRelationships();
        for (Iterator i=entryMapping.relationships.iterator(); i.hasNext(); ) {
            Relationship relationship = (Relationship)i.next();
            addRelationship((Relationship)relationship.clone());
        }

        removeACL();
        for (Iterator i=entryMapping.acl.iterator(); i.hasNext(); ) {
            ACI aci = (ACI)i.next();
            addACI((ACI)aci.clone());
        }

        parameters.clear();
        parameters.putAll(entryMapping.parameters);
    }

    public Object clone() {
        EntryMapping entryMapping = new EntryMapping();
        entryMapping.copy(this);
        return entryMapping;
    }
}
