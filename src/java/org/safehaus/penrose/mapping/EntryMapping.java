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
import org.ietf.ldap.LDAPEntry;
import org.ietf.ldap.LDAPAttributeSet;
import org.ietf.ldap.LDAPAttribute;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class EntryMapping implements Cloneable {

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
    private String rdn;

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
        int i = dn.indexOf(",");
        if (i < 0) {
            rdn = dn;
        } else {
            rdn = dn.substring(0, i);
        }
        parentDn = Entry.getParentDn(dn);
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
            AttributeMapping attributeMapping = (AttributeMapping)i.next();
            if (!attributeMapping.isRdn()) continue;
            if (attributeMapping.getConstant() == null) return true;
        }
        return false;
    }

    public boolean isDynamic() {
        for (Iterator i=attributeMappings.values().iterator(); i.hasNext(); ) {
            AttributeMapping attributeMapping = (AttributeMapping)i.next();
            if (attributeMapping.getConstant() == null) return true;
        }
        return false;
    }
    
    public Collection getRdnAttributes() {
        Collection results = new ArrayList();
        for (Iterator i=attributeMappings.values().iterator(); i.hasNext(); ) {
            AttributeMapping attributeMapping = (AttributeMapping)i.next();
            if (!attributeMapping.isRdn()) continue;
            results.add(attributeMapping);
        }
        return results;
    }

    public Collection getNonRdnAttributes() {
        Collection results = new ArrayList();
        for (Iterator i=attributeMappings.values().iterator(); i.hasNext(); ) {
            AttributeMapping attributeMapping = (AttributeMapping)i.next();
            if (attributeMapping.isRdn()) continue;
            results.add(attributeMapping);
        }
        return results;
    }

    public String getDn() {
        if (rdn == null) return null;
        if (parentDn == null) return rdn;
        return rdn+","+parentDn;
    }

    public void setDn(String dn) {
        int i = dn.indexOf(",");
        if (i < 0) {
            rdn = dn;
            parentDn = null;
        } else {
            rdn = dn.substring(0, i);
            parentDn = dn.substring(i+1);
        }
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public Collection getAttributeMappings() {
        return attributeMappings.values();
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

    public void removeObjectClass(String oc) {
        objectClasses.remove(oc);
    }

    public void removeObjectClasses() {
        objectClasses.clear();
    }

    public void addChildMapping(MappingRule mappingRule) {
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

    public SourceMapping removeSourceMapping(String name) {
        SourceMapping sourceMapping = getSourceMapping(name);
        if (sourceMapping != null) {
            sourceMappings.remove(sourceMapping);
        }
        return sourceMapping;
    }

	public void addAttributeMapping(AttributeMapping attribute) {
		attributeMappings.put(attribute.getName(), attribute);
	}

    public AttributeMapping getAttributeMapping(String name) {
        return (AttributeMapping)attributeMappings.get(name);
    }

    public Collection getAttributeMappings(Collection names) {
        if (names == null) return attributeMappings.values();

        Collection results = new ArrayList();
        for (Iterator i=names.iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            AttributeMapping attributeMapping = (AttributeMapping)attributeMappings.get(name);
            if (attributeMapping == null) continue;
            results.add(attributeMapping);
        }
        return results;
    }

    public AttributeMapping removeAttributeMapping(String name) {
        return (AttributeMapping)attributeMappings.remove(name);
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
        if((object == null) || (object.getClass() != getClass())) return false;

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

    public void copy(EntryMapping entry) {
        rdn = entry.rdn;
        parentDn = entry.parentDn;
        enabled = entry.enabled;
        script = entry.script;

        removeObjectClasses();
        for (Iterator i=entry.objectClasses.iterator(); i.hasNext(); ) {
            String objectClass = (String)i.next();
            addObjectClass(objectClass);
        }

        removeAttributeMappings();
        for (Iterator i=entry.attributeMappings.values().iterator(); i.hasNext(); ) {
            AttributeMapping attribute = (AttributeMapping)i.next();
            addAttributeMapping((AttributeMapping)attribute.clone());
        }

        removeSourceMappings();
        for (Iterator i=entry.sourceMappings.iterator(); i.hasNext(); ) {
            SourceMapping sourceMapping = (SourceMapping)i.next();
            addSourceMapping((SourceMapping)sourceMapping.clone());
        }

        removeRelationships();
        for (Iterator i=entry.relationships.iterator(); i.hasNext(); ) {
            Relationship relationship = (Relationship)i.next();
            addRelationship((Relationship)relationship.clone());
        }

        removeACL();
        for (Iterator i=entry.acl.iterator(); i.hasNext(); ) {
            ACI aci = (ACI)i.next();
            addACI((ACI)aci.clone());
        }

        parameters.clear();
        parameters.putAll(entry.parameters);
    }

    public Object clone() {
        EntryMapping entry = new EntryMapping();
        entry.copy(this);
        return entry;
    }
}
