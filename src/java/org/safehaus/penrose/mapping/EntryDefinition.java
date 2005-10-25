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


import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.acl.ACI;
import org.ietf.ldap.LDAPEntry;
import org.ietf.ldap.LDAPAttributeSet;
import org.ietf.ldap.LDAPAttribute;

import java.util.*;
import java.io.Serializable;

/**
 * @author Endi S. Dewata
 */
public class EntryDefinition implements Cloneable, Serializable {

    public final static String FILTER_CACHE_SIZE       = "filterCacheSize";
    public final static String FILTER_CACHE_EXPIRATION = "filterCacheExpiration";

    public final static String DATA_CACHE_SIZE         = "dataCacheSize";
    public final static String DATA_CACHE_EXPIRATION   = "dataCacheExpiration";

    public final static String SOURCE_CACHE_SIZE       = "sourceCacheSize";
    public final static String SOURCE_CACHE_EXPIRATION = "sourceCacheExpiration";

    public final static String BATCH_SIZE              = "batchSize";

    public final static String CACHE                   = "cache";

    public final static int    DEFAULT_FILTER_CACHE_SIZE       = 100;
    public final static int    DEFAULT_FILTER_CACHE_EXPIRATION = 5;

    public final static int    DEFAULT_DATA_CACHE_SIZE         = 100;
    public final static int    DEFAULT_DATA_CACHE_EXPIRATION   = 5;

    public final static int    DEFAULT_BATCH_SIZE              = 20;

    public final static String DEFAULT_CACHE                   = "EntryCache";

    /**
     * Distinguished name.
     */
    private String rdn;

    private String parentDn;

	/**
	 * Children. Each element is of type org.safehaus.penrose.mapping.EntryDefinition.
	 */
    private Collection childDefinitions = new ArrayList();

    /**
     * Object classes. Each element is of type String.
     */
    private Collection objectClasses = new TreeSet();
    
    private String script;

    /**
     * Attributes. The keys are the attribute names (java.lang.String). Each value is of type org.safehaus.penrose.mapping.AttributeDefinition.
     */
    private Map attributeDefinitions = new TreeMap();

    /**
     * Sources. Each element is of type org.safehaus.penrose.mapping.Source.
     */
    private List sources = new ArrayList();
    
    /**
     * Relationship. Each element is of type org.safehaus.penrose.mapping.Relationship.
     */
    private Collection relationships = new ArrayList();

    /**
     * Access Control Instruction. Each element is of type org.safehaus.penrose.acl.ACI.
     */
    private Collection acl = new ArrayList();

    private Map parameters = new TreeMap();

	public EntryDefinition() {
	}

	public EntryDefinition(String dn) {
        int i = dn.indexOf(",");
        if (i < 0) {
            rdn = dn;
            parentDn = null;
        } else {
            rdn = dn.substring(0, i);
            parentDn = dn.substring(i+1);
        }
    }

    public EntryDefinition(String rdn, EntryDefinition parent) {
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
            AttributeDefinition rdnAttribute = (AttributeDefinition)i.next();
            String name = rdnAttribute.getName();
            Object value = attributeValues.getOne(name);
            row.set(name, value);
        }

        return row;
    }

    public String getParentDn() {
        return parentDn;
    }

    public boolean isDynamic() {
        return rdn.indexOf("...") >= 0;
    }
    
    public void setDynamic(boolean mapping) {
    	if (mapping && !isDynamic()) {
    		int j = rdn.indexOf("=")+1;
    		rdn = rdn.substring(0, j) + "...";

    	} else if (!mapping && isDynamic()) {
    		rdn = rdn.replaceAll("\\.\\.\\.", "value");
    	}
    }

    public Collection getRdnAttributes() {
        Collection results = new ArrayList();
        for (Iterator i=attributeDefinitions.values().iterator(); i.hasNext(); ) {
            AttributeDefinition attribute = (AttributeDefinition)i.next();
            if (!attribute.isRdn()) continue;
            results.add(attribute);
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

    public Collection getAttributeDefinitions() {
        return attributeDefinitions.values();
    }

    public void setAttributeDefinitions(Map attributeDefinitions) {
        this.attributeDefinitions = attributeDefinitions;
    }

    public void removeAttributeDefinitions() {
        attributeDefinitions.clear();
    }

    public Collection getRelationships() {
        return relationships;
    }

    public void setRelationships(Collection relationships) {
        this.relationships = relationships;
    }

    public Collection getSources() {
        return sources;
    }

    public Collection getObjectClasses() {
        return objectClasses;
    }

    public void setObjectClasses(Collection objectClasses) {
        this.objectClasses = objectClasses;
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

    public void addChildDefinition(MappingRule mappingRule) {
        childDefinitions.add(mappingRule);
    }
    
    public void addSource(Source source) {
        sources.add(source);
    }

    public int getSourceIndex(Source source) {
        return sources.indexOf(source);
    }

    public void setSourceIndex(Source source, int index) {
        sources.remove(source);
        sources.add(index, source);
    }

    public void removeSources() {
        sources.clear();
    }

    public Source getSource(String name) {
        for (Iterator i=sources.iterator(); i.hasNext(); ) {
            Source source = (Source)i.next();
            if (name.equals(source.getName())) return source;
        }
        return null;
    }

    public Source removeSource(String name) {
        Source source = getSource(name);
        if (source != null) {
            sources.remove(source);
        }
        return source;
    }

	public void addAttributeDefinition(AttributeDefinition attribute) {
		attributeDefinitions.put(attribute.getName(), attribute);
	}

    public AttributeDefinition getAttributeDefinition(String name) {
        return (AttributeDefinition)attributeDefinitions.get(name);
    }

    public Collection getAttributeDefinitions(Collection names) {
        Collection results = new ArrayList();
        for (Iterator i=names.iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            AttributeDefinition attributeDefinition = (AttributeDefinition)attributeDefinitions.get(name);
            if (attributeDefinition == null) continue;
            results.add(attributeDefinition);
        }
        return results;
    }

    public AttributeDefinition removeAttributeDefinition(String name) {
        return (AttributeDefinition)attributeDefinitions.remove(name);
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
    
    public LDAPAttributeSet getAttributeSet(AttributeValues attributeValues) {
        LDAPAttributeSet set = new LDAPAttributeSet();

        for (Iterator i=attributeValues.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Collection values = attributeValues.get(name);

            LDAPAttribute attribute = new LDAPAttribute(name);
            for (Iterator j=values.iterator(); j.hasNext(); ) {
                Object value = j.next();
                attribute.addValue(value.toString());
            }

            set.add(attribute);
        }

        LDAPAttribute attribute = new LDAPAttribute("objectClass");

        for (Iterator i=objectClasses.iterator(); i.hasNext(); ) {
            String objectClass = (String)i.next();
            attribute.addValue(objectClass);
        }

        set.add(attribute);

        return set;
    }

    public LDAPEntry toLDAPEntry(String dn, AttributeValues attributeValues) {
        return new LDAPEntry(dn, getAttributeSet(attributeValues));
    }

    public AttributeValues getAttributeValues(Interpreter interpreter) throws Exception {

        AttributeValues values = new AttributeValues();

        for (Iterator i=attributeDefinitions.values().iterator(); i.hasNext(); ) {
            AttributeDefinition attribute = (AttributeDefinition)i.next();

            String name = attribute.getName();
            Object value = interpreter.eval(attribute.getExpression());
            if (value == null) continue;
            
            Collection set = values.get(name);
            if (set == null) set = new HashSet();
            set.add(value);
            values.set(name, set);
        }

        return values;
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

    public Collection getChildDefinitions() {
        return childDefinitions;
    }

    public void setChildDefinitions(List childDefinitions) {
        this.childDefinitions = childDefinitions;
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

    public void copy(EntryDefinition entry) {
        rdn = entry.rdn;
        parentDn = entry.parentDn;
        script = entry.script;

        removeObjectClasses();
        for (Iterator i=entry.objectClasses.iterator(); i.hasNext(); ) {
            String objectClass = (String)i.next();
            addObjectClass(objectClass);
        }

        removeAttributeDefinitions();
        for (Iterator i=entry.attributeDefinitions.values().iterator(); i.hasNext(); ) {
            AttributeDefinition attribute = (AttributeDefinition)i.next();
            addAttributeDefinition((AttributeDefinition)attribute.clone());
        }

        removeSources();
        for (Iterator i=entry.sources.iterator(); i.hasNext(); ) {
            Source source = (Source)i.next();
            addSource((Source)source.clone());
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

    public Object clone() {
        EntryDefinition entry = new EntryDefinition();
        entry.copy(this);
        return entry;
    }

    public int hashCode() {
        return (rdn == null ? 0 : rdn.hashCode()) +
                (parentDn == null ? 0 : parentDn.hashCode()) +
                (objectClasses == null ? 0 : objectClasses.hashCode()) +
                (script == null ? 0 : script.hashCode()) +
                (attributeDefinitions == null ? 0 : attributeDefinitions.hashCode()) +
                (sources == null ? 0 : sources.hashCode()) +
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

        EntryDefinition entryDefinition = (EntryDefinition)object;
        if (!equals(rdn, entryDefinition.rdn)) return false;
        if (!equals(parentDn, entryDefinition.parentDn)) return false;
        if (!equals(objectClasses, entryDefinition.objectClasses)) return false;
        if (!equals(script, entryDefinition.script)) return false;
        if (!equals(attributeDefinitions, entryDefinition.attributeDefinitions)) return false;
        if (!equals(sources, entryDefinition.sources)) return false;
        if (!equals(relationships, entryDefinition.relationships)) return false;
        if (!equals(acl, entryDefinition.acl)) return false;
        if (!equals(parameters, entryDefinition.parameters)) return false;

        return true;
    }

    public String toString() {
    	StringBuffer sb = new StringBuffer();
    	Iterator iter = null;

    	sb.append("dn="+rdn);
        if (parentDn != null) {
            sb.append(","+parentDn);
        }
        sb.append(",");

    	sb.append("objectClasses=[");
    	iter = objectClasses.iterator();
    	while (iter.hasNext()) {
    		Object next = (Object) iter.next();
    		sb.append(next.toString()+", ");
    	}
    	sb.append("], ");

    	sb.append("objectClasses=[");
    	iter = objectClasses.iterator();
    	while (iter.hasNext()) {
    		Object next = (Object) iter.next();
    		sb.append(next.toString()+", ");
    	}
    	sb.append("], ");

    	sb.append("attributes=[");
    	iter = attributeDefinitions.keySet().iterator();
    	while (iter.hasNext()) {
    		Object next = (Object) iter.next();
    		Object val  = (Object) attributeDefinitions.get(next);
    		sb.append(next.toString()+"="+val+", ");
    	}
    	sb.append("], ");

    	sb.append("sources=[");
    	iter = sources.iterator();
    	while (iter.hasNext()) {
    		Object next = (Object) iter.next();
    		sb.append(next.toString()+", ");
    	}
    	sb.append("], ");

    	sb.append("relationships=[");
    	iter = relationships.iterator();
    	while (iter.hasNext()) {
    		Object next = (Object) iter.next();
    		sb.append(next.toString()+", ");
    	}
    	sb.append("], ");

    	return sb.toString();
    }

}
