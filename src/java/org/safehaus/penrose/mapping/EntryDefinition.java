/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.mapping;


import org.safehaus.penrose.interpreter.Interpreter;
import org.ietf.ldap.LDAPEntry;
import org.ietf.ldap.LDAPAttributeSet;
import org.ietf.ldap.LDAPAttribute;

import java.util.*;
import java.io.Serializable;

/**
 * @author Endi S. Dewata
 */
public class EntryDefinition implements Cloneable, Serializable {

	/**
	 * Parent entry. If this is a root entry, parent is null.
	 */
    private EntryDefinition parent;
    
    /**
     * Distinguished name.
     */
	private String dn;
	
	/**
	 * Children. Each element is of type org.safehaus.penrose.mapping.EntryDefinition.
	 */
    private List children = new ArrayList();

    /**
     * Object classes. Each element is of type String.
     */
    private List objectClasses = new ArrayList();
    
    /**
     * Attributes. The keys are the attribute names (java.lang.String). Each value is of type org.safehaus.penrose.mapping.AttributeDefinition.
     */
    private Map attributes = new LinkedHashMap();

    /**
     * Sources. Each element is of type org.safehaus.penrose.mapping.Source.
     */
    private Map sources = new LinkedHashMap();
    
    /**
     * Relationship. Each element is of type org.safehaus.penrose.mapping.Relationship.
     */
    private List relationships = new ArrayList();

    private String script;

	public EntryDefinition() {
	}

    public EntryDefinition(EntryDefinition parent) {
        this.parent = parent;
    }

	public EntryDefinition(String dn) {
        this.parent = null;
        this.dn = dn;
    }

    public EntryDefinition(String rdn, EntryDefinition parent) {
        this.dn = rdn+","+parent.getDn();
        this.parent = parent;
    }

    public String getRdn() {
        int i = dn.indexOf(",");
        return dn.substring(0, i);
    }
    
    public String getParentDn() {
        int i = dn.indexOf(",");
        String rdn = dn.substring(0, i);
        String parentDn = dn.substring(i+1);
        return parentDn;
    }

    public boolean isDynamic() {
        int i = dn.indexOf(",");
        String rdn = dn.substring(0, i);
        String parentDn = dn.substring(i+1);

        return rdn.indexOf("...") >= 0;
    }
    
    public void setMapping(boolean mapping) {
        int i = dn.indexOf(",");
        String rdn = dn.substring(0, i);
        String parentDn = dn.substring(i+1);
    	if (mapping && !isDynamic()) {
    		int j = rdn.indexOf("=")+1;
    		rdn = rdn.substring(0, j) + "...";
    	} else if (!mapping && isDynamic()) {
    		rdn = rdn.replaceAll("\\.\\.\\.", "value");
    	}
    	dn = rdn + "," + parentDn;
    }

    public String getDn(AttributeValues attributes) {
        if (isDynamic()) {
            Collection rdnAttributes = getRdnAttributes();

            // TODO fix for multiple rdn attributes
            AttributeDefinition rdnAttribute = (AttributeDefinition)rdnAttributes.iterator().next();

            // TODO fix for multiple values
            Collection rdnValues = attributes.get(rdnAttribute.getName());
            Object rdnValue = rdnValues.iterator().next();

            // TODO fix if parent is also a mapping entry
            return rdnAttribute.getName()+"="+rdnValue+","+parent.getDn();

        } else {
            return getDn();
        }
    }

    public Collection getRdnAttributes() {
        List results = new ArrayList();
        for (Iterator i=attributes.values().iterator(); i.hasNext(); ) {
            AttributeDefinition attribute = (AttributeDefinition)i.next();
            if (!attribute.isRdn()) continue;
            results.add(attribute);
        }
        return results;
    }

    public String getDn() {
        return dn;
    }

    public void setDn(String dn) {
        this.dn = dn;
    }

    public Map getAttributes() {
        return attributes;
    }

    public void setAttributes(Map attributes) {
        this.attributes = attributes;
    }

    public EntryDefinition getParent() {
        return parent;
    }

    public void setParent(EntryDefinition parent) {
        this.parent = parent;
    }

    public Collection getChildren() {
        return children;
    }

    public void setChildren(List children) {
        this.children = children;
    }

    public Collection getRelationships() {
        return relationships;
    }

    public void setRelationships(List relationships) {
        this.relationships = relationships;
    }

    public Collection getSources() {
        return sources.values();
    }

    public List getObjectClasses() {
        return objectClasses;
    }

    public void setObjectClasses(List objectClasses) {
        this.objectClasses = objectClasses;
    }

    ///////////
    

	public void addObjectClass(String oc) {
		objectClasses.add(oc);
	}

	public void addChild(EntryDefinition child) {
		child.setParent(this);
		children.add(child);
	}

    public void addSource(Source source) {
        sources.put(source.getName(), source);
    }

    public Source getSource(String name) {
        return (Source)sources.get(name);
    }

    public Source removeSource(String name) {
        return (Source)sources.remove(name);
    }

	public void addAttributeDefinition(AttributeDefinition attribute) {
		attributes.put(attribute.getName(), attribute);
	}
	
	public void addRelationship(Relationship relationship) {
		relationships.add(relationship);
	}

    public Object clone() {
        EntryDefinition entry = new EntryDefinition();
        entry.setParent(parent);
        entry.setDn(dn);
        entry.getChildren().addAll(children);
        entry.getObjectClasses().addAll(objectClasses);

        Map a = entry.getAttributes();
        for (Iterator i=attributes.keySet().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            AttributeDefinition attribute = (AttributeDefinition)attributes.get(name);
            a.put(name, attribute.clone());
        }

        for (Iterator i=sources.values().iterator(); i.hasNext(); ) {
            Source source = (Source)i.next();
            entry.addSource((Source)source.clone());
        }

        Collection r = entry.getRelationships();
        for (Iterator i=relationships.iterator(); i.hasNext(); ) {
            Relationship relationship = (Relationship)i.next();
            r.add(relationship.clone());
        }

        return entry;
    }

    public void normalize() {
        List list = new ArrayList();
        List rdns = new ArrayList();

        for (Iterator i=attributes.keySet().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            AttributeDefinition attribute = (AttributeDefinition)attributes.get(name);

            if (attribute.getExpression() == null) { // remove empty attributes
                list.add(name);
                continue;
            }

            if (attribute.isRdn()) {
                rdns.add(attribute);
            }
        }

        for (Iterator i=list.iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            attributes.remove(name);
        }

        StringBuffer sb = new StringBuffer();
        for (Iterator i=rdns.iterator(); i.hasNext(); ) {
            AttributeDefinition attribute = (AttributeDefinition)i.next();
            String name = attribute.getName();
            String value = attribute.getConstant();
            if (value == null) value = "...";

            if (sb.length() > 0) sb.append("+");

            sb.append(name);
            sb.append("=");
            sb.append(value);
        }

        if (parent != null) {
            sb.append(",");
            sb.append(parent.getDn());
        }

        setDn(sb.toString());
    }

    public String toString() {
    	StringBuffer sb = new StringBuffer();
    	Iterator iter = null;

    	sb.append("dn="+dn+", ");

    	sb.append("children=[");
    	iter = children.iterator();
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
		
    	sb.append("objectClasses=[");
    	iter = objectClasses.iterator();
    	while (iter.hasNext()) {
    		Object next = (Object) iter.next();
    		sb.append(next.toString()+", ");
    	}
    	sb.append("], ");

    	sb.append("attributes=[");
    	iter = attributes.keySet().iterator();
    	while (iter.hasNext()) {
    		Object next = (Object) iter.next();
    		Object val  = (Object) attributes.get(next); 
    		sb.append(next.toString()+"="+val+", ");
    	}
    	sb.append("], ");

    	sb.append("sources=[");
    	iter = sources.values().iterator();
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
    
    public LDAPAttributeSet getAttributeSet(AttributeValues attributeValues) {
        LDAPAttributeSet set = new LDAPAttributeSet();

        for (Iterator i=attributeValues.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Collection values = attributeValues.get(name);

            LDAPAttribute attribute = new LDAPAttribute(name);
            for (Iterator j=values.iterator(); j.hasNext(); ) {
                String value = (String)j.next();
                attribute.addValue(value);
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

    public LDAPEntry toLDAPEntry(AttributeValues attributeValues) {
        return new LDAPEntry(getDn(attributeValues), getAttributeSet(attributeValues));
    }

    public String toString(AttributeValues attributeValues) {

        StringBuffer sb = new StringBuffer();
        sb.append("dn: "+getDn(attributeValues)+"\n");

        for (Iterator i = objectClasses.iterator(); i.hasNext(); ) {
            String oc = (String)i.next();
            sb.append("objectClass: "+oc+"\n");
        }

        for (Iterator i = attributeValues.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Collection values = attributeValues.get(name);

            for (Iterator j = values.iterator(); j.hasNext(); ) {
                String value = (String)j.next();
                sb.append(name+": "+value+"\n");
            }
        }

        return sb.toString();
    }

    public AttributeValues getAttributeValues(Interpreter interpreter) throws Exception {

        AttributeValues values = new AttributeValues();

        for (Iterator i=attributes.values().iterator(); i.hasNext(); ) {
            AttributeDefinition attribute = (AttributeDefinition)i.next();

            String name = attribute.getName();
            Object value = interpreter.eval(attribute.getExpression());

            Collection set = values.get(name);
            if (set == null) {
                set = new HashSet();
                values.set(name, set);
            }

            set.add(value);
        }

        return values;
    }

    public String getScript() {
        return script;
    }

    public void setScript(String script) {
        this.script = script;
    }
}