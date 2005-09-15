/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 * All rights reserved.
 */
package org.safehaus.penrose.schema;

import java.util.ArrayList;
import java.util.List;
import java.io.Serializable;

/**
 * @author Endi S. Dewata
 * @author Adison Wongkar
 */
public class ObjectClass implements Serializable {
	
	public final static String ABSTRACT   = "ABSTRACT";
	public final static String STRUCTURAL = "STRUCTURAL";
	public final static String AUXILIARY  = "AUXILIARY";
	
    /**
     * Identifier.
     */
	public String oid;
    
	/**
	 * Name.
	 */
	public List names = new ArrayList();
    
    /**
     * Description.
     */
	public String description;
    
    /**
     * Obsolete.
     */
	public boolean obsolete;
    
    /**
     * Super class.
     */
	public List superClasses = new ArrayList();
    
    /**
     * Type (ABSTRACT, STRUCTURAL, AUXILIARY). Default: STRUCTURAL.
     */
	public String type;

    /**
     * Required attribute types. Each element is of type AttributeType.
     */
	public List requiredAttributes = new ArrayList();
    
    /**
     * Optional attribute types. Each element is of type AttributeType.
     */
	public List optionalAttributes = new ArrayList();
	
	public ObjectClass() {
		super();
	}

    public ObjectClass(String name, String superClass, String description) {
        this.names.add(name);
        this.superClasses.add(superClass);
        this.description = description;
    }

    public ObjectClass(List names, List superClasses, String description) {
        this.names.addAll(names);
        this.superClasses.addAll(superClasses);
        this.description = description;
    }
    
    public String getName() {
    	if (names != null && names.size() >= 1) return names.get(0).toString();
    	return null;
    }
    
    public void setName(String name) {
    	if (names == null) names = new ArrayList();
    	names.clear();
    	names.add(name);
    }

    public List getNames() {
        return names;
    }

    public void setNames(List names) {
        this.names = names;
    }

    public List getSuperClasses() {
        return superClasses;
    }

    public void setSuperClasses(List superClasses) {
        this.superClasses = superClasses;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public List getRequiredAttributes() {
        return requiredAttributes;
    }

    public void setRequiredAttributes(List requiredAttributes) {
        this.requiredAttributes = requiredAttributes;
    }

    public List getOptionalAttributes() {
        return optionalAttributes;
    }

    public void setOptionalAttributes(List optionalAttributes) {
        this.optionalAttributes = optionalAttributes;
    }
	/**
	 * @return Returns the obsolete.
	 */
	public boolean isObsolete() {
		return obsolete;
	}
	/**
	 * @param obsolete The obsolete to set.
	 */
	public void setObsolete(boolean obsolete) {
		this.obsolete = obsolete;
	}
	/**
	 * @return Returns the oid.
	 */
	public String getOid() {
		return oid;
	}
	/**
	 * @param oid The oid to set.
	 */
	public void setOid(String oid) {
		this.oid = oid;
	}
	/**
	 * @return Returns the type.
	 */
	public String getType() {
		return type;
	}
	/**
	 * @param type The type to set.
	 */
	public void setType(String type) {
		this.type = type;
	}
	
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("ObjectClass: name="+getName()+", oid="+oid+", type="+type+", obsolete="+obsolete+", description="+description+
				", superClasses="+superClasses+", requiredAttributes="+requiredAttributes+", optionalAttributes="+optionalAttributes);
		return sb.toString();
	}

    public int hashCode() {
        return oid.hashCode();
    }

    boolean compare(Object o1, Object o2) {
        if (o1 == null && o2 == null) return true;
        if (o1 != null) return o1.equals(o2);
        return o2.equals(o1);
    }

    public boolean equals(Object object) {
        if (object == null) return false;
        if (!(object instanceof ObjectClass)) return false;

        ObjectClass oc = (ObjectClass)object;
        if (!compare(oid, oc.oid)) return false;
        if (!compare(names, oc.names)) return false;
        if (!compare(description, oc.description)) return false;
        if (obsolete != oc.obsolete) return false;
        if (!compare(superClasses, oc.superClasses)) return false;
        if (!compare(type, oc.type)) return false;

        return true;
    }
}
