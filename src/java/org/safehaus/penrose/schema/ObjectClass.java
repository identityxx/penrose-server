/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
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
}
