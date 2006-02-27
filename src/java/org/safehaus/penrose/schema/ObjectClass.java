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
package org.safehaus.penrose.schema;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

public class ObjectClass {

    Logger log = Logger.getLogger(getClass());

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
	public Collection names = new ArrayList();
    
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
	public Collection superClasses = new ArrayList();
    
    /**
     * Type (ABSTRACT, STRUCTURAL, AUXILIARY). Default: STRUCTURAL.
     */
	public String type;

    /**
     * Required attribute types. Each element is of type AttributeType.
     */
	public Collection requiredAttributes = new ArrayList();
    
    /**
     * Optional attribute types. Each element is of type AttributeType.
     */
	public Collection optionalAttributes = new ArrayList();
	
	public ObjectClass() {
		super();
	}

    public ObjectClass(String name, String superClass, String description) {
        this.names.add(name);
        this.superClasses.add(superClass);
        this.description = description;
    }

    public ObjectClass(Collection names, Collection superClasses, String description) {
        this.names.addAll(names);
        this.superClasses.addAll(superClasses);
        this.description = description;
    }
    
    public String getName() {
    	if (names != null && names.size() >= 1) return names.iterator().next().toString();
    	return null;
    }
    
    public void setName(String name) {
    	if (names == null) names = new ArrayList();
    	names.clear();
    	names.add(name);
    }

    public Collection getNames() {
        return names;
    }

    public void setNames(Collection names) {
        this.names = names;
    }

    public Collection getSuperClasses() {
        return superClasses;
    }

    public void setSuperClasses(Collection superClasses) {
        this.superClasses = superClasses;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Collection getRequiredAttributes() {
        return requiredAttributes;
    }

    public void setRequiredAttributes(Collection requiredAttributes) {
        this.requiredAttributes = requiredAttributes;
    }

    public Collection getOptionalAttributes() {
        return optionalAttributes;
    }

    public void setOptionalAttributes(Collection optionalAttributes) {
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
	
    public boolean containsRequiredAttribute(String name) {
        name = name.toLowerCase();

        for (Iterator i=requiredAttributes.iterator(); i.hasNext(); ) {
            String attrName = (String)i.next();
            if (name.equals(attrName.toLowerCase())) return true;
        }

        return false;
    }

    public boolean containsOptionalAttribute(String name) {
        name = name.toLowerCase();

        for (Iterator i=optionalAttributes.iterator(); i.hasNext(); ) {
            String attrName = (String)i.next();
            if (name.equals(attrName.toLowerCase())) return true;
        }

        return false;
    }

	public String toString() {
		return "ObjectClass<"+getName()+">";
	}

    public int hashCode() {
        //log.debug("hashCode()");
        return oid.hashCode();
    }

    boolean compare(Object o1, Object o2) {
        if (o1 == null && o2 == null) return true;
        if (o1 != null) return o1.equals(o2);
        return o2.equals(o1);
    }

    public boolean equals(Object object) {
        //log.debug("equals("+object+")");
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
