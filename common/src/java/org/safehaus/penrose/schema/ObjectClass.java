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
package org.safehaus.penrose.schema;

import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

public class ObjectClass implements Serializable, Cloneable, Comparable {

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
	public Collection<String> names = new ArrayList<String>();
    
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
	public Collection<String> superClasses = new ArrayList<String>();
    
    /**
     * Type (ABSTRACT, STRUCTURAL, AUXILIARY). Default: STRUCTURAL.
     */
	public String type = STRUCTURAL;

    /**
     * Required attribute types. Each element is of type String.
     */
	public Collection<String> requiredAttributes = new ArrayList<String>();
    public Collection<String> normalizedRequiredAttributes = new HashSet<String>();

    /**
     * Optional attribute types. Each element is of type String.
     */
	public Collection<String> optionalAttributes = new ArrayList<String>();
    public Collection<String> normalizedOptionalAttributes = new HashSet<String>();

	public ObjectClass() {
	}

    public ObjectClass(String name, String superClass, String description) {
        this.names.add(name);
        this.superClasses.add(superClass);
        this.description = description;
    }

    public ObjectClass(Collection<String> names, Collection<String> superClasses, String description) {
        this.names.addAll(names);
        this.superClasses.addAll(superClasses);
        this.description = description;
    }
    
    public String getName() {
    	if (names != null && names.size() >= 1) return names.iterator().next();
    	return null;
    }
    
    public void setName(String name) {
    	names.clear();
    	names.add(name);
    }

    public void addName(String name) {
        names.add(name);
    }

    public Collection<String> getNames() {
        return names;
    }

    public void setNames(Collection<String> names) {
        if (this.names == names) return;
        this.names.clear();
        this.names.addAll(names);
    }

    public void removeNames() {
        names.clear();
    }

    public void addSuperClass(String superClass) {
        superClasses.add(superClass);
    }

    public Collection<String> getSuperClasses() {
        return superClasses;
    }

    public void setSuperClasses(Collection<String> superClasses) {
        if (this.superClasses == superClasses) return;
        this.superClasses.clear();
        this.superClasses.addAll(superClasses);
    }

    public void removeSuperClasses() {
        superClasses.clear();
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Collection<String> getRequiredAttributes() {
        return requiredAttributes;
    }

    public void addRequiredAttribute(String requiredAttribute) {
        requiredAttributes.add(requiredAttribute);
        normalizedRequiredAttributes.add(requiredAttribute.toLowerCase());
    }
    
    public void addRequiredAttributes(Collection<String> requiredAttributes) {
        if (requiredAttributes == null) return;
        for (String requiredAttribute : requiredAttributes) {
            addRequiredAttribute(requiredAttribute);
        }
    }

    public void setRequiredAttributes(Collection<String> requiredAttributes) {
        if (this.requiredAttributes == requiredAttributes) return;
        removeRequiredAttributes();
        addRequiredAttributes(requiredAttributes);
    }

    public void removeRequiredAttribute(String requiredAttribute) {
        requiredAttributes.remove(requiredAttribute);
        normalizedRequiredAttributes.remove(requiredAttribute.toLowerCase());
    }

    public void removeRequiredAttributes() {
        requiredAttributes.clear();
        normalizedRequiredAttributes.clear();
    }
    
    public Collection<String> getOptionalAttributes() {
        return optionalAttributes;
    }

    public void addOptionalAttribute(String optionalAttribute) {
        optionalAttributes.add(optionalAttribute);
        normalizedOptionalAttributes.add(optionalAttribute.toLowerCase());
    }

    public void addOptionalAttributes(Collection<String> optionalAttributes) {
        if (optionalAttributes == null) return;
        for (String optionalAttribute : optionalAttributes) {
            addOptionalAttribute(optionalAttribute);
        }
    }

    public void setOptionalAttributes(Collection<String> optionalAttributes) {
        if (this.optionalAttributes == optionalAttributes) return;
        removeOptionalAttributes();
        addOptionalAttributes(optionalAttributes);
    }

    public void removeOptionalAttribute(String optionalAttribute) {
        optionalAttributes.remove(optionalAttribute);
        normalizedOptionalAttributes.remove(optionalAttribute.toLowerCase());
    }

    public void removeOptionalAttributes() {
        optionalAttributes.clear();
        normalizedOptionalAttributes.clear();
    }

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
        return normalizedRequiredAttributes.contains(name.toLowerCase());
    }

    public boolean containsOptionalAttribute(String name) {
        return normalizedOptionalAttributes.contains(name.toLowerCase());
    }

    public int hashCode() {
        return oid == null ? 0 : oid.hashCode();
    }

    boolean compare(Object o1, Object o2) {
        if (o1 == null && o2 == null) return true;
        if (o1 != null) return o1.equals(o2);
        return o2.equals(o1);
    }

    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null) return false;
        if (object.getClass() != this.getClass()) return false;

        ObjectClass oc = (ObjectClass)object;
        if (!compare(oid, oc.oid)) return false;
        if (!compare(names, oc.names)) return false;
        if (!compare(description, oc.description)) return false;
        if (obsolete != oc.obsolete) return false;
        if (!compare(superClasses, oc.superClasses)) return false;
        if (!compare(type, oc.type)) return false;
        if (!compare(requiredAttributes, oc.requiredAttributes)) return false;
        if (!compare(optionalAttributes, oc.optionalAttributes)) return false;

        return true;
    }

    boolean equals(Object o1, Object o2) {
        if (o1 == null && o2 == null) return true;
        if (o1 != null) return o1.equals(o2);
        return o2.equals(o1);
    }

    public Object clone() throws CloneNotSupportedException {
        ObjectClass oc = (ObjectClass)super.clone();

        oc.oid = oid;

        oc.names = new ArrayList<String>();
        oc.names.addAll(names);

        oc.description = description;
        oc.obsolete = obsolete;

        oc.superClasses = new ArrayList<String>();
        oc.superClasses.addAll(superClasses);

        oc.type = type;

        oc.requiredAttributes = new ArrayList<String>();
        oc.requiredAttributes.addAll(requiredAttributes);

        oc.normalizedRequiredAttributes = new HashSet<String>();
        oc.normalizedRequiredAttributes.addAll(normalizedRequiredAttributes);

        oc.optionalAttributes = new ArrayList<String>();
        oc.optionalAttributes.addAll(optionalAttributes);

        oc.normalizedOptionalAttributes = new HashSet<String>();
        oc.normalizedOptionalAttributes.addAll(normalizedOptionalAttributes);

        return oc;
    }

    public int compareTo(Object object) {
        if (!(object instanceof ObjectClass)) return 0;

        ObjectClass oc = (ObjectClass)object;
        return oid.compareTo(oc.getOid());
    }

    public String toString() {
        return toString(false);
    }

    public String toString(boolean multiLine) {
        StringWriter sw = new StringWriter();
        PrintWriter out = new PrintWriter(sw);

        out.print(oid);
        if (multiLine) out.println();

        if (names.size() == 1) {
            if (multiLine) out.print("   ");
            out.print(" NAME '"+names.iterator().next()+"'");
            if (multiLine) out.println();

        } else if (names.size() > 1) {
            if (multiLine) out.print("   ");
            out.print(" NAME ( ");
            for (String name : names) {
                out.print("'" + name + "' ");
            }
            out.print(")");
            if (multiLine) out.println();
        }

        if (description != null) {
            if (multiLine) out.print("   ");
            out.print(" DESC '"+escape(description)+"'");
            if (multiLine) out.println();
        }

        if (obsolete) {
            if (multiLine) out.print("   ");
            out.print(" OBSOLETE");
            if (multiLine) out.println();
        }

        if (superClasses.size() == 1) {
            if (multiLine) out.print("   ");
            out.print(" SUP "+superClasses.iterator().next());
            if (multiLine) out.println();

        } else if (superClasses.size() > 1) {
            if (multiLine) out.print("   ");
            out.print(" SUP ( ");
            for (Iterator i=superClasses.iterator(); i.hasNext(); ) {
                String name = (String)i.next();
                out.print(name);
                if (i.hasNext()) out.print(" $ ");
            }
            out.print(" )");
            if (multiLine) out.println();
        }

        if (!STRUCTURAL.equals(type)) {
            if (multiLine) out.print("   ");
            out.print(" "+type);
            if (multiLine) out.println();
        }

        if (requiredAttributes.size() == 1) {
            if (multiLine) out.print("   ");
            out.print(" MUST "+requiredAttributes.iterator().next());
            if (multiLine) out.println();

        } else if (requiredAttributes.size() > 1) {
            if (multiLine) out.print("   ");
            out.print(" MUST ( ");
            for (Iterator i=requiredAttributes.iterator(); i.hasNext(); ) {
                String name = (String)i.next();
                out.print(name);
                if (i.hasNext()) out.print(" $ ");
            }
            out.print(" )");
            if (multiLine) out.println();
        }

        if (optionalAttributes.size() == 1) {
            if (multiLine) out.print("   ");
            out.print(" MAY "+optionalAttributes.iterator().next());
            if (multiLine) out.println();

        } else if (optionalAttributes.size() > 1) {
            if (multiLine) out.print("   ");
            out.print(" MAY ( ");
            for (Iterator i=optionalAttributes.iterator(); i.hasNext(); ) {
                String name = (String)i.next();
                out.print(name);
                if (i.hasNext()) out.print(" $ ");
            }
            out.print(" )");
            if (multiLine) out.println();
        }

        return sw.toString();
    }

    public static String escape(String s) {
        StringBuilder sb = new StringBuilder();

        for (int i=0; i<s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\'' || c == '\\') {
                sb.append('\\');
                sb.append(toHex(c));
            } else {
                sb.append(c);
            }
        }

        return sb.toString();
    }

    public static String toHex(char c) {
        String s = Integer.toHexString(c);
        return s.length() == 1 ? '0'+s : s;
    }
}
