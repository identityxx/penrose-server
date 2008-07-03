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

public class AttributeType implements Serializable, Cloneable, Comparable {

	public final static String USER_APPLICATIONS     = "userApplications";
	public final static String DIRECTORY_OPERATION   = "directoryOperation";
	public final static String DISTRIBUTED_OPERATION = "distributedOperation";
	public final static String DSA_OPERATION         = "dSAOperation";
	
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
	public String superClass;
	
	/**
	 * Equality OID.
	 */
	public String equality;
	
	/**
	 * Ordering OID.
	 */
	public String ordering;
	
	/**
	 * Substring OID.
	 */
	public String substring;
	
	/**
	 * Syntax OID.
	 */
	public String syntax;
	
	/**
	 * Single-valued. Default: false.
	 */
	public boolean singleValued;
	
	/**
	 * Collective. Default: false.
	 */
	public boolean collective;
	
	/**
	 * User modifiable. Default: true.
	 */
	public boolean modifiable = true;
	
	/**
	 * Usage (userApplications, directoryOperation, distributedOperation, dSAOperation).
	 * Default: userApplications.
	 */
	public String usage = USER_APPLICATIONS;
    public boolean operational;

    public boolean isCollective() {
		return collective;
	}

	public void setCollective(boolean collective) {
		this.collective = collective;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getEquality() {
		return equality;
	}

	public void setEquality(String equality) {
		this.equality = equality;
	}

	public boolean isModifiable() {
		return modifiable;
	}

	public void setModifiable(boolean modifiable) {
		this.modifiable = modifiable;
	}

    public String getName() {
    	if (names.isEmpty()) return null;
        return names.iterator().next();
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

	public String getOid() {
		return oid;
	}

	public void setOid(String oid) {
		this.oid = oid;
	}

	public String getOrdering() {
		return ordering;
	}

	public void setOrdering(String ordering) {
		this.ordering = ordering;
	}

	public boolean isSingleValued() {
		return singleValued;
	}

	public void setSingleValued(boolean singleValued) {
		this.singleValued = singleValued;
	}

	public String getSubstring() {
		return substring;
	}

	public void setSubstring(String substring) {
		this.substring = substring;
	}

	public String getSuperClass() {
		return superClass;
	}

	public void setSuperClass(String superClass) {
		this.superClass = superClass;
	}

	public String getSyntax() {
		return syntax;
	}

	public void setSyntax(String syntax) {
		this.syntax = syntax;
	}

	public String getUsage() {
		return usage;
	}

	public void setUsage(String usage) {
		this.usage = usage;
        operational = DIRECTORY_OPERATION.equals(usage);
    }

	public boolean isObsolete() {
		return obsolete;
	}

	public void setObsolete(boolean obsolete) {
		this.obsolete = obsolete;
	}

    public int hashCode() {
        return oid == null ? 0 : oid.hashCode();
    }

    boolean equals(Object o1, Object o2) {
        if (o1 == null && o2 == null) return true;
        if (o1 != null) return o1.equals(o2);
        return o2.equals(o1);
    }

    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null) return false;
        if (object.getClass() != this.getClass()) return false;

        AttributeType at = (AttributeType)object;
        if (!equals(oid, at.oid)) return false;
        if (!equals(names, at.names)) return false;
        if (!equals(description, at.description)) return false;
        if (obsolete != at.obsolete) return false;
        if (!equals(superClass, at.superClass)) return false;
        if (!equals(equality, at.equality)) return false;
        if (!equals(ordering, at.ordering)) return false;
        if (!equals(substring, at.substring)) return false;
        if (!equals(syntax, at.syntax)) return false;
        if (singleValued != at.singleValued) return false;
        if (collective != at.collective) return false;
        if (modifiable != at.modifiable) return false;
        if (!equals(usage, at.usage)) return false;

        return true;
    }

    public void copy(AttributeType at) {
        oid = at.oid;

        names = new ArrayList<String>();
        names.addAll(at.names);

        description = at.description;
        obsolete = at.obsolete;
        superClass = at.superClass;
        equality = at.equality;
        ordering = at.ordering;
        substring = at.substring;
        syntax = at.syntax;
        singleValued = at.singleValued;
        collective = at.collective;
        modifiable = at.modifiable;
        usage = at.usage;
        operational = at.operational;
    }

    public Object clone() throws CloneNotSupportedException {
        AttributeType at = (AttributeType)super.clone();
        at.copy(this);
        return at;
    }

    public int compareTo(Object object) {
        if (!(object instanceof AttributeType)) return 0;

        AttributeType at = (AttributeType)object;
        return oid.compareTo(at.getOid());
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

        if (superClass != null) {
            if (multiLine) out.print("   ");
            out.print(" SUP "+superClass);
            if (multiLine) out.println();
        }

        if (equality != null) {
            if (multiLine) out.print("   ");
            out.print(" EQUALITY "+equality);
            if (multiLine) out.println();
        }

        if (ordering != null) {
            if (multiLine) out.print("   ");
            out.print(" ORDERING "+ordering);
            if (multiLine) out.println();
        }

        if (substring != null) {
            if (multiLine) out.print("   ");
            out.print(" SUBSTR "+substring);
            if (multiLine) out.println();
        }

        if (syntax != null) {
            if (multiLine) out.print("   ");
            out.print(" SYNTAX "+syntax);
            if (multiLine) out.println();
        }

        if (singleValued) {
            if (multiLine) out.print("   ");
            out.print(" SINGLE-VALUE");
            if (multiLine) out.println();
        }

        if (collective) {
            if (multiLine) out.print("   ");
            out.print(" COLLECTIVE");
            if (multiLine) out.println();
        }

        if (!modifiable) {
            if (multiLine) out.print("   ");
            out.print(" NO-USER-MODIFICATION");
            if (multiLine) out.println();
        }

        if (usage != null && !USER_APPLICATIONS.equals(usage)) {
            if (multiLine) out.print("   ");
            out.print(" USAGE "+usage);
            if (multiLine) out.println();
        }

        out.close();

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

    public boolean isOperational() {
        return operational;
    }

    public void setOperational(boolean operational) {
        this.operational = operational;
    }
}
