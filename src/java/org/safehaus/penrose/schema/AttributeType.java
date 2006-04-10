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

import java.util.ArrayList;
import java.util.Collection;

public class AttributeType implements Cloneable, Comparable {

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
        return (String)names.iterator().next();
    }

    public void setName(String name) {
    	names.clear();
    	names.add(name);
    }

    public void addName(String name) {
        names.add(name);
    }

	public Collection getNames() {
		return names;
	}

	public void setNames(Collection names) {
		this.names = names;
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
	}

	public boolean isObsolete() {
		return obsolete;
	}

	public void setObsolete(boolean obsolete) {
		this.obsolete = obsolete;
	}

    public int hashCode() {
        return (oid == null ? 0 : oid.hashCode()) +
                (names == null ? 0 : names.hashCode()) +
                (description == null ? 0 : description.hashCode()) +
                (obsolete ? 0 : 1) +
                (superClass == null ? 0 : superClass.hashCode()) +
                (equality == null ? 0 : equality.hashCode()) +
                (ordering == null ? 0 : ordering.hashCode()) +
                (substring == null ? 0 : substring.hashCode()) +
                (syntax == null ? 0 : syntax.hashCode()) +
                (singleValued ? 0 : 1) +
                (collective ? 0 : 1) +
                (modifiable ? 0 : 1) +
                (usage == null ? 0 : usage.hashCode());
    }

    boolean equals(Object o1, Object o2) {
        if (o1 == null && o2 == null) return true;
        if (o1 != null) return o1.equals(o2);
        return o2.equals(o1);
    }

    public boolean equals(Object object) {
        if (this == object) return true;
        if ((object == null) || (object.getClass() != this.getClass())) return false;

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

        names.clear();
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
    }

    public Object clone() {
        AttributeType at = new AttributeType();
        at.copy(this);
        return at;
    }

    public int compareTo(Object object) {
        if (!(object instanceof AttributeType)) return 0;

        AttributeType at = (AttributeType)object;
        return oid.compareTo(at.getOid());
    }
}
