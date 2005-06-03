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
public class AttributeType implements Serializable {

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
	public List names;
	
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
	 * User modifiable. Default: false.
	 */
	public boolean modifiable;
	
	/**
	 * Usage (userApplications, directoryOperation, distributedOperation, dSAOperation).
	 * Default: userApplications.
	 */
	public String usage;
	
	/**
	 * @return Returns the collective.
	 */
	public boolean isCollective() {
		return collective;
	}
	/**
	 * @param collective The collective to set.
	 */
	public void setCollective(boolean collective) {
		this.collective = collective;
	}
	/**
	 * @return Returns the description.
	 */
	public String getDescription() {
		return description;
	}
	/**
	 * @param description The description to set.
	 */
	public void setDescription(String description) {
		this.description = description;
	}
	/**
	 * @return Returns the equality.
	 */
	public String getEquality() {
		return equality;
	}
	/**
	 * @param equality The equality to set.
	 */
	public void setEquality(String equality) {
		this.equality = equality;
	}
	/**
	 * @return Returns the modifiable.
	 */
	public boolean isModifiable() {
		return modifiable;
	}
	/**
	 * @param modifiable The modifiable to set.
	 */
	public void setModifiable(boolean modifiable) {
		this.modifiable = modifiable;
	}
	/**
	 * get the name (first index)
	 * @return attribute name
	 */
    public String getName() {
    	if (names != null && names.size() >= 1) return names.get(0).toString();
    	return null;
    }
    /**
     * set name (first index)
     * @param name
     */
    public void setName(String name) {
    	if (names == null) names = new ArrayList();
    	names.clear();
    	names.add(name);
    }
	/**
	 * @return Returns the name.
	 */
	public List getNames() {
		return names;
	}
	/**
	 * @param names The name to set.
	 */
	public void setNames(List names) {
		this.names = names;
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
	 * @return Returns the ordering.
	 */
	public String getOrdering() {
		return ordering;
	}
	/**
	 * @param ordering The ordering to set.
	 */
	public void setOrdering(String ordering) {
		this.ordering = ordering;
	}
	/**
	 * @return Returns the singleValued.
	 */
	public boolean isSingleValued() {
		return singleValued;
	}
	/**
	 * @param singleValued The singleValued to set.
	 */
	public void setSingleValued(boolean singleValued) {
		this.singleValued = singleValued;
	}
	/**
	 * @return Returns the substring.
	 */
	public String getSubstring() {
		return substring;
	}
	/**
	 * @param substring The substring to set.
	 */
	public void setSubstring(String substring) {
		this.substring = substring;
	}
	/**
	 * @return Returns the superClass.
	 */
	public String getSuperClass() {
		return superClass;
	}
	/**
	 * @param superClass The superClass to set.
	 */
	public void setSuperClass(String superClass) {
		this.superClass = superClass;
	}
	/**
	 * @return Returns the syntax.
	 */
	public String getSyntax() {
		return syntax;
	}
	/**
	 * @param syntax The syntax to set.
	 */
	public void setSyntax(String syntax) {
		this.syntax = syntax;
	}
	/**
	 * @return Returns the usage.
	 */
	public String getUsage() {
		return usage;
	}
	/**
	 * @param usage The usage to set.
	 */
	public void setUsage(String usage) {
		this.usage = usage;
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
	
	public String toString() {
		return "AttributeType: names="+names+", description="+description+
		", singleValued="+singleValued+", collective="+collective+", modifiable="+modifiable+
		", substring="+substring+", superClass="+superClass+", syntax="+syntax+
		", usage="+usage+", obsolete="+obsolete+", equality="+equality;
	}
}