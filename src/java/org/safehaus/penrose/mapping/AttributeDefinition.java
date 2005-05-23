/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.mapping;

import java.io.Serializable;

/**
 * @author Endi S. Dewata
 */
public class AttributeDefinition implements Cloneable, Serializable {

	/**
	 * Name. This refers to AttributeType's name.
	 */
	private String name;
	
	/**
	 * Values.
	 */
	private String expression;

    /**
     * This attribute is used in RDN.
     */
    private boolean rdn;

    /**
     * Encryption method used to encrypt the value
     */
    private String encryption;

    /**
     * Encoding method used to encode the value
     */
    private String encoding;

    public AttributeDefinition() {
    }

    public AttributeDefinition(String name) {
        this(name, null, false);
    }

    public AttributeDefinition(String name, String expression) {
        this(name, expression, false);
    }

    public AttributeDefinition(String name, String value, boolean rdn) {
        this.name = name;
        this.expression = value;
        this.rdn = rdn;
    }

	public String getName() {
		return name;
	}

    public void setName(String name) {
        this.name = name;
    }

    public String getExpression() {
        return expression;
    }

    public void setExpression(String expression) {
        this.expression = expression;
    }

    public boolean isRdn() {
        return rdn;
    }

    public void setRdn(boolean rdn) {
        this.rdn = rdn;
    }

    public void setRdn(String rdn) {
    	this.rdn = Boolean.getBoolean(rdn);
    }

    public String toString() {
        return "["+name+":"+expression+"]";
    }
    
    public Object clone() {
        AttributeDefinition attribute = new AttributeDefinition(name, expression);
        attribute.setRdn(rdn);
        attribute.setEncryption(encryption);
        attribute.setEncoding(encoding);
        return attribute;
    }

    public boolean isConstant() {
        return getConstant() != null;
    }

    public String getConstant() {
        if (expression == null || "".equals(expression.trim())) return null;
        if (expression.length() < 2) return null;
        if (!expression.startsWith("\"")) return null;
        if (!expression.endsWith("\"")) return null;
        String constant = expression.substring(1, expression.length()-1);
        if (constant.indexOf("\"") >= 0) return null;
        return constant;
    }

    public String getEncryption() {
        return encryption;
    }

    public void setEncryption(String encryption) {
        this.encryption = encryption;
    }

    public String getEncoding() {
        return encoding;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }
}
