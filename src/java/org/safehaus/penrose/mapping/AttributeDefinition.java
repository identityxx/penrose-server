/**
 * Copyright (c) 2000-2005, Identyx Corporation.
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
     * Script.
     */
    private String script;

	/**
	 * Values.
	 */
    private Expression expression;

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

    public AttributeDefinition(String name, String expression) {
        this(name, expression, false);
    }

    public AttributeDefinition(String name, Expression expression) {
        this(name, expression, false);
    }

    public AttributeDefinition(String name, String value, boolean rdn) {
        this.name = name;
        this.expression = new Expression(value);
        this.rdn = rdn;
    }

    public AttributeDefinition(String name, Expression expression, boolean rdn) {
        this.name = name;
        this.expression = expression;
        this.rdn = rdn;
    }

	public String getName() {
		return name;
	}

    public void setName(String name) {
        this.name = name;
    }

    public Expression getExpression() {
        return expression;
    }

    public void setExpression(String expression) {
        this.expression.setScript(expression);
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
        return expression.getConstant();
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

    public String getScript() {
        return script;
    }

    public void setScript(String script) {
        this.script = script;
    }

    public void addExpression(Expression expression) {
        this.expression = expression;
    }
}
