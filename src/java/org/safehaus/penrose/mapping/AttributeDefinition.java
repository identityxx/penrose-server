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

    public void setExpression(Expression expression) {
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

    public int hashCode() {
        return (name == null ? 0 : name.hashCode()) +
                (script == null ? 0 : script.hashCode()) +
                (expression == null ? 0 : expression.hashCode()) +
                (rdn ? 0 : 1) +
                (encryption == null ? 0 : encryption.hashCode()) +
                (encoding == null ? 0 : encoding.hashCode());
    }

    boolean equals(Object o1, Object o2) {
        if (o1 == null && o2 == null) return true;
        if (o1 != null) return o1.equals(o2);
        return o2.equals(o1);
    }

    public boolean equals(Object object) {
        if (this == object) return true;
        if((object == null) || (object.getClass() != this.getClass())) return false;

        AttributeDefinition attributeDefinition = (AttributeDefinition)object;
        if (!equals(name, attributeDefinition.name)) return false;
        if (!equals(script, attributeDefinition.script)) return false;
        if (!equals(expression, attributeDefinition.expression)) return false;
        if (rdn != attributeDefinition.rdn) return false;
        if (!equals(encryption, attributeDefinition.encryption)) return false;
        if (!equals(encoding, attributeDefinition.encoding)) return false;

        return true;
    }

    public Object clone() {
        AttributeDefinition attribute = new AttributeDefinition();
        attribute.name = name;
        attribute.script = script;
        attribute.expression = expression == null ? null : (Expression)expression.clone();
        attribute.rdn = rdn;
        attribute.encryption = encryption;
        attribute.encoding = encoding;
        return attribute;
    }

    public String toString() {
        return "["+name+":"+expression+"]";
    }

}
