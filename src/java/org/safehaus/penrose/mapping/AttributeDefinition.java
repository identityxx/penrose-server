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

    public final static String DEFAULT_TYPE   = "VARCHAR";
    public final static int DEFAULT_LENGTH    = 50;
    public final static int DEFAULT_PRECISION = 0;

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
    private String constant;
    private String variable;
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

    private String type   = DEFAULT_TYPE;
    private int length    = DEFAULT_LENGTH;
    private int precision = DEFAULT_PRECISION;

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

    public String getConstant() {
        return constant;
    }

    public void setConstant(String constant) {
        this.constant = constant;
    }

    public String getVariable() {
        return variable;
    }

    public void setVariable(String variable) {
        this.variable = variable;
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

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public int getPrecision() {
        return precision;
    }

    public void setPrecision(int precision) {
        this.precision = precision;
    }

    public int hashCode() {
        return (name == null ? 0 : name.hashCode()) +
                (script == null ? 0 : script.hashCode()) +
                (constant == null ? 0 : constant.hashCode()) +
                (variable == null ? 0 : variable.hashCode()) +
                (expression == null ? 0 : expression.hashCode()) +
                (rdn ? 0 : 1) +
                (encryption == null ? 0 : encryption.hashCode()) +
                (encoding == null ? 0 : encoding.hashCode()) +
                (type == null ? 0 : type.hashCode()) +
                (length) +
                (precision);
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
        if (!equals(constant, attributeDefinition.constant)) return false;
        if (!equals(variable, attributeDefinition.variable)) return false;
        if (!equals(expression, attributeDefinition.expression)) return false;
        if (rdn != attributeDefinition.rdn) return false;
        if (!equals(encryption, attributeDefinition.encryption)) return false;
        if (!equals(encoding, attributeDefinition.encoding)) return false;
        if (!equals(type, attributeDefinition.type)) return false;
        if (length != attributeDefinition.length) return false;
        if (precision != attributeDefinition.precision) return false;

        return true;
    }

    public Object copy(AttributeDefinition attributeDefinition) {
        name = attributeDefinition.name;
        script = attributeDefinition.script;
        constant = attributeDefinition.constant;
        variable = attributeDefinition.variable;
        expression = attributeDefinition.expression == null ? null : (Expression)attributeDefinition.expression.clone();
        rdn = attributeDefinition.rdn;
        encryption = attributeDefinition.encryption;
        encoding = attributeDefinition.encoding;
        type = attributeDefinition.type;
        length = attributeDefinition.length;
        precision = attributeDefinition.precision;
        return attributeDefinition;
    }

    public Object clone() {
        AttributeDefinition attributeDefinition = new AttributeDefinition();
        attributeDefinition.copy(this);
        return attributeDefinition;
    }

    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("[");
        sb.append(name);
        sb.append(": ");

        if (constant != null) {
            sb.append("\"");
            sb.append(constant);
            sb.append("\"");

        } else if (variable != null) {
            sb.append(variable);

        } else {
            sb.append("...");
        }

        sb.append("]");

        return sb.toString();
    }
}
