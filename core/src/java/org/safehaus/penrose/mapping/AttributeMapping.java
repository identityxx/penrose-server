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
package org.safehaus.penrose.mapping;

import org.safehaus.penrose.util.BinaryUtil;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.Arrays;
import java.io.Serializable;

/**
 * @author Endi S. Dewata
 */
public class AttributeMapping implements Serializable, Cloneable {

    static {
        log = LoggerFactory.getLogger(AttributeMapping.class);
    }

    public static transient Logger log;
    public static boolean debug = log.isDebugEnabled();

    public final static String CONSTANT       = "CONSTANT";
    public final static String VARIABLE       = "VARIABLE";
    public final static String EXPRESSION     = "EXPRESSION";

    public final static String DEFAULT_TYPE   = "VARCHAR";
    public final static int DEFAULT_LENGTH    = 50;
    public final static int DEFAULT_PRECISION = 0;

	/**
	 * Name. This refers to AttributeType's name.
	 */
	private String name;

	/**
	 * Values.
	 */
    private Object constant;
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

    public AttributeMapping() {
    }

    public AttributeMapping(String name, String type, Object value) {
        this(name, type, value, false);
    }
    
    public AttributeMapping(String name, String valueType, Object value, boolean rdn) {
        this.name = name;

        if (CONSTANT.equals(valueType)) {
            this.constant = value;

        } else if (VARIABLE.equals(valueType)) {
            this.variable = (String)value;

        } else {
            this.expression = (Expression)value;
        }

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

    public byte[] getBinary() {
        return (byte[])constant;
    }

    public void setBinary(byte[] bytes) {
        constant = bytes;
    }

    public void setBinary(String encodedData) throws Exception {
        constant = BinaryUtil.decode(BinaryUtil.BASE64, encodedData);
    }

    public Object getConstant() {
        return constant;
    }

    public void setConstant(Object constant) {
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
        return name == null ? 0 : name.hashCode();
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

        AttributeMapping attributeMapping = (AttributeMapping)object;
        if (!equals(name, attributeMapping.name)) return false;

        if (constant instanceof byte[] && attributeMapping.constant instanceof byte[]) {
            if (!Arrays.equals((byte[])constant, (byte[])attributeMapping.constant)) return false;
        } else {
            if (!equals(constant, attributeMapping.constant)) return false;
        }

        if (!equals(variable, attributeMapping.variable)) return false;
        if (!equals(expression, attributeMapping.expression)) return false;
        if (rdn != attributeMapping.rdn) return false;
        if (!equals(encryption, attributeMapping.encryption)) return false;
        if (!equals(encoding, attributeMapping.encoding)) return false;
        if (!equals(type, attributeMapping.type)) return false;
        if (length != attributeMapping.length) return false;
        if (precision != attributeMapping.precision) return false;

        return true;
    }

    public Object copy(AttributeMapping attributeMapping) throws CloneNotSupportedException {
        name = attributeMapping.name;

        if (attributeMapping.constant instanceof byte[]) {
            constant = ((byte[])attributeMapping.constant).clone();
        } else {
            constant = attributeMapping.constant;
        }

        variable = attributeMapping.variable;
        expression = attributeMapping.expression == null ? null : (Expression)attributeMapping.expression.clone();
        rdn = attributeMapping.rdn;
        encryption = attributeMapping.encryption;
        encoding = attributeMapping.encoding;
        type = attributeMapping.type;
        length = attributeMapping.length;
        precision = attributeMapping.precision;

        return attributeMapping;
    }

    public Object clone() throws CloneNotSupportedException {
        AttributeMapping attributeMapping = (AttributeMapping)super.clone();
        attributeMapping.copy(this);
        return attributeMapping;
    }
}
