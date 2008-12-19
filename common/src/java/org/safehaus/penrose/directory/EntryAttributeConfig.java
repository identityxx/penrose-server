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
package org.safehaus.penrose.directory;

import org.safehaus.penrose.util.BinaryUtil;
import org.safehaus.penrose.mapping.Expression;

import java.util.Arrays;
import java.io.Serializable;

/**
 * @author Endi S. Dewata
 */
public class EntryAttributeConfig implements Serializable, Cloneable {

    public final static String CONSTANT   = "CONSTANT";
    public final static String VARIABLE   = "VARIABLE";
    public final static String EXPRESSION = "EXPRESSION";

	private String name;
    private boolean rdn;
    private String encryption;

    private Object constant;
    private String variable;
    private Expression expression;

    public EntryAttributeConfig() {
    }

    public EntryAttributeConfig(String name, Object value) {
        this.name = name;
        this.constant = value;
    }

    public EntryAttributeConfig(String name, Object value, boolean rdn) {
        this.name = name;
        this.constant = value;
        this.rdn = rdn;
    }

    public EntryAttributeConfig(String name, String type, Object value) {
        this(name, type, value, false);
    }
    
    public EntryAttributeConfig(String name, String type, Object value, boolean rdn) {
        this.name = name;

        if (CONSTANT.equals(type)) {
            this.constant = value;

        } else if (VARIABLE.equals(type)) {
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

    public int hashCode() {
        return (name == null ? 0 : name.hashCode()) +
                (rdn ? 0 : 1) +
                (encryption == null ? 0 : encryption.hashCode()) +
                (constant == null ? 0 : constant.hashCode()) +
                (variable == null ? 0 : variable.hashCode()) +
                (expression == null ? 0 : expression.hashCode());
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

        EntryAttributeConfig attributeConfig = (EntryAttributeConfig)object;
        if (!equals(name, attributeConfig.name)) return false;
        if (rdn != attributeConfig.rdn) return false;
        if (!equals(encryption, attributeConfig.encryption)) return false;

        if (constant instanceof byte[] && attributeConfig.constant instanceof byte[]) {
            if (!Arrays.equals((byte[])constant, (byte[])attributeConfig.constant)) return false;
        } else {
            if (!equals(constant, attributeConfig.constant)) return false;
        }

        if (!equals(variable, attributeConfig.variable)) return false;
        if (!equals(expression, attributeConfig.expression)) return false;

        return true;
    }

    public Object copy(EntryAttributeConfig attributeConfig) throws CloneNotSupportedException {
        name = attributeConfig.name;
        rdn = attributeConfig.rdn;
        encryption = attributeConfig.encryption;

        if (attributeConfig.constant instanceof byte[]) {
            constant = ((byte[])attributeConfig.constant).clone();
        } else {
            constant = attributeConfig.constant;
        }

        variable = attributeConfig.variable;
        expression = attributeConfig.expression == null ? null : (Expression)attributeConfig.expression.clone();

        return attributeConfig;
    }

    public Object clone() throws CloneNotSupportedException {
        EntryAttributeConfig attributeConfig = (EntryAttributeConfig)super.clone();
        attributeConfig.copy(this);
        return attributeConfig;
    }
}
