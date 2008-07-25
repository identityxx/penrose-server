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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.io.Serializable;

/**
 * @author Endi S. Dewata
 */
public class MappingFieldConfig implements Serializable, Cloneable {

    static {
        log = LoggerFactory.getLogger(MappingFieldConfig.class);
    }

    public static transient Logger log;
    public static boolean debug = log.isDebugEnabled();

    public final static String CONSTANT       = "CONSTANT";
    public final static String VARIABLE       = "VARIABLE";
    public final static String EXPRESSION     = "EXPRESSION";

    protected String name;

    protected boolean required = true;
    protected String condition;

    protected Object constant;
    protected String variable;
	protected Expression expression;

    public MappingFieldConfig() {
    }

    public MappingFieldConfig(String name) {
        this.name = name;
    }

    public MappingFieldConfig(String name, String type, String value) {
        this.name = name;

        if (CONSTANT.equals(type)) {
            this.constant = value;

        } else if (VARIABLE.equals(type)) {
            this.variable = value;

        } else {
            this.expression = new Expression(value);
        }
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
        constant = null;
        variable = null;
	}

    public byte[] getBinary() {
        return (byte[])constant;
    }

    public void setBinary(byte[] bytes) {
        constant = bytes;
        variable = null;
        expression = null;
    }

    public void setBinary(String encodedData) throws Exception {
        constant = BinaryUtil.decode(BinaryUtil.BASE64, encodedData);
        variable = null;
        expression = null;
    }

    public Object getConstant() {
        return constant;
    }

    public void setConstant(Object constant) {
        this.constant = constant;
        variable = null;
        expression = null;
    }

    public String getVariable() {
        return variable;
    }

    public void setVariable(String variable) {
        this.variable = variable;
        constant = null;
        expression = null;
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

        MappingFieldConfig fieldConfig = (MappingFieldConfig)object;
        if (!equals(name, fieldConfig.name)) return false;

        if (!equals(required, fieldConfig.required)) return false;
        if (!equals(condition, fieldConfig.condition)) return false;

        if (constant instanceof byte[] && fieldConfig.constant instanceof byte[]) {
            if (!Arrays.equals((byte[])constant, (byte[])fieldConfig.constant)) return false;
        } else {
            if (!equals(constant, fieldConfig.constant)) return false;
        }

        if (!equals(variable, fieldConfig.variable)) return false;
        if (!equals(expression, fieldConfig.expression)) return false;

        return true;
    }

    public void copy(MappingFieldConfig fieldConfig) throws CloneNotSupportedException {
        name = fieldConfig.name;

        required = fieldConfig.required;
        condition = fieldConfig.condition;

        if (fieldConfig.constant instanceof byte[]) {
            constant = ((byte[])fieldConfig.constant).clone();
        } else {
            constant = fieldConfig.constant;
        }

        variable = fieldConfig.variable;
        expression = fieldConfig.expression == null ? null : (Expression)fieldConfig.expression.clone();
    }

    public Object clone() throws CloneNotSupportedException {
        MappingFieldConfig fieldConfig = (MappingFieldConfig)super.clone();
        fieldConfig.copy(this);
        return fieldConfig;
    }

    public String getCondition() {
        return condition;
    }

    public void setCondition(String condition) {
        this.condition = condition;
    }

    public boolean isRequired() {
        return required;
    }

    public void setRequired(boolean required) {
        this.required = required;
    }
}