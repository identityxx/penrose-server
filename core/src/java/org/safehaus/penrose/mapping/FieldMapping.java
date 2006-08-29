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

import org.safehaus.penrose.util.BinaryUtil;

import java.util.Arrays;

/**
 * @author Endi S. Dewata
 */
public class FieldMapping implements Cloneable {

    public final static String CONSTANT       = "CONSTANT";
    public final static String VARIABLE       = "VARIABLE";
    public final static String EXPRESSION     = "EXPRESSION";

    public final static String DEFAULT_TYPE   = "VARCHAR";

    public final static String PK_TRUE  = "true";
    public final static String PK_FIRST = "first";
    public final static String PK_FALSE = "false";

	/**
	 * Name.
	 */
	private String name;

    private String type   = DEFAULT_TYPE;

	/**
	 * Expression.
	 */
    private Object constant;
    private String variable;
	private Expression expression;

    /**
     * This field is used in primary key.
     */
    private String primaryKey = PK_FALSE;

    public FieldMapping() {
    }
    
    public FieldMapping(String name) {
        this.name = name;
    }

    public FieldMapping(String name, String type, String value) {
        this.name = name;
        this.type = type;

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
        this.type = EXPRESSION;
		this.expression = expression;
	}

    public byte[] getBinary() {
        return (byte[])constant;
    }

    public void setBinary(byte[] bytes) {
        this.type = CONSTANT;
        constant = bytes;
    }

    public void setBinary(String encodedData) throws Exception {
        this.type = CONSTANT;
        constant = BinaryUtil.decode(BinaryUtil.BASE64, encodedData);
    }

    public Object getConstant() {
        return constant;
    }

    public void setConstant(Object constant) {
        this.type = CONSTANT;
        this.constant = constant;
    }

    public String getVariable() {
        return variable;
    }

    public void setVariable(String variable) {
        this.type = VARIABLE;
        this.variable = variable;
    }

    public int hashCode() {
        return (name == null ? 0 : name.hashCode()) +
                (type == null ? 0 : type.hashCode()) +
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
        if((object == null) || (object.getClass() != this.getClass())) return false;

        FieldMapping fieldMapping = (FieldMapping)object;
        if (!equals(name, fieldMapping.name)) return false;
        if (!equals(type, fieldMapping.type)) return false;

        if (constant instanceof byte[] && fieldMapping.constant instanceof byte[]) {
            if (!Arrays.equals((byte[])constant, (byte[])fieldMapping.constant)) return false;
        } else {
            if (!equals(constant, fieldMapping.constant)) return false;
        }

        if (!equals(variable, fieldMapping.variable)) return false;
        if (!equals(expression, fieldMapping.expression)) return false;

        return true;
    }

    public void copy(FieldMapping fieldMapping) {
        name = fieldMapping.name;
        type = fieldMapping.type;

        if (fieldMapping.constant instanceof byte[]) {
            constant = ((byte[])fieldMapping.constant).clone();
        } else {
            constant = fieldMapping.constant;
        }

        variable = fieldMapping.variable;
        expression = fieldMapping.expression == null ? null : (Expression)fieldMapping.expression.clone();
    }

    public Object clone() {
        FieldMapping fieldMapping = new FieldMapping();
        fieldMapping.copy(this);
        return fieldMapping;
    }

    public boolean isPK() {
        return !PK_FALSE.equals(primaryKey);
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(String primaryKey) {
        this.primaryKey = primaryKey;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}