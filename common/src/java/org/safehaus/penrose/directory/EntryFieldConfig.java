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
import java.util.Collection;
import java.util.HashSet;
import java.util.StringTokenizer;
import java.io.Serializable;

/**
 * @author Endi S. Dewata
 */
public class EntryFieldConfig implements Serializable, Cloneable {

    public final static String CONSTANT       = "CONSTANT";
    public final static String VARIABLE       = "VARIABLE";
    public final static String EXPRESSION     = "EXPRESSION";

    public final static String ADD     = "add";
    public final static String BIND    = "bind";
    public final static String COMPARE = "compare";
    public final static String DELETE  = "delete";
    public final static String MODIFY  = "modify";
    public final static String MODRDN  = "modrdn";
    public final static String SEARCH  = "search";

    private String name;
    private boolean primaryKey;

    private Object constant;
    private String variable;
	private Expression expression;

    private HashSet<String> operations = new HashSet<String>();

    public EntryFieldConfig() {
    }
    
    public EntryFieldConfig(String name) {
        this.name = name;
    }

    public EntryFieldConfig(String name, String type, String value) {
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

        EntryFieldConfig fieldConfig = (EntryFieldConfig)object;
        if (!equals(name, fieldConfig.name)) return false;
        if (!equals(primaryKey, fieldConfig.primaryKey)) return false;

        if (constant instanceof byte[] && fieldConfig.constant instanceof byte[]) {
            if (!Arrays.equals((byte[])constant, (byte[])fieldConfig.constant)) return false;
        } else {
            if (!equals(constant, fieldConfig.constant)) return false;
        }

        if (!equals(variable, fieldConfig.variable)) return false;
        if (!equals(expression, fieldConfig.expression)) return false;
        if (!equals(operations, fieldConfig.operations)) return false;

        return true;
    }

    public void copy(EntryFieldConfig fieldConfig) throws CloneNotSupportedException {
        name = fieldConfig.name;
        primaryKey = fieldConfig.primaryKey;

        if (fieldConfig.constant instanceof byte[]) {
            constant = ((byte[])fieldConfig.constant).clone();
        } else {
            constant = fieldConfig.constant;
        }

        variable = fieldConfig.variable;
        expression = fieldConfig.expression == null ? null : (Expression)fieldConfig.expression.clone();

        operations = new HashSet<String>();
        operations.addAll(fieldConfig.operations);
    }

    public Object clone() throws CloneNotSupportedException {
        EntryFieldConfig fieldConfig = (EntryFieldConfig)super.clone();
        fieldConfig.copy(this);
        return fieldConfig;
    }

    public Collection<String> getOperations() {
        return operations;
    }

    public void setOperations(Collection<String> operations) {
        if (this.operations == operations) return;
        this.operations.clear();
        this.operations.addAll(operations);
    }
    
    public void setStringOperations(String operations) {
        StringTokenizer st = new StringTokenizer(operations, ",");
        while (st.hasMoreTokens()) {
            String operation = st.nextToken();
            this.operations.add(operation);
        }
    }

    public boolean isPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(boolean primaryKey) {
        this.primaryKey = primaryKey;
    }
}