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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.StringTokenizer;
import java.io.Serializable;

/**
 * @author Endi S. Dewata
 */
public class FieldMapping implements Serializable, Cloneable {

    static {
        log = LoggerFactory.getLogger(FieldMapping.class);
    }

    public static transient Logger log;
    public static boolean debug = log.isDebugEnabled();

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

    public FieldMapping() {
    }
    
    public FieldMapping(String name) {
        this.name = name;
    }

    public FieldMapping(String name, String type, String value) {
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

        FieldMapping fieldMapping = (FieldMapping)object;
        if (!equals(name, fieldMapping.name)) return false;
        if (!equals(primaryKey, fieldMapping.primaryKey)) return false;

        if (constant instanceof byte[] && fieldMapping.constant instanceof byte[]) {
            if (!Arrays.equals((byte[])constant, (byte[])fieldMapping.constant)) return false;
        } else {
            if (!equals(constant, fieldMapping.constant)) return false;
        }

        if (!equals(variable, fieldMapping.variable)) return false;
        if (!equals(expression, fieldMapping.expression)) return false;
        if (!equals(operations, fieldMapping.operations)) return false;

        return true;
    }

    public void copy(FieldMapping fieldMapping) throws CloneNotSupportedException {
        name = fieldMapping.name;
        primaryKey = fieldMapping.primaryKey;

        if (fieldMapping.constant instanceof byte[]) {
            constant = ((byte[])fieldMapping.constant).clone();
        } else {
            constant = fieldMapping.constant;
        }

        variable = fieldMapping.variable;
        expression = fieldMapping.expression == null ? null : (Expression)fieldMapping.expression.clone();

        operations = new HashSet<String>();
        operations.addAll(fieldMapping.operations);
    }

    public Object clone() throws CloneNotSupportedException {
        FieldMapping fieldMapping = (FieldMapping)super.clone();
        fieldMapping.copy(this);
        return fieldMapping;
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