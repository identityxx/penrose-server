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
package org.safehaus.penrose.partition;

import org.safehaus.penrose.mapping.Expression;
import org.safehaus.penrose.util.BinaryUtil;

import java.util.Arrays;

/**
 * @author Endi S. Dewata
 */
public class FieldConfig implements FieldConfigMBean, Comparable, Cloneable {

    public final static String TYPE_VARCHAR   = "VARCHAR";
    public final static String TYPE_INTEGER   = "INTEGER";
    public final static String TYPE_DOUBLE    = "DOUBLE";
    public final static String TYPE_DATETIME  = "DATETIME";

    public final static String DEFAULT_TYPE   = TYPE_VARCHAR;
    public final static int DEFAULT_LENGTH    = 50;
    public final static int DEFAULT_PRECISION = 0;

	/**
	 * Name.
	 */
	private String name;

    private String originalName;

    private String type   = DEFAULT_TYPE;
    private int length    = DEFAULT_LENGTH;
    private int precision = DEFAULT_PRECISION;

	private boolean primaryKey;
    private boolean searchable = true;
    private boolean unique;
    private boolean index;
    private boolean caseSensitive;

    private Object constant;
    private String variable;
	private Expression expression;

	public FieldConfig() {
	}

    public FieldConfig(String name) {
        this.name = name;
    }

    public FieldConfig(String name, boolean primaryKey) {
        this.name = name;
        this.primaryKey = primaryKey;
    }

    public FieldConfig(String name, String originalName, boolean primaryKey) {
        this.name = name;
        this.originalName = originalName;
        this.primaryKey = primaryKey;
    }

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public boolean isPrimaryKey() {
		return primaryKey;
	}

    public void setPrimaryKey(boolean primaryKey) {
		this.primaryKey = primaryKey;
	}

    public String getOriginalName() {
        return originalName == null ? name : originalName;
    }

    public void setOriginalName(String originalName) {
        this.originalName = originalName;
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

    public boolean isSearchable() {
        return searchable;
    }

    public void setSearchable(boolean searchable) {
        this.searchable = searchable;
    }

    public boolean isUnique() {
        return unique;
    }

    public void setUnique(boolean unique) {
        this.unique = unique;
    }

    public boolean isIndex() {
        return index;
    }

    public void setIndex(boolean index) {
        this.index = index;
    }

    public boolean isCaseSensitive() {
        return caseSensitive;
    }

    public void setCaseSensitive(boolean caseSensitive) {
        this.caseSensitive = caseSensitive;
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
        if (object == null) return false;
        if (!(object instanceof FieldConfig)) return false;

        FieldConfig fieldConfig = (FieldConfig)object;
        if (!equals(name, fieldConfig.name)) return false;
        if (!equals(originalName, fieldConfig.originalName)) return false;
        if (primaryKey != fieldConfig.primaryKey) return false;
        if (searchable != fieldConfig.searchable) return false;
        if (unique != fieldConfig.unique) return false;
        if (index != fieldConfig.index) return false;
        if (caseSensitive != fieldConfig.caseSensitive) return false;
        if (!equals(type, fieldConfig.type)) return false;
        if (length != fieldConfig.length) return false;
        if (precision != fieldConfig.precision) return false;

        if (constant instanceof byte[] && fieldConfig.constant instanceof byte[]) {
            if (!Arrays.equals((byte[])constant, (byte[])fieldConfig.constant)) return false;
        } else {
            if (!equals(constant, fieldConfig.constant)) return false;
        }

        if (!equals(variable, fieldConfig.variable)) return false;
        if (!equals(expression, fieldConfig.expression)) return false;

        return true;
    }

    public int compareTo(Object object) {
        if (object == null) return 0;
        if (!(object instanceof FieldConfig)) return 0;

        FieldConfig fd = (FieldConfig)object;
        return name.compareTo(fd.name);
    }

    public void copy(FieldConfig fieldConfig) {
        name = fieldConfig.name;
        originalName = fieldConfig.originalName;
        primaryKey = fieldConfig.primaryKey;
        searchable = fieldConfig.searchable;
        unique = fieldConfig.unique;
        index = fieldConfig.index;
        caseSensitive = fieldConfig.caseSensitive;
        type = fieldConfig.type;
        length = fieldConfig.length;
        precision = fieldConfig.precision;

        if (fieldConfig.constant instanceof byte[]) {
            constant = ((byte[])fieldConfig.constant).clone();
        } else {
            constant = fieldConfig.constant;
        }

        variable = fieldConfig.variable;
        expression = fieldConfig.expression == null ? null : (Expression)fieldConfig.expression.clone();
    }

    public Object clone() {
        FieldConfig fieldConfig = new FieldConfig();
        fieldConfig.copy(this);
        return fieldConfig;
    }
}