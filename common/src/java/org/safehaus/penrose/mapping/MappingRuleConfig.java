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
public class MappingRuleConfig implements Serializable, Cloneable {

    static {
        log = LoggerFactory.getLogger(MappingRuleConfig.class);
    }

    public static transient Logger log;
    public static boolean debug = log.isDebugEnabled();

    public final static String ADD            = "add";
    public final static String REPLACE        = "replace";

    public final static String CONSTANT       = "CONSTANT";
    public final static String VARIABLE       = "VARIABLE";
    public final static String EXPRESSION     = "EXPRESSION";

    protected String name;

    protected String action    = ADD;
    protected boolean required = true;
    protected String condition;

    protected Object constant;
    protected String variable;
	protected Expression expression;

    public MappingRuleConfig() {
    }

    public MappingRuleConfig(String name) {
        this.name = name;
    }

    public MappingRuleConfig(String name, String type, String value) {
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

        MappingRuleConfig ruleConfig = (MappingRuleConfig)object;
        if (!equals(name, ruleConfig.name)) return false;

        if (!equals(action, ruleConfig.action)) return false;
        if (!equals(required, ruleConfig.required)) return false;
        if (!equals(condition, ruleConfig.condition)) return false;

        if (constant instanceof byte[] && ruleConfig.constant instanceof byte[]) {
            if (!Arrays.equals((byte[])constant, (byte[]) ruleConfig.constant)) return false;
        } else {
            if (!equals(constant, ruleConfig.constant)) return false;
        }

        if (!equals(variable, ruleConfig.variable)) return false;
        if (!equals(expression, ruleConfig.expression)) return false;

        return true;
    }

    public void copy(MappingRuleConfig ruleConfig) throws CloneNotSupportedException {
        name = ruleConfig.name;

        action = ruleConfig.action;
        required = ruleConfig.required;
        condition = ruleConfig.condition;

        if (ruleConfig.constant instanceof byte[]) {
            constant = ((byte[]) ruleConfig.constant).clone();
        } else {
            constant = ruleConfig.constant;
        }

        variable = ruleConfig.variable;
        expression = ruleConfig.expression == null ? null : (Expression) ruleConfig.expression.clone();
    }

    public Object clone() throws CloneNotSupportedException {
        MappingRuleConfig ruleConfig = (MappingRuleConfig)super.clone();
        ruleConfig.copy(this);
        return ruleConfig;
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

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }
}