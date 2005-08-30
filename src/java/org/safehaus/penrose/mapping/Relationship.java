/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 * All rights reserved.
 */
package org.safehaus.penrose.mapping;

import java.io.Serializable;

/**
 * @author Endi S. Dewata
 */
public class Relationship implements Cloneable, Serializable {

	/**
	 * Expression.
	 */
    private String expression;

    private String lhs;
    private String operator;
    private String rhs;

    public Relationship() {
    }

    public Relationship(String expression) {
        setExpression(expression);
    }

    public String getExpression() {
        return expression;
    }

    public void setExpression(String expression) {
        this.expression = expression;

        int i = expression.indexOf("=");
        lhs = expression.substring(0, i).trim();
        operator = "=";
        rhs = expression.substring(i+1).trim();
    }
    
    public Object clone() {
        Relationship relationship = new Relationship(expression);
        return relationship;
    }
    
    public String toString() {
    	return expression;
    }

    public String getLhs() {
        return lhs;
    }

    public void setLhs(String lhs) {
        this.lhs = lhs;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    public String getRhs() {
        return rhs;
    }

    public void setRhs(String rhs) {
        this.rhs = rhs;
    }
}
