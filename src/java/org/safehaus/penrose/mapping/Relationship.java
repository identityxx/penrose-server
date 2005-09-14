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

    private String lhs;
    private String operator;
    private String rhs;

    public Relationship() {
    }

    public Relationship(String expression) {
        setExpression(expression);
    }

    public String getExpression() {
        return lhs+" "+operator+" "+rhs;
    }

    public void setExpression(String expression) {
        int i = expression.indexOf("=");
        lhs = expression.substring(0, i).trim();
        operator = "=";
        rhs = expression.substring(i+1).trim();
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

    public int hashCode() {
        return (lhs == null ? 0 : lhs.hashCode()) +
                (operator == null ? 0 : operator.hashCode()) +
                (rhs == null ? 0 : rhs.hashCode());
    }

    boolean equals(Object o1, Object o2) {
        if (o1 == null && o2 == null) return true;
        if (o1 != null) return o1.equals(o2);
        return o2.equals(o1);
    }

    public boolean equals(Object object) {
        if (this == object) return true;
        if((object == null) || (object.getClass() != this.getClass())) return false;

        Relationship relationship = (Relationship)object;
        if (!equals(lhs, relationship.lhs)) return false;
        if (!equals(operator, relationship.operator)) return false;
        if (!equals(rhs, relationship.rhs)) return false;

        return true;
    }

    public Object clone() {
        Relationship relationship = new Relationship();
        relationship.lhs = lhs;
        relationship.operator = operator;
        relationship.rhs = rhs;
        return relationship;
    }

    public String toString() {
    	return lhs+" "+operator+" "+rhs;
    }

}
