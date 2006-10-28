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

import Zql.ZqlParser;
import Zql.ZExpression;
import Zql.ZExp;

import java.io.ByteArrayInputStream;
import java.io.Serializable;
import java.util.*;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/**
 * @author Endi S. Dewata
 */
public class Relationship implements Cloneable, Serializable {

    private String operator = "=";
    private List operands = new ArrayList();

    public Relationship() {
    }

    public Relationship(String expression) throws Exception {
        setExpression(expression);
    }

    public String getExpression() {
        StringBuffer sb = new StringBuffer();
        for (Iterator i=operands.iterator(); i.hasNext(); ) {
            String operand = i.next().toString();
            if (sb.length() > 0) {
                sb.append(" ");
                sb.append(operator);
                sb.append(" ");
            }
            sb.append(operand);
        }
        return sb.toString();
    }

    public void setExpression(String expression) throws Exception {
        //System.out.println("EXPRESSION: "+expression);

        ZqlParser parser = new ZqlParser(new ByteArrayInputStream(expression.getBytes()));

        ZExpression exp = (ZExpression)parser.readExpression();
        //System.out.println("Operator: "+exp.getOperator());
        operator = exp.getOperator();

        operands.clear();
        for (Iterator i=exp.getOperands().iterator(); i.hasNext(); ) {
            ZExp operand = (ZExp)i.next();
            //System.out.println("Operand: "+operand+" ("+operand.getClass()+")");
            operands.add(operand);
        }

        //System.out.println("Polish: "+exp.toReversePolish());
    }

    public String getLhs() {
        if (operands.size() < 1) return null;
        return operands.get(0).toString();
    }

    public void setLhs(String lhs) {
        operands.set(0, lhs);
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    public String getRhs() {
        if (operands.size() < 2) return null;
        return operands.get(1).toString();
    }

    public void setRhs(String rhs) {
        operands.set(1, rhs);
    }

    public int hashCode() {
        return (operator == null ? 0 : operator.hashCode()) +
                (operands == null ? 0 : operands.hashCode());
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
        if (!equals(operator, relationship.operator)) return false;
        if (!equals(operands, relationship.operands)) return false;

        return true;
    }

    public Object clone() {
        Relationship relationship = new Relationship();
        relationship.operator = operator;
        relationship.operands = operands;
        return relationship;
    }

    public String toString() {
        return getExpression();
    }

    public Collection getOperands() {
        return operands;
    }
}
