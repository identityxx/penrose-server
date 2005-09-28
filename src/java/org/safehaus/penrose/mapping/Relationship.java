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

import Zql.ZqlParser;
import Zql.ZExpression;
import Zql.ZExp;

import java.io.Serializable;
import java.io.ByteArrayInputStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Endi S. Dewata
 */
public class Relationship implements Cloneable, Serializable {

    private String operator;
    private List operands = new ArrayList();
    //private String lhs;
    //private String rhs;

    public Relationship() {
    }

    public Relationship(String expression) {
        setExpression(expression);
    }

    public String getExpression() {
        return getLhs()+" "+operator+" "+getRhs();
    }

    public void setExpression(String expression) {
        try {
            ZqlParser parser = new ZqlParser(new ByteArrayInputStream(expression.getBytes()));

            ZExpression exp = (ZExpression)parser.readExpression();
            //System.out.println("Operator: "+exp.getOperator());
            operator = exp.getOperator();

            for (Iterator i=exp.getOperands().iterator(); i.hasNext(); ) {
                ZExp operand = (ZExp)i.next();
                //System.out.println("Operand: "+operand);
                operands.add(operand.toString());
            }

            //System.out.println("Polish: "+exp.toReversePolish());

        } catch (Exception e) {
            e.printStackTrace();
        }

        //int i = expression.indexOf("=");
        //operator = "=";
        //lhs = expression.substring(0, i).trim();
        //rhs = expression.substring(i+1).trim();
    }
    
    public String getLhs() {
        //return lhs;
        return (String)operands.get(0);
    }

    public void setLhs(String lhs) {
        //this.lhs = lhs;
        operands.set(0, lhs);
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    public String getRhs() {
        //return rhs;
        return (String)operands.get(1);
    }

    public void setRhs(String rhs) {
        //this.rhs = rhs;
        operands.set(1, rhs);
    }

    public int hashCode() {
        return (operator == null ? 0 : operator.hashCode()) +
                //(lhs == null ? 0 : lhs.hashCode()) +
                //(rhs == null ? 0 : rhs.hashCode()) +
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
        //if (!equals(lhs, relationship.lhs)) return false;
        //if (!equals(rhs, relationship.rhs)) return false;
        if (!equals(operands, relationship.operands)) return false;

        return true;
    }

    public Object clone() {
        Relationship relationship = new Relationship();
        relationship.operator = operator;
        //relationship.lhs = lhs;
        //relationship.rhs = rhs;
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
