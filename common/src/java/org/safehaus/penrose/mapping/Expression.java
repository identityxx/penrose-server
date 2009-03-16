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

import java.io.Serializable;

/**
 * @author Endi S. Dewata
 */
public class Expression implements Serializable, Cloneable {

    public final static long serialVersionUID = 1L;

    protected String foreach;
    protected String var;
    protected String script;

    public Expression() {
    }

    public Expression(String script) {
        this.script = script;
    }

    public Expression(String foreach, String var, String script) {
        this.foreach = foreach;
        this.var = var;
        this.script = script;
    }

    public String getScript() {
        return script;
    }

    public void setScript(String script) {
        this.script = script;
    }

    public String getForeach() {
        return foreach;
    }

    public void setForeach(String foreach) {
        this.foreach = foreach;
    }

    public String getVar() {
        return var;
    }

    public void setVar(String var) {
        this.var = var;
    }

    public int hashCode() {
        return (foreach == null ? 0 : foreach.hashCode()) +
                (var == null ? 0 : var.hashCode()) +
                (script == null ? 0 : script.hashCode());
    }

    boolean equals(Object o1, Object o2) {
        if (o1 == null && o2 == null) return true;
        if (o1 != null) return o1.equals(o2);
        return o2.equals(o1);
    }

    public boolean equals(Object object) {
        if (this == object) return true;
        if((object == null) || (object.getClass() != this.getClass())) return false;

        Expression expression = (Expression)object;
        if (!equals(foreach, expression.foreach)) return false;
        if (!equals(var, expression.var)) return false;
        if (!equals(script, expression.script)) return false;

        return true;
    }

    public void copy(Expression expression) {
        foreach = expression.foreach;
        var     = expression.var;
        script  = expression.script;
    }

    public Object clone() throws CloneNotSupportedException {
        Expression expression = (Expression)super.clone();
        expression.copy(this);
        return expression;
    }

    public String toString() {
        return foreach == null ? script : "foreach "+foreach+" ("+var+"): "+script;
    }

}
