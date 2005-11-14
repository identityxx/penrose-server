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
package org.safehaus.penrose.interpreter;

import org.safehaus.penrose.mapping.*;
import org.apache.log4j.Logger;

import java.util.Iterator;
import java.util.Collection;
import java.util.ArrayList;
import java.util.HashSet;

/**
 * @author Endi S. Dewata
 */
public abstract class Interpreter {

    Logger log = Logger.getLogger(getClass());

    public void set(Row row) throws Exception {
        for (Iterator i=row.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Object value = row.get(name);
            set(name, value);
        }
    }

    public void set(AttributeValues av) throws Exception {
        for (Iterator i=av.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Collection list = av.get(name);
            //set(name, list);

            Object value;
            if (list.size() == 1) {
                value = list.iterator().next();
            } else {
                value = list;
            }
            set(name, value);

        }
    }

    public abstract Collection parseVariables(String script) throws Exception;

    public abstract void set(String name, Object value) throws Exception;

    public abstract Object get(String name) throws Exception;

    public abstract Object eval(String script) throws Exception;

    public abstract void clear() throws Exception;

    public Object eval(AttributeDefinition attributeDefinition) throws Exception {
        if (attributeDefinition.getConstant() != null) {
            return attributeDefinition.getConstant();

        } else if (attributeDefinition.getVariable() != null) {
            return get(attributeDefinition.getVariable());

        } else if (attributeDefinition.getExpression() != null) {
            return eval(attributeDefinition.getExpression());

        } else {
            return null;
        }
    }

    public Object eval(Field field) throws Exception {
        if (field.getConstant() != null) {
            return field.getConstant();

        } else if (field.getVariable() != null) {
            return get(field.getVariable());

        } else if (field.getExpression() != null) {
            return eval(field.getExpression());

        } else {
            return null;
        }
    }

    public Object eval(Expression expression) throws Exception {

        String foreach = expression.getForeach();
        String var = expression.getVar();
        String script = expression.getScript();

        Object value = null;
        if (foreach == null) {
            //log.debug("Evaluating expression: "+expression);
            value = eval(script);

        } else {
            //log.debug("Evaluating expression: "+expression);

            Object v = get(foreach);
            //log.debug("Values: "+v);

            if (v != null) {
                Collection values;
                if (v instanceof Collection) {
                    values = (Collection)v;
                } else {
                    values = new ArrayList();
                    values.add(v);
                }

                Collection newValues = new HashSet();
                for (Iterator k=values.iterator(); k.hasNext(); ) {
                    Object o = k.next();
                    set(var, o);
                    value = eval(script);
                    if (value == null) continue;

                    //log.debug(" - "+value);
                    newValues.add(value);
                }

                if (newValues.size() == 1) {
                    value = newValues.iterator().next();
                    
                } else if (newValues.size() > 1) {
                    value = newValues;
                }
            }
        }

        return value;
    }
}
