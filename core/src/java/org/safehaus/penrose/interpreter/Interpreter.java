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
package org.safehaus.penrose.interpreter;

import org.safehaus.penrose.mapping.*;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.Iterator;
import java.util.Collection;
import java.util.ArrayList;
import java.util.HashSet;

/**
 * @author Endi S. Dewata
 */
public abstract class Interpreter {

    Logger log = LoggerFactory.getLogger(getClass());

    Collection rows;

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

    public void set(AttributeValues av, Collection rows) throws Exception {
        set(av);
        this.rows = rows;
    }

    public abstract Collection parseVariables(String script) throws Exception;

    public abstract void set(String name, Object value) throws Exception;

    public abstract Object get(String name) throws Exception;

    public abstract Object eval(String script) throws Exception;

    public abstract void clear() throws Exception;

    public Object eval(EntryMapping entryMapping, AttributeMapping attributeMapping) throws Exception {
        try {
            if (attributeMapping.getConstant() != null) {
                return attributeMapping.getConstant();

            } else if (attributeMapping.getVariable() != null) {
                String name = attributeMapping.getVariable();
                Object value = get(name);
                if (value == null && name.startsWith("primaryKey.")) {
                    value = get(name.substring(11));
                }
                return value;

            } else if (attributeMapping.getExpression() != null) {
                return eval(entryMapping, attributeMapping.getExpression());

            } else {
                return null;
            }
        } catch (Exception e) {
            throw new Exception("Error evaluating attribute "+attributeMapping.getName()+": "+e.getMessage(), e);
        }
    }

    public Object eval(EntryMapping entryMapping, FieldMapping fieldMapping) throws Exception {
        try {
            if (fieldMapping.getConstant() != null) {
                return fieldMapping.getConstant();

            } else if (fieldMapping.getVariable() != null) {
                String name = fieldMapping.getVariable();
                Object value = get(name);
                if (value == null && name.startsWith("rdn.")) {
                    value = get(name.substring(4));
                }
                return value;

            } else if (fieldMapping.getExpression() != null) {
                return eval(entryMapping, fieldMapping.getExpression());

            } else {
                return null;
            }
        } catch (Exception e) {
            throw new Exception("Error evaluating field "+fieldMapping.getName(), e);
        }
    }

    public Object eval(EntryMapping entryMapping, Expression expression) throws Exception {

        String foreach = expression.getForeach();
        String var = expression.getVar();
        String script = expression.getScript();

        Object value = null;
        if (foreach == null) {
            //log.debug("Evaluating expression: "+expression);
            value = eval(script);

        } else {
            //log.debug("Foreach: "+foreach);

            Object v = get(foreach);
            //log.debug("Values: "+v);

            Collection newValues = new HashSet();

            if (entryMapping.getSourceMapping(foreach) != null) { // process each row in source

                //log.debug("Rows:");
                for (Iterator i=rows.iterator(); i.hasNext(); ) {
                    AttributeValues row = (AttributeValues)i.next();
                    //log.debug(" - "+row);

                    for (Iterator j=row.getNames().iterator(); j.hasNext(); ) {
                        String name = (String)j.next();
                        Collection values = row.get(name);

                        if (values.size() == 1) {
                            value = values.iterator().next();

                        } else if (values.size() > 1) {
                            value = values;
                        }

                        int k = name.indexOf(".");
                        if (k < 0) {
                            //log.debug("setting "+var+"."+name+" = "+value);
                            set(var+"."+name, value);

                        } else if (foreach.equals(name.substring(0, k))) {
                            //log.debug("setting "+var+"."+name.substring(k+1)+" = "+value);
                            set(var+"."+name.substring(k+1), value);
                        }
                    }

                    value = eval(script);
                    if (value == null) continue;

                    //log.debug(" - "+value);
                    newValues.add(value);
                }

            } else if (v != null) {

                Collection values;
                if (v instanceof Collection) {
                    values = (Collection)v;
                } else {
                    values = new ArrayList();
                    values.add(v);
                }

                for (Iterator i=values.iterator(); i.hasNext(); ) {
                    Object o = i.next();
                    set(var, o);
                    value = eval(script);
                    if (value == null) continue;

                    //log.debug(" - "+value);
                    newValues.add(value);
                }
            }

            if (newValues.size() == 1) {
                value = newValues.iterator().next();

            } else if (newValues.size() > 1) {
                value = newValues;
            }
        }

        return value;
    }
}
