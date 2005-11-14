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
package org.safehaus.penrose.engine;

import org.safehaus.penrose.filter.*;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.mapping.*;
import org.apache.log4j.Logger;

import java.util.Iterator;
import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class EngineFilterTool {

    Logger log = Logger.getLogger(getClass());

    public Engine engine;

    public EngineFilterTool(Engine engine) {
        this.engine = engine;
    }

    public Filter toSourceFilter(AttributeValues parentValues, EntryDefinition entry, Source source, Filter filter) throws Exception {
        log.debug("Converting filter "+filter+" for "+source.getName());

        if (filter instanceof NotFilter) {
            return toSourceFilter(parentValues, entry, source, (NotFilter) filter);

        } else if (filter instanceof AndFilter) {
            return toSourceFilter(parentValues, entry, source, (AndFilter) filter);

        } else if (filter instanceof OrFilter) {
            return toSourceFilter(parentValues, entry, source, (OrFilter) filter);

        } else if (filter instanceof SimpleFilter) {
            return toSourceFilter(parentValues, entry, source, (SimpleFilter) filter);

        } else if (filter instanceof SubstringFilter) {
            return toSourceFilter(parentValues, entry, source, (SubstringFilter) filter);
        }

        return null;
    }

    public Filter toSourceFilter(AttributeValues parentValues, EntryDefinition entry, Source source, SimpleFilter filter)
            throws Exception {

        String attributeName = filter.getAttribute();
        String operator = filter.getOperator();
        String attributeValue = filter.getValue();

        if (attributeName.equals("objectClass")) {
            if (attributeValue.equals("*"))
                return null;
        }

        Interpreter interpreter = engine.getInterpreterFactory().newInstance();
        interpreter.set(attributeName, attributeValue);

        if (parentValues != null) {
            interpreter.set(parentValues);
        }

        Collection fields = source.getFields();
        Filter newFilter = null;

        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            Field field = (Field)i.next();

            String v = (String)interpreter.eval(field);
            if (v == null) continue;

            //System.out.println("Adding filter "+field.getName()+"="+v);
            SimpleFilter f = new SimpleFilter(field.getName(), operator, v);

            newFilter = FilterTool.appendAndFilter(newFilter, f);
        }

        interpreter.clear();

        return newFilter;
    }

    public Filter toSourceFilter(AttributeValues parentValues, EntryDefinition entry, Source source, SubstringFilter filter)
            throws Exception {

        String attributeName = filter.getAttribute();
        Collection substrings = filter.getSubstrings();

        AttributeDefinition attributeDefinition = entry.getAttributeDefinition(attributeName);
        String variable = attributeDefinition.getVariable();
        log.debug("variable: "+variable);

        if (variable == null) return null;

        int index = variable.indexOf(".");
        String sourceName = variable.substring(0, index);
        String fieldName = variable.substring(index+1);
        log.debug("sourceName: "+sourceName);
        log.debug("fieldName: "+fieldName);

        if (!sourceName.equals(source.getName())) return null;

        Field field = source.getField(fieldName);

        StringBuffer sb = new StringBuffer();
        for (Iterator i=substrings.iterator(); i.hasNext(); ) {
            String substring = (String)i.next();
            if ("*".equals(substring)) {
                sb.append("%");
            } else {
                sb.append(substring);
            }
        }

        return new SimpleFilter(field.getName(), "like", sb.toString());
    }

    public Filter toSourceFilter(AttributeValues parentValues, EntryDefinition entry, Source source, NotFilter filter)
            throws Exception {

        Filter f = filter.getFilter();

        Filter newFilter = toSourceFilter(parentValues, entry, source, f);

        return new NotFilter(newFilter);
    }

    public Filter toSourceFilter(AttributeValues parentValues, EntryDefinition entry, Source source, AndFilter filter)
            throws Exception {

        Collection filters = filter.getFilters();

        AndFilter af = new AndFilter();
        for (Iterator i=filters.iterator(); i.hasNext(); ) {
            Filter f = (Filter)i.next();

            Filter nf = toSourceFilter(parentValues, entry, source, f);
            if (nf == null) continue;

            af.addFilter(nf);
        }

        if (af.size() == 0) return null;

        return af;
    }

    public Filter toSourceFilter(AttributeValues parentValues, EntryDefinition entry, Source source, OrFilter filter)
            throws Exception {

        Collection filters = filter.getFilters();

        OrFilter of = new OrFilter();
        for (Iterator i=filters.iterator(); i.hasNext(); ) {
            Filter f = (Filter)i.next();

            Filter nf = toSourceFilter(parentValues, entry, source, f);
            if (nf == null) continue;

            of.addFilter(nf);
        }

        if (of.size() == 0) return null;

        return of;
    }

}
