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

import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.SimpleFilter;
import org.safehaus.penrose.filter.AndFilter;
import org.safehaus.penrose.filter.OrFilter;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.mapping.*;

import java.util.Iterator;
import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class EngineFilterTool {

    public EngineContext engineContext;

    public EngineFilterTool(EngineContext engineContext) {
        this.engineContext = engineContext;
    }

    public Filter toSourceFilter(AttributeValues parentValues, EntryDefinition entry, Source source, Filter filter) throws Exception {

        if (filter instanceof SimpleFilter) {
            return toSourceFilter(parentValues, entry, source, (SimpleFilter) filter);

        } else if (filter instanceof AndFilter) {
            return toSourceFilter(parentValues, entry, source, (AndFilter) filter);

        } else if (filter instanceof OrFilter) {
            return toSourceFilter(parentValues, entry, source, (OrFilter) filter);
        }

        return null;
    }

    public Filter toSourceFilter(AttributeValues parentValues, EntryDefinition entry, Source source, SimpleFilter filter)
            throws Exception {

        String name = filter.getAttribute();
        String value = filter.getValue();

        if (name.equals("objectClass")) {
            if (value.equals("*"))
                return null;
        }

        Interpreter interpreter = engineContext.newInterpreter();
        interpreter.set(name, value);

        if (parentValues != null) {
            interpreter.set(parentValues);
        }

        Collection fields = source.getFields();
        Filter newFilter = null;

        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            Field field = (Field)i.next();

            Expression expression = field.getExpression();
            if (expression == null) continue;

            // this assumes that the field's value can be computed using the attribute value in the filter
            String v = (String)interpreter.eval(expression);
            if (v == null) continue;

            //System.out.println("Adding filter "+field.getName()+"="+v);
            SimpleFilter f = new SimpleFilter(field.getName(), "=", v);

            newFilter = engineContext.getFilterTool().appendAndFilter(newFilter, f);
        }

        return newFilter;
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
