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
package org.safehaus.penrose.adapter.jdbc;

import org.safehaus.penrose.filter.*;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.entry.AttributeValues;
import org.safehaus.penrose.source.SourceRef;
import org.safehaus.penrose.source.FieldRef;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class FilterBuilder {

    public Logger log = LoggerFactory.getLogger(getClass());

    EntryMapping entryMapping;

    Map sourceRefs = new LinkedHashMap(); // need to maintain order

    Interpreter interpreter;

    Map sourceAliases = new LinkedHashMap(); // need to maintain order
    Filter sourceFilter;

    public FilterBuilder(
            EntryMapping entryMapping,
            Collection sourceRefs,
            AttributeValues sourceValues,
            Interpreter interpreter
    ) throws Exception {

        this.entryMapping = entryMapping;

        for (Iterator i=sourceRefs.iterator(); i.hasNext(); ) {
            SourceRef sourceRef = (SourceRef)i.next();
            this.sourceRefs.put(sourceRef.getAlias(), sourceRef);
        }

        this.interpreter = interpreter;

        boolean debug = log.isDebugEnabled();
        if (debug) log.debug("Creating filters:");

        for (Iterator i=sourceValues.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Collection values = sourceValues.get(name);

            int p = name.indexOf(".");
            String sourceName = name.substring(0, p);
            String fieldName = name.substring(p+1);

            String alias = createTableAlias(sourceName);
            setTableAlias(sourceName, alias);

            for (Iterator j=values.iterator(); j.hasNext(); ) {
                Object value = j.next();

                SimpleFilter f = new SimpleFilter(alias+"."+fieldName, "=", value);
                if (debug) log.debug(" - Filter "+f);

                sourceFilter = FilterTool.appendAndFilter(sourceFilter, f);
            }
        }
    }

    public Filter convert(
            Filter filter
    ) throws Exception {

        if (filter instanceof NotFilter) {
            return convert((NotFilter)filter);

        } else if (filter instanceof AndFilter) {
            return convert((AndFilter)filter);

        } else if (filter instanceof OrFilter) {
            return convert((OrFilter)filter);

        } else if (filter instanceof SimpleFilter) {
            return convert((SimpleFilter)filter);

        } else if (filter instanceof SubstringFilter) {
            return convert((SubstringFilter)filter);

        } else if (filter instanceof PresentFilter) {
            return convert((PresentFilter)filter);
        }

        return null;
    }

    public Filter convert(NotFilter filter) throws Exception {
        Filter newFilter = convert(filter.getFilter());
        return new NotFilter(newFilter);
    }

    public Filter convert(AndFilter filter) throws Exception {

        Filter newFilter = null;

        Collection filters = filter.getFilters();
        for (Iterator i=filters.iterator(); i.hasNext(); ) {
            Filter f = (Filter)i.next();

            Filter nf = convert(f);
            newFilter = FilterTool.appendAndFilter(newFilter, nf);
        }

        return newFilter;
    }

    public Filter convert(OrFilter filter) throws Exception {

        Filter newFilter = null;

        Collection filters = filter.getFilters();
        for (Iterator i=filters.iterator(); i.hasNext(); ) {
            Filter f = (Filter)i.next();

            Filter nf = convert(f);
            newFilter = FilterTool.appendOrFilter(newFilter, nf);
        }

        return newFilter;
    }

    public Filter convert(
            SimpleFilter filter
    ) throws Exception {

        boolean debug = log.isDebugEnabled();
        if (debug) log.debug("Converting filter "+filter);

        String attributeName = filter.getAttribute();
        String operator = filter.getOperator();
        Object attributeValue = filter.getValue();

        interpreter.set(attributeName, attributeValue);

        Filter newFilter = null;
        for (Iterator i= sourceRefs.values().iterator(); i.hasNext(); ) {
            SourceRef sourceRef = (SourceRef)i.next();
            String sourceName = sourceRef.getAlias();

            String alias = createTableAlias(sourceName);

            Filter f = null;
            for (Iterator j= sourceRef.getFieldRefs().iterator(); j.hasNext(); ) {
                FieldRef fieldRef = (FieldRef)j.next();
                String fieldName = fieldRef.getName();

                FieldMapping fieldMapping = fieldRef.getFieldMapping();

                Object value = interpreter.eval(fieldMapping);
                if (value == null) {
                    //if (debug) log.debug("Field "+fieldName+" is null.");
                    continue;
                }

                setTableAlias(sourceName, alias);
                SimpleFilter sf = new SimpleFilter(alias+"."+fieldName, operator, value);

                f = FilterTool.appendAndFilter(f, sf);
                if (debug) log.debug(" - Filter "+sf);
            }

            newFilter = FilterTool.appendOrFilter(newFilter, f);
        }

        return newFilter;
    }

    public Filter convert(
            SubstringFilter filter
    ) throws Exception {

        boolean debug = log.isDebugEnabled();
        if (debug) log.debug("Converting filter "+filter);

        String attributeName = filter.getAttribute();
        Collection substrings = filter.getSubstrings();

        AttributeMapping attributeMapping = entryMapping.getAttributeMapping(attributeName);
        String variable = attributeMapping.getVariable();

        if (variable == null) {
            if (debug) log.debug("Attribute "+attributeName+" is not mapped to a variable.");
            return null;
        }

        int index = variable.indexOf(".");
        String sourceName = variable.substring(0, index);
        String fieldName = variable.substring(index+1);

        String alias = createTableAlias(sourceName);
        setTableAlias(sourceName, alias);

        StringBuilder sb = new StringBuilder();
        for (Iterator j=substrings.iterator(); j.hasNext(); ) {
            Object o = j.next();
            if (o.equals(SubstringFilter.STAR)) {
                sb.append("%");
            } else {
                String substring = (String)o;
                sb.append(substring);
            }
        }

        String value = sb.toString();

        SimpleFilter f = new SimpleFilter(alias+"."+fieldName, "like", value);
        if (debug) log.debug(" - Filter "+f);

        return f;
    }

    public Filter convert(
            PresentFilter filter
    ) throws Exception {

        boolean debug = log.isDebugEnabled();
        if (debug) log.debug("Converting filter "+filter);

        String attributeName = filter.getAttribute();

        if (attributeName.equalsIgnoreCase("objectClass")) return null;

        Filter newFilter = null;

        for (Iterator i= sourceRefs.values().iterator(); i.hasNext(); ) {
            SourceRef sourceRef = (SourceRef)i.next();

            String sourceName = sourceRef.getAlias();
            String alias = createTableAlias(sourceName);

            for (Iterator j= sourceRef.getFieldRefs().iterator(); j.hasNext(); ) {
                FieldRef fieldRef = (FieldRef)j.next();
                FieldMapping fieldMapping = fieldRef.getFieldMapping();
                String fieldName = fieldRef.getName();

                String variable = fieldMapping.getVariable();
                if (variable == null) {
                    Expression expression = fieldMapping.getExpression();
                    if (expression != null) {
                        variable = expression.getForeach();
                    }
                }

                if (variable == null) {
                    //if (debug) log.debug("Attribute "+attributeName+" can't be converted.");
                    continue;
                }

                if (!attributeName.equalsIgnoreCase(variable)) {
                    //if (debug) log.debug("Attribute "+attributeName+" doesn't match "+variable);
                    continue;
                }

                setTableAlias(sourceName, alias);

                PresentFilter f = new PresentFilter(alias+"."+fieldName);
                if (debug) log.debug(" - Filter "+f);

                newFilter = FilterTool.appendAndFilter(newFilter, f);
            }
        }

        return newFilter;
    }

    public void append(Filter filter) throws Exception {
        sourceFilter = FilterTool.appendAndFilter(sourceFilter, convert(filter));
    }

    public Filter getFilter() {
        return sourceFilter;
    }

    public String createTableAlias(String sourceName) {
        int counter = 2;
        String alias = sourceName+counter;

        while (sourceRefs.get(alias) != null) {
            counter++;
            alias = sourceName+counter;
        }

        return alias;
    }

    public void setTableAlias(String sourceName, String alias) {
        SourceRef sourceRef = (SourceRef) sourceRefs.get(sourceName);
        sourceAliases.put(alias, sourceRef);
    }

    public Map getSourceAliases() {
        return sourceAliases;
    }

    public void setSourceAliases(Map sourceAliases) {
        this.sourceAliases = sourceAliases;
    }
}
