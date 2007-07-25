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
package org.safehaus.penrose.jdbc.adapter;

import org.safehaus.penrose.filter.*;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.entry.SourceValues;
import org.safehaus.penrose.source.SourceRef;
import org.safehaus.penrose.source.FieldRef;
import org.safehaus.penrose.ldap.Attributes;
import org.safehaus.penrose.ldap.Attribute;
import org.safehaus.penrose.partition.Partition;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class SearchFilterBuilder {

    public Logger log = LoggerFactory.getLogger(getClass());
    public boolean debug = log.isDebugEnabled();

    Partition partition;
    EntryMapping entryMapping;

    Map<String,SourceRef> sourceRefs = new LinkedHashMap<String,SourceRef>(); // need to maintain order

    Interpreter interpreter;

    Map<String,SourceRef> sourceAliases = new LinkedHashMap<String,SourceRef>(); // need to maintain order
    Filter sourceFilter;

    public SearchFilterBuilder(
            Interpreter interpreter,
            Partition partition,
            EntryMapping entryMapping,
            Collection<SourceRef> sourceRefs,
            SourceValues sourceValues
    ) throws Exception {

        this.partition = partition;
        this.entryMapping = entryMapping;

        Set<String> aliases = new HashSet<String>();

        for (SourceRef sourceRef : sourceRefs) {

            String alias = sourceRef.getAlias();
            aliases.add(alias);

            if (entryMapping.getSourceMapping(alias) == null) continue;
            this.sourceRefs.put(alias, sourceRef);
        }

        this.interpreter = interpreter;

        if (debug) log.debug("Creating filters:");

        for (String sourceName : sourceValues.getNames()) {
            if (!aliases.contains(sourceName)) continue;

            Attributes attributes = sourceValues.get(sourceName);

            for (String fieldName : attributes.getNames()) {

                Attribute attribute = attributes.get(fieldName);

                Filter of = null;

                for (Object value : attribute.getValues()) {
                    SimpleFilter f = new SimpleFilter(sourceName + "." + fieldName, "=", value);
                    if (debug) log.debug(" - Filter " + f);

                    of = FilterTool.appendOrFilter(of, f);
                }

                sourceFilter = FilterTool.appendAndFilter(sourceFilter, of);
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

        Collection<Filter> filters = filter.getFilters();
        for (Filter f : filters) {
            Filter nf = convert(f);
            newFilter = FilterTool.appendAndFilter(newFilter, nf);
        }

        return newFilter;
    }

    public Filter convert(OrFilter filter) throws Exception {

        Filter newFilter = null;

        Collection<Filter> filters = filter.getFilters();
        for (Filter f : filters) {
            Filter nf = convert(f);
            newFilter = FilterTool.appendOrFilter(newFilter, nf);
        }

        return newFilter;
    }

    public Filter convert(
            SimpleFilter filter
    ) throws Exception {

        if (debug) log.debug("Converting filter "+filter);

        String attributeName = filter.getAttribute();
        String operator = filter.getOperator();
        Object attributeValue = filter.getValue();

        interpreter.set(attributeName, attributeValue);

        Filter newFilter = null;
        for (SourceRef sourceRef : sourceRefs.values()) {
            String sourceName = sourceRef.getAlias();

            String alias = createTableAlias(sourceName);

            Filter f = null;
            for (FieldRef fieldRef : sourceRef.getFieldRefs()) {
                String fieldName = fieldRef.getName();

                FieldMapping fieldMapping = fieldRef.getFieldMapping();

                Object value = interpreter.eval(fieldMapping);
                if (value == null) {
                    //if (debug) log.debug("Field "+fieldName+" is null.");
                    continue;
                }

                setTableAlias(sourceName, alias);
                SimpleFilter sf = new SimpleFilter(alias + "." + fieldName, operator, value);

                f = FilterTool.appendAndFilter(f, sf);
                if (debug) log.debug(" - Filter " + sf);
            }

            newFilter = FilterTool.appendOrFilter(newFilter, f);
        }

        return newFilter;
    }

    public Filter convert(
            SubstringFilter filter
    ) throws Exception {

        if (debug) log.debug("Converting filter "+filter);

        String attributeName = filter.getAttribute();
        Collection<Object> substrings = filter.getSubstrings();

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
        for (Object o : substrings) {
            if (o.equals(SubstringFilter.STAR)) {
                sb.append("%");
            } else {
                String substring = (String) o;
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

        if (debug) log.debug("Converting filter "+filter);

        String attributeName = filter.getAttribute();

        if (attributeName.equalsIgnoreCase("objectClass")) return null;

        Filter newFilter = null;

        for (SourceRef sourceRef : sourceRefs.values()) {

            String sourceName = sourceRef.getAlias();
            String alias = createTableAlias(sourceName);

            for (FieldRef fieldRef : sourceRef.getFieldRefs()) {
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

                PresentFilter f = new PresentFilter(alias + "." + fieldName);
                if (debug) log.debug(" - Filter " + f);

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

    public boolean isPrimarySource(String sourceName) {
        EntryMapping em = entryMapping;

        while (em != null) {
            SourceMapping sourceMapping = em.getSourceMapping(0);
            if (sourceMapping != null && sourceMapping.getName().equals(sourceName)) return true;
            em = partition.getMappings().getParent(em);
        }

        return false;
    }

    public String createTableAlias(String sourceName) {
        if (isPrimarySource(sourceName)) return sourceName;

        int counter = 2;
        String alias = sourceName+counter;

        while (sourceRefs.get(alias) != null) {
            counter++;
            alias = sourceName+counter;
        }

        return alias;
    }

    public void setTableAlias(String sourceName, String alias) {
        if (isPrimarySource(sourceName)) return;

        SourceRef sourceRef = sourceRefs.get(sourceName);
        sourceAliases.put(alias, sourceRef);
    }

    public Map<String,SourceRef> getSourceAliases() {
        return sourceAliases;
    }

    public void setSourceAliases(Map<String,SourceRef> sourceAliases) {
        this.sourceAliases = sourceAliases;
    }
}
