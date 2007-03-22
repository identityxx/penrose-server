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
import org.safehaus.penrose.partition.FieldConfig;
import org.safehaus.penrose.partition.SourceConfig;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.entry.AttributeValues;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class FilterBuilder {

    Logger log = LoggerFactory.getLogger(getClass());

    Partition partition;
    EntryMapping entryMapping;

    Collection sourceMappings;
    SourceMapping primarySourceMapping;

    Interpreter interpreter;

    Map tableAliases = new LinkedHashMap(); // need to maintain order
    Filter sourceFilter;
    Collection parameters = new ArrayList();

    public FilterBuilder(
            Partition partition,
            EntryMapping entryMapping,
            SourceMapping sourceMapping,
            Interpreter interpreter
    ) throws Exception {

        this.partition = partition;
        this.entryMapping = entryMapping;

        this.interpreter = interpreter;
    }

    public FilterBuilder(
            Partition partition,
            EntryMapping entryMapping,
            Collection sourceMappings,
            AttributeValues sourceValues,
            Interpreter interpreter
    ) throws Exception {

        this.partition = partition;
        this.entryMapping = entryMapping;

        this.sourceMappings = sourceMappings;
        primarySourceMapping = (SourceMapping)sourceMappings.iterator().next();

        this.interpreter = interpreter;

        boolean debug = log.isDebugEnabled();
        if (debug) log.debug("Creating filters:");

        for (Iterator i=sourceValues.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Collection values = sourceValues.get(name);

            int p = name.indexOf(".");
            String sourceName = name.substring(0, p);
            String fieldName = name.substring(p+1);

            SourceMapping sourceMapping = entryMapping.getSourceMapping(sourceName);
            SourceConfig sourceConfig = partition.getSourceConfig(sourceMapping);

            String alias = createTableAlias(sourceName);
            setTableAlias(sourceName, alias);

            for (Iterator j=values.iterator(); j.hasNext(); ) {
                Object value = j.next();

                SimpleFilter f = new SimpleFilter(alias+"."+fieldName, "=", "?");
                if (debug) log.debug(" - Filter "+f);

                sourceFilter = FilterTool.appendAndFilter(sourceFilter, f);

                FieldConfig fieldConfig = sourceConfig.getFieldConfig(fieldName);
                parameters.add(new Parameter(fieldConfig, value));
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
        String attributeValue = filter.getValue();

        if (attributeValue.startsWith("'") && attributeValue.endsWith("'")) {
            attributeValue = attributeValue.substring(1, attributeValue.length()-1);
        }

        interpreter.set(attributeName, attributeValue);

        Filter newFilter = null;
        for (Iterator i=sourceMappings.iterator(); i.hasNext(); ) {
            SourceMapping sourceMapping = (SourceMapping)i.next();

            String sourceName = sourceMapping.getName();
            SourceConfig sourceConfig = partition.getSourceConfig(sourceMapping);

            String alias = createTableAlias(sourceName);

            Collection fields = sourceMapping.getFieldMappings();
            for (Iterator j=fields.iterator(); j.hasNext(); ) {
                FieldMapping fieldMapping = (FieldMapping)j.next();
                String fieldName = fieldMapping.getName();

                Object value = interpreter.eval(entryMapping, fieldMapping);
                if (value == null) {
                    //if (debug) log.debug("Field "+fieldName+" is null.");
                    continue;
                }

                setTableAlias(sourceName, alias);

                SimpleFilter f = new SimpleFilter(alias+"."+fieldName, operator, "?");
                if (debug) log.debug(" - Filter "+f);
                
                newFilter = FilterTool.appendAndFilter(newFilter, f);

                FieldConfig fieldConfig = sourceConfig.getFieldConfig(fieldName);
                parameters.add(new Parameter(fieldConfig, value));
            }
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

        SimpleFilter f = new SimpleFilter(alias+"."+fieldName, "like", sb.toString());
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

        for (Iterator i=sourceMappings.iterator(); i.hasNext(); ) {
            SourceMapping sourceMapping = (SourceMapping)i.next();
            String sourceName = sourceMapping.getName();
            String alias = createTableAlias(sourceName);

            Collection fields = sourceMapping.getFieldMappings();
            for (Iterator j=fields.iterator(); j.hasNext(); ) {
                FieldMapping fieldMapping = (FieldMapping)j.next();
                String fieldName = fieldMapping.getName();

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


    public String generate() throws Exception {
        return generate(sourceFilter);
    }

    public String generate(Filter filter) throws Exception {
        StringBuilder sb = new StringBuilder();
        generate(filter, sb);
        return sb.toString();
    }

    public void generate(Filter filter, StringBuilder sb) throws Exception {

        if (filter instanceof NotFilter) {
            generate((NotFilter)filter, sb);

        } else if (filter instanceof AndFilter) {
            generate((AndFilter)filter, sb);

        } else if (filter instanceof OrFilter) {
            generate((OrFilter)filter, sb);

        } else if (filter instanceof SimpleFilter) {
            generate((SimpleFilter)filter, sb);

        } else if (filter instanceof PresentFilter) {
            generate((PresentFilter)filter, sb);
        }
    }

    public void generate(
            SimpleFilter filter,
            StringBuilder sb
    ) throws Exception {

        String name = filter.getAttribute();
        String operator = filter.getOperator();

        int i = name.indexOf('.');
        String alias = name.substring(0, i);
        String fieldName = name.substring(i+1);

        String sourceName = getSourceName(alias);

        SourceMapping sourceMapping = entryMapping.getSourceMapping(sourceName);
        SourceConfig sourceConfig = partition.getSourceConfig(sourceMapping);
        Collection fieldMappings = sourceMapping.getFieldMappings(fieldName);

        FieldMapping fieldMapping = (FieldMapping)fieldMappings.iterator().next();
        FieldConfig fieldConfig = sourceConfig.getFieldConfig(fieldMapping.getName());

        generate(
                fieldConfig,
                alias+"."+fieldConfig.getOriginalName(),
                operator,
                "?",
                sb
        );
    }

    public void generate(
            FieldConfig fieldConfig,
            String lhs,
            String operator,
            String rhs,
            StringBuilder sb
    ) throws Exception {

        if ("VARCHAR".equals(fieldConfig.getType()) && !fieldConfig.isCaseSensitive()) {
            sb.append("lower(");
            sb.append(lhs);
            sb.append(")");
            sb.append(" ");
            sb.append(operator);
            sb.append(" ");
            sb.append("lower(");
            sb.append(rhs);
            sb.append(")");

        } else {
            sb.append(lhs);
            sb.append(" ");
            sb.append(operator);
            sb.append(" ");
            sb.append(rhs);
        }
    }

    public void generate(
            PresentFilter filter,
            StringBuilder sb
    ) throws Exception {

        String name = filter.getAttribute();
        
        sb.append(name);
        sb.append(" is not null");
    }

    public void generate(
            NotFilter filter,
            StringBuilder sb
    ) throws Exception {

        StringBuilder sb2 = new StringBuilder();

        Filter f = filter.getFilter();

        generate(
                f,
                sb2
        );

        sb.append("not (");
        sb.append(sb2);
        sb.append(")");
    }

    public void generate(
            AndFilter filter,
            StringBuilder sb
    ) throws Exception {

        StringBuilder sb2 = new StringBuilder();
        for (Iterator i = filter.getFilters().iterator(); i.hasNext();) {
            Filter f = (Filter) i.next();

            StringBuilder sb3 = new StringBuilder();

            generate(
                    f,
                    sb3
            );

            if (sb2.length() > 0 && sb3.length() > 0) {
                sb2.append(" and ");
            }

            sb2.append(sb3);
        }

        if (sb2.length() == 0) return;

        sb.append("(");
        sb.append(sb2);
        sb.append(")");
    }

    public void generate(
            OrFilter filter,
            StringBuilder sb
    ) throws Exception {

        StringBuilder sb2 = new StringBuilder();
        for (Iterator i = filter.getFilters().iterator(); i.hasNext();) {
            Filter f = (Filter) i.next();

            StringBuilder sb3 = new StringBuilder();

            generate(
                    f,
                    sb3
            );

            if (sb2.length() > 0 && sb3.length() > 0) {
                sb2.append(" or ");
            }

            sb2.append(sb3);
        }

        if (sb2.length() == 0) return;

        sb.append("(");
        sb.append(sb2);
        sb.append(")");
    }

    public String createTableAlias(String sourceName) {
        if (sourceName.equals(primarySourceMapping.getName())) return sourceName;

        int counter = 2;
        String alias = sourceName+counter;

        while (entryMapping.getSourceMapping(alias) != null) {
            counter++;
            alias = sourceName+counter;
        }

        return alias;
    }

    public void setTableAlias(String sourceName, String alias) {
        if (sourceName.equals(primarySourceMapping.getName())) return;
        tableAliases.put(alias, sourceName);
    }

    public String getSourceName(String alias) {
        String sourceName = (String)tableAliases.get(alias);
        return sourceName == null ? alias : sourceName;
    }

    public Map getTableAliases() {
        return tableAliases;
    }

    public void setTableAliases(Map tableAliases) {
        this.tableAliases = tableAliases;
    }

    public Collection getParameters() {
        return parameters;
    }

    public void setParameters(Collection parameters) {
        this.parameters = parameters;
    }
}
