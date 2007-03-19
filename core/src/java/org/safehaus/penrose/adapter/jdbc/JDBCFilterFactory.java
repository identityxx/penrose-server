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
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class JDBCFilterFactory {

    Logger log = LoggerFactory.getLogger(getClass());

    Partition partition;
    EntryMapping entryMapping;

    Collection sourceMappings;
    SourceMapping primarySourceMapping;

    Interpreter interpreter;

    Collection parameterValues;
    Collection parameterFieldConfigs;

    Map tableAliases = new HashMap();

    public JDBCFilterFactory(
            Partition partition,
            EntryMapping entryMapping,
            SourceMapping sourceMapping,
            Interpreter interpreter,
            Collection parameterValues,
            Collection parameterFieldConfigs
    ) {
        this.partition = partition;
        this.entryMapping = entryMapping;
        this.interpreter = interpreter;
        this.parameterValues = parameterValues;
        this.parameterFieldConfigs = parameterFieldConfigs;
    }

    public JDBCFilterFactory(
            Partition partition,
            EntryMapping entryMapping,
            Collection sourceMappings,
            Interpreter interpreter,
            Collection parameterValues,
            Collection parameterFieldConfigs
    ) {
        this.partition = partition;
        this.entryMapping = entryMapping;

        this.sourceMappings = sourceMappings;
        primarySourceMapping = (SourceMapping)sourceMappings.iterator().next();

        this.interpreter = interpreter;
        this.parameterValues = parameterValues;
        this.parameterFieldConfigs = parameterFieldConfigs;
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
            if (nf == null) continue;

            if (newFilter == null) {
                newFilter = nf;

            } else if (newFilter instanceof AndFilter) {
                AndFilter andFilter = (AndFilter)newFilter;
                andFilter.addFilter(nf);

            } else {
                AndFilter andFilter = new AndFilter();
                andFilter.addFilter(newFilter);
                andFilter.addFilter(nf);
                newFilter = andFilter;
            }
        }

        return newFilter;
    }

    public Filter convert(OrFilter filter) throws Exception {

        Filter newFilter = null;

        Collection filters = filter.getFilters();
        for (Iterator i=filters.iterator(); i.hasNext(); ) {
            Filter f = (Filter)i.next();

            Filter nf = convert(f);
            if (nf == null) continue;

            if (newFilter == null) {
                newFilter = nf;

            } else if (newFilter instanceof OrFilter) {
                OrFilter orFilter = (OrFilter)newFilter;
                orFilter.addFilter(nf);

            } else {
                OrFilter orFilter = new OrFilter();
                orFilter.addFilter(newFilter);
                orFilter.addFilter(nf);
                newFilter = orFilter;
            }
        }

        return newFilter;
    }

    public Filter convert(
            SimpleFilter filter
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        String attributeName = filter.getAttribute();
        String operator = filter.getOperator();
        String attributeValue = filter.getValue();

        if (attributeName.equalsIgnoreCase("objectClass")) {
            if (attributeValue.equals("*")) return null;
        }

        if (attributeValue.startsWith("'") && attributeValue.endsWith("'")) {
            attributeValue = attributeValue.substring(1, attributeValue.length()-1);
        }

        interpreter.set(attributeName, attributeValue);

        Filter newFilter = null;
        for (Iterator i=sourceMappings.iterator(); i.hasNext(); ) {
            SourceMapping sourceMapping = (SourceMapping)i.next();
            String sourceName = sourceMapping.getName();

            Collection fields = sourceMapping.getFieldMappings();
            for (Iterator j=fields.iterator(); j.hasNext(); ) {
                FieldMapping fieldMapping = (FieldMapping)j.next();
                String fieldName = fieldMapping.getName();

                String fieldValue = (String)interpreter.eval(entryMapping, fieldMapping);
                if (fieldValue == null) {
                    //if (debug) log.debug("Field "+fieldName+" is null.");
                    continue;
                }

                String alias = createTableAlias(sourceName);

                SimpleFilter f = new SimpleFilter(alias+"."+fieldName, operator, fieldValue);
                newFilter = FilterTool.appendAndFilter(newFilter, f);
            }
        }

        return newFilter;
    }

    public Filter convert(
            SubstringFilter filter
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

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

        Filter newFilter = null;
        for (Iterator i=sourceMappings.iterator(); i.hasNext(); ) {
            SourceMapping sourceMapping = (SourceMapping)i.next();
            if (!sourceName.equals(sourceMapping.getName())) continue;

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

            String alias = createTableAlias(sourceName);

            Filter f = new SimpleFilter(alias+"."+fieldName, "like", sb.toString());
            newFilter = FilterTool.appendAndFilter(newFilter, f);
        }

        return newFilter;
    }

    public String createTableAlias(String sourceName) {
        if (sourceName.equals(primarySourceMapping.getName())) return sourceName;

        int counter = 2;
        String alias = sourceName+counter;

        while (entryMapping.getSourceMapping(alias) != null) {
            counter++;
            alias = sourceName+counter;
        }

        tableAliases.put(alias, sourceName);

        return alias;
    }

    public String getSourceName(String alias) {
        String sourceName = (String)tableAliases.get(alias);
        return sourceName == null ? alias : sourceName;
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
        }
    }

    public void generate(
            SimpleFilter filter,
            StringBuilder sb
    ) throws Exception {

        String name = filter.getAttribute();
        String operator = filter.getOperator();
        String value = filter.getValue();

        int i = name.indexOf('.');
        String alias = name.substring(0, i);
        String fieldName = name.substring(i+1);

        String sourceName = getSourceName(alias);

        SourceMapping sourceMapping = entryMapping.getSourceMapping(sourceName);
        SourceConfig sourceConfig = partition.getSourceConfig(sourceMapping);
        Collection fieldMappings = sourceMapping.getFieldMappings(fieldName);

        FieldMapping fieldMapping = (FieldMapping)fieldMappings.iterator().next();
        FieldConfig fieldConfig = sourceConfig.getFieldConfig(fieldMapping.getName());

        if ("VARCHAR".equals(fieldConfig.getType())) {

            if (fieldConfig.isCaseSensitive()) {
                sb.append(alias);
                sb.append(".");
                sb.append(fieldConfig.getOriginalName());
                sb.append(" ");
                sb.append(operator);
                sb.append(" ");
                sb.append("?");

            } else {
                sb.append("lower(");
                sb.append(alias);
                sb.append(".");
                sb.append(fieldConfig.getOriginalName());
                sb.append(")");
                sb.append(" ");
                sb.append(operator);
                sb.append(" ");
                sb.append("lower(");
                sb.append("?");
                sb.append(")");
            }

        } else {
            sb.append(alias);
            sb.append(".");
            sb.append(fieldConfig.getOriginalName());
            sb.append(" ");
            sb.append(operator);
            sb.append(" ?");
        }

        parameterValues.add(value);
        parameterFieldConfigs.add(fieldConfig);
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

    public Map getTableAliases() {
        return tableAliases;
    }

    public void setTableAliases(Map tableAliases) {
        this.tableAliases = tableAliases;
    }
}
