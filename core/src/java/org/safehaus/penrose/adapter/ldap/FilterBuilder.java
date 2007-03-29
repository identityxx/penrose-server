package org.safehaus.penrose.adapter.ldap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.filter.*;
import org.safehaus.penrose.entry.AttributeValues;

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

    Filter filter;

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
            String fieldName = name.substring(p+1);

            for (Iterator j=values.iterator(); j.hasNext(); ) {
                Object value = j.next();

                SimpleFilter f = new SimpleFilter(fieldName, "=", value.toString());
                if (debug) log.debug(" - Filter "+f);

                filter = FilterTool.appendAndFilter(filter, f);
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

            Collection fields = sourceMapping.getFieldMappings();
            for (Iterator j=fields.iterator(); j.hasNext(); ) {
                FieldMapping fieldMapping = (FieldMapping)j.next();
                String fieldName = fieldMapping.getName();

                Object value = interpreter.eval(fieldMapping);
                if (value == null) {
                    //if (debug) log.debug("Field "+fieldName+" is null.");
                    continue;
                }

                SimpleFilter f = new SimpleFilter(fieldName, operator, value.toString());
                if (debug) log.debug(" - Filter "+f);

                newFilter = FilterTool.appendAndFilter(newFilter, f);
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
        String fieldName = variable.substring(index+1);

        SubstringFilter f = new SubstringFilter(fieldName, substrings);
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

                PresentFilter f = new PresentFilter(fieldName);
                if (debug) log.debug(" - Filter "+f);

                newFilter = FilterTool.appendAndFilter(newFilter, f);
            }
        }

        return newFilter;
    }

    public void append(Filter filter) throws Exception {
        this.filter = FilterTool.appendAndFilter(this.filter, convert(filter));
    }

    public Filter getFilter() {
        return filter;
    }
}
