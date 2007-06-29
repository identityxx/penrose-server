package org.safehaus.penrose.adapter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.filter.*;
import org.safehaus.penrose.entry.SourceValues;
import org.safehaus.penrose.source.SourceRef;
import org.safehaus.penrose.source.FieldRef;
import org.safehaus.penrose.ldap.Attributes;
import org.safehaus.penrose.ldap.Attribute;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class FilterBuilder {

    Logger log = LoggerFactory.getLogger(getClass());

    Partition partition;
    EntryMapping entryMapping;

    Map<String,SourceRef> sourceRefs = new LinkedHashMap<String,SourceRef>();

    Interpreter interpreter;

    Filter filter;

    public FilterBuilder(
            Partition partition,
            EntryMapping entryMapping,
            Collection<SourceRef> sourceRefs,
            SourceValues sourceValues,
            Interpreter interpreter
    ) throws Exception {

        this.partition = partition;
        this.entryMapping = entryMapping;

        for (SourceRef sourceRef : sourceRefs) {
            this.sourceRefs.put(sourceRef.getAlias(), sourceRef);
        }

        this.interpreter = interpreter;

        boolean debug = log.isDebugEnabled();
        if (debug) log.debug("Creating filters:");

        for (String sourceName : sourceValues.getNames()) {

            SourceRef sourceRef = this.sourceRefs.get(sourceName);
            if (sourceRef == null) continue;

            Attributes attributes = sourceValues.get(sourceName);

            for (String fieldName : attributes.getNames()) {

                FieldRef fieldRef = sourceRef.getFieldRef(fieldName);

                Collection<String> operations = fieldRef.getOperations();
                if (!operations.isEmpty() && !operations.contains(FieldMapping.SEARCH)) continue;

                Attribute attribute = attributes.get(fieldName);

                for (Object value : attribute.getValues()) {
                    SimpleFilter f = new SimpleFilter(fieldName, "=", value);
                    if (debug) log.debug(" - Filter " + f);

                    filter = FilterTool.appendAndFilter(filter, f);
                }
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

        boolean debug = log.isDebugEnabled();
        if (debug) log.debug("Converting filter "+filter);

        String attributeName = filter.getAttribute();
        String operator = filter.getOperator();
        Object attributeValue = filter.getValue();

        interpreter.set(attributeName, attributeValue);

        Filter newFilter = null;
        for (SourceRef sourceRef : sourceRefs.values()) {

            for (FieldRef fieldRef : sourceRef.getFieldRefs()) {
                FieldMapping fieldMapping = fieldRef.getFieldMapping();
                String fieldName = fieldMapping.getName();

                Object value = interpreter.eval(fieldMapping);
                if (value == null) {
                    //if (debug) log.debug("Field "+fieldName+" is null.");
                    continue;
                }

                SimpleFilter f = new SimpleFilter(fieldName, operator, value.toString());
                if (debug) log.debug(" - Filter " + f);

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
        Collection<Object> substrings = filter.getSubstrings();

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

        for (SourceRef sourceRef : sourceRefs.values()) {

            for (FieldRef fieldRef : sourceRef.getFieldRefs()) {
                FieldMapping fieldMapping = fieldRef.getFieldMapping();
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
                if (debug) log.debug(" - Filter " + f);

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
