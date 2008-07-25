package org.safehaus.penrose.adapter;

import org.safehaus.penrose.directory.EntryFieldConfig;
import org.safehaus.penrose.directory.EntryField;
import org.safehaus.penrose.directory.EntrySource;
import org.safehaus.penrose.filter.*;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.ldap.Attribute;
import org.safehaus.penrose.ldap.Attributes;
import org.safehaus.penrose.ldap.LDAP;
import org.safehaus.penrose.ldap.SourceAttributes;
import org.safehaus.penrose.mapping.Expression;
import org.safehaus.penrose.partition.Partition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author Endi S. Dewata
 */
public class FilterBuilder {

    public Logger log = LoggerFactory.getLogger(getClass());
    public boolean debug = log.isDebugEnabled();

    Partition partition;

    Map<String, EntrySource> sources = new LinkedHashMap<String, EntrySource>();

    Interpreter interpreter;

    Filter filter;

    public FilterBuilder(
            Partition partition,
            Collection<EntrySource> sources,
            SourceAttributes sourceAttributes,
            Interpreter interpreter
    ) throws Exception {

        this.partition = partition;

        for (EntrySource source : sources) {
            this.sources.put(source.getAlias(), source);
        }

        this.interpreter = interpreter;

        if (debug) log.debug("Creating filters:");

        for (String sourceName : sourceAttributes.getNames()) {

            EntrySource source = this.sources.get(sourceName);
            if (source == null) continue;

            Attributes attributes = sourceAttributes.get(sourceName);

            for (String fieldName : attributes.getNames()) {

                EntryField field;

                if (fieldName.startsWith("primaryKey.")) {
                    fieldName = fieldName.substring(11);
                    field = source.getPrimaryKeyField(fieldName);

                } else {
                    field = source.getField(fieldName);
                }

                if (field == null) {
                    log.error("Unknown field "+fieldName);
                    throw LDAP.createException(LDAP.OPERATIONS_ERROR);
                }

                Collection<String> operations = field.getOperations();
                if (!operations.isEmpty() && !operations.contains(EntryFieldConfig.SEARCH)) continue;

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

        if (debug) log.debug("Converting filter "+filter);

        String attributeName = filter.getAttribute();
        String operator = filter.getOperator();
        Object attributeValue = filter.getValue();

        interpreter.set(attributeName, attributeValue);

        Filter newFilter = null;
        for (EntrySource sourceRef : sources.values()) {

            for (EntryField fieldRef : sourceRef.getFields()) {

                Collection<String> operations = fieldRef.getOperations();
                if (!operations.isEmpty() && !operations.contains(EntryFieldConfig.SEARCH)) continue;

                String fieldName = fieldRef.getName();

                Object value = interpreter.eval(fieldRef);
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

        if (debug) log.debug("Converting filter "+filter);

        String attributeName = filter.getAttribute();
        Collection<Object> substrings = filter.getSubstrings();

        Filter f = null;

        for (EntrySource sourceRef : sources.values()) {
            for (EntryField fieldRef : sourceRef.getFields()) {

                String variable = fieldRef.getVariable();
                if (variable == null || !attributeName.equals(variable)) continue;

                Collection<String> operations = fieldRef.getOperations();
                if (!operations.isEmpty() && !operations.contains(EntryFieldConfig.SEARCH)) continue;

                SubstringFilter sf = new SubstringFilter(fieldRef.getName(), substrings);
                if (debug) log.debug(" - Filter "+sf);

                f = FilterTool.appendAndFilter(f, sf);
            }
        }

        return f;
    }

    public Filter convert(
            PresentFilter filter
    ) throws Exception {

        if (debug) log.debug("Converting filter "+filter);

        String attributeName = filter.getAttribute();

        if (attributeName.equalsIgnoreCase("objectClass")) return null;

        Filter newFilter = null;

        for (EntrySource sourceRef : sources.values()) {

            for (EntryField fieldRef : sourceRef.getFields()) {

                Collection<String> operations = fieldRef.getOperations();
                if (!operations.isEmpty() && !operations.contains(EntryFieldConfig.SEARCH)) continue;

                String fieldName = fieldRef.getName();

                String variable = fieldRef.getVariable();
                if (variable == null) {
                    Expression expression = fieldRef.getExpression();
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
