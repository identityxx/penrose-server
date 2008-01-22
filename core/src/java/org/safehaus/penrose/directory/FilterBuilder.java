package org.safehaus.penrose.directory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.filter.*;
import org.safehaus.penrose.mapping.Expression;

import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class FilterBuilder {

    public Logger log = LoggerFactory.getLogger(getClass());
    public boolean debug = log.isDebugEnabled();

    Entry entry;
    Interpreter interpreter;

    public FilterBuilder(
            Entry entry,
            Interpreter interpreter
    ) throws Exception {

        this.entry = entry;
        this.interpreter = interpreter;
    }

    public Filter convert(
            Filter filter,
            SourceRef sourceRef
    ) throws Exception {

        if (filter instanceof NotFilter) {
            return convert((NotFilter)filter, sourceRef);

        } else if (filter instanceof AndFilter) {
            return convert((AndFilter)filter, sourceRef);

        } else if (filter instanceof OrFilter) {
            return convert((OrFilter)filter, sourceRef);

        } else if (filter instanceof SimpleFilter) {
            return convert((SimpleFilter)filter, sourceRef);

        } else if (filter instanceof SubstringFilter) {
            return convert((SubstringFilter)filter, sourceRef);

        } else if (filter instanceof PresentFilter) {
            return convert((PresentFilter)filter, sourceRef);
        }

        return null;
    }

    public Filter convert(NotFilter filter, SourceRef sourceRef) throws Exception {
        Filter newFilter = convert(filter.getFilter(), sourceRef);
        return new NotFilter(newFilter);
    }

    public Filter convert(AndFilter filter, SourceRef sourceRef) throws Exception {

        Filter newFilter = null;

        Collection<Filter> filters = filter.getFilters();
        for (Filter f : filters) {
            Filter nf = convert(f, sourceRef);
            newFilter = FilterTool.appendAndFilter(newFilter, nf);
        }

        return newFilter;
    }

    public Filter convert(OrFilter filter, SourceRef sourceRef) throws Exception {

        Filter newFilter = null;

        Collection<Filter> filters = filter.getFilters();
        for (Filter f : filters) {
            Filter nf = convert(f, sourceRef);
            newFilter = FilterTool.appendOrFilter(newFilter, nf);
        }

        return newFilter;
    }

    public Filter convert(SimpleFilter filter, SourceRef sourceRef) throws Exception {

        if (debug) log.debug("Converting filter "+filter);

        String attributeName = filter.getAttribute();
        String operator = filter.getOperator();
        Object attributeValue = filter.getValue();

        interpreter.set(attributeName, attributeValue);

        Filter newFilter = null;

        for (FieldRef fieldRef : sourceRef.getFieldRefs()) {

            Collection<String> operations = fieldRef.getOperations();
            if (!operations.isEmpty() && !operations.contains(FieldMapping.SEARCH)) continue;

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

        interpreter.clear();

        return newFilter;
    }

    public Filter convert(SubstringFilter filter, SourceRef sourceRef) throws Exception {

        if (debug) log.debug("Converting filter "+filter);

        String attributeName = filter.getAttribute();
        Collection<Object> substrings = filter.getSubstrings();

        Filter newFilter = null;

        for (FieldRef fieldRef : sourceRef.getFieldRefs()) {

            String variable = fieldRef.getVariable();
            if (variable == null || !attributeName.equals(variable)) continue;

            Collection<String> operations = fieldRef.getOperations();
            if (!operations.isEmpty() && !operations.contains(FieldMapping.SEARCH)) continue;

            SubstringFilter sf = new SubstringFilter(fieldRef.getName(), substrings);
            if (debug) log.debug(" - Filter "+sf);

            newFilter = FilterTool.appendAndFilter(newFilter, sf);
        }

        return newFilter;
    }

    public Filter convert(PresentFilter filter, SourceRef sourceRef) throws Exception {

        if (debug) log.debug("Converting filter "+filter);

        String attributeName = filter.getAttribute();

        if (attributeName.equalsIgnoreCase("objectClass")) return null;

        Filter newFilter = null;

        for (FieldRef fieldRef : sourceRef.getFieldRefs()) {

            Collection<String> operations = fieldRef.getOperations();
            if (!operations.isEmpty() && !operations.contains(FieldMapping.SEARCH)) continue;

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

        return newFilter;
    }
}
