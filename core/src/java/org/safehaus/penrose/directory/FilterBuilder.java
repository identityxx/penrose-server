package org.safehaus.penrose.directory;

import org.safehaus.penrose.filter.*;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.ldap.Attributes;
import org.safehaus.penrose.ldap.SourceAttributes;
import org.safehaus.penrose.mapping.Expression;
import org.safehaus.penrose.mapping.Mapping;
import org.safehaus.penrose.mapping.MappingRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class FilterBuilder {

    public Logger log = LoggerFactory.getLogger(getClass());
    public boolean debug = log.isDebugEnabled();

    Entry entry;
    SourceAttributes sourceAttributes;
    Interpreter interpreter;

    public FilterBuilder(
            Entry entry,
            SourceAttributes sourceAttributes,
            Interpreter interpreter
    ) throws Exception {

        this.entry = entry;
        this.sourceAttributes = sourceAttributes;
        this.interpreter = interpreter;
    }

    public FilterBuilder(
            Entry entry,
            Interpreter interpreter
    ) throws Exception {

        this.entry = entry;
        this.interpreter = interpreter;
    }

    public Filter convert(
            Filter filter,
            EntrySource source
    ) throws Exception {

        if (filter instanceof NotFilter) {
            return convert((NotFilter)filter, source);

        } else if (filter instanceof AndFilter) {
            return convert((AndFilter)filter, source);

        } else if (filter instanceof OrFilter) {
            return convert((OrFilter)filter, source);

        } else if (filter instanceof SimpleFilter) {
            return convert((SimpleFilter)filter, source);

        } else if (filter instanceof SubstringFilter) {
            return convert((SubstringFilter)filter, source);

        } else if (filter instanceof PresentFilter) {
            return convert((PresentFilter)filter, source);
        }

        return null;
    }

    public Filter convert(NotFilter filter, EntrySource source) throws Exception {
        Filter newFilter = convert(filter.getFilter(), source);
        return new NotFilter(newFilter);
    }

    public Filter convert(AndFilter filter, EntrySource source) throws Exception {

        Filter newFilter = null;

        Collection<Filter> filters = filter.getFilters();
        for (Filter f : filters) {
            Filter nf = convert(f, source);
            newFilter = FilterTool.appendAndFilter(newFilter, nf);
        }

        return newFilter;
    }

    public Filter convert(OrFilter filter, EntrySource source) throws Exception {

        Filter newFilter = null;

        Collection<Filter> filters = filter.getFilters();
        for (Filter f : filters) {
            Filter nf = convert(f, source);
            newFilter = FilterTool.appendOrFilter(newFilter, nf);
        }

        return newFilter;
    }

    public Filter convert(SimpleFilter filter, EntrySource source) throws Exception {

        if (debug) log.debug("Converting filter "+filter);

        String attributeName = filter.getAttribute();
        String operator = filter.getOperator();
        Object attributeValue = filter.getValue();

        interpreter.set(sourceAttributes);
        interpreter.set(attributeName, attributeValue);

        Filter newFilter = null;

        String mappingName = source.getMappingName();
        if (mappingName != null) {
            Mapping mapping = entry.getPartition().getMappingManager().getMapping(mappingName);

            Attributes output = new Attributes();
            mapping.map(interpreter, output);

            for (String name : output.getNames()) {
                Object value = output.getValue(name);
                SimpleFilter f = new SimpleFilter(name, operator, value);
                if (debug) log.debug(" - Filter " + f);

                newFilter = FilterTool.appendAndFilter(newFilter, f);
            }

        } else {
            for (EntryField field : source.getFields()) {

                Collection<String> operations = field.getOperations();
                if (!operations.isEmpty() && !operations.contains(EntryFieldConfig.SEARCH)) continue;

                String fieldName = field.getName();

                Object value = interpreter.eval(field);
                if (value == null) {
                    //if (debug) log.debug("Field "+fieldName+" is null.");
                    continue;
                }

                SimpleFilter f = new SimpleFilter(fieldName, operator, value.toString());
                if (debug) log.debug(" - Filter " + f);

                newFilter = FilterTool.appendAndFilter(newFilter, f);
            }
        }

        interpreter.clear();

        return newFilter;
    }

    public Filter convert(SubstringFilter filter, EntrySource source) throws Exception {

        if (debug) log.debug("Converting filter "+filter);

        String attributeName = filter.getAttribute();
        Collection<Object> substrings = filter.getSubstrings();

        Filter newFilter = null;

        String mappingName = source.getMappingName();
        if (mappingName != null) {
            Mapping mapping = entry.getPartition().getMappingManager().getMapping(mappingName);

            for (MappingRule rule : mapping.getRules()) {

                String variable = rule.getVariable();
                if (variable == null || !attributeName.equals(variable)) continue;

                SubstringFilter sf = new SubstringFilter(rule.getName(), substrings);
                if (debug) log.debug(" - Filter "+sf);

                newFilter = FilterTool.appendAndFilter(newFilter, sf);
            }

        } else {
            
            for (EntryField field : source.getFields()) {

                String variable = field.getVariable();
                if (variable == null || !attributeName.equals(variable)) continue;

                Collection<String> operations = field.getOperations();
                if (!operations.isEmpty() && !operations.contains(EntryFieldConfig.SEARCH)) continue;

                SubstringFilter sf = new SubstringFilter(field.getName(), substrings);
                if (debug) log.debug(" - Filter "+sf);

                newFilter = FilterTool.appendAndFilter(newFilter, sf);
            }
        }

        return newFilter;
    }

    public Filter convert(PresentFilter filter, EntrySource source) throws Exception {

        if (debug) log.debug("Converting filter "+filter);

        String attributeName = filter.getAttribute();

        if (attributeName.equalsIgnoreCase("objectClass")) return null;

        Filter newFilter = null;

        String mappingName = source.getMappingName();
        if (mappingName != null) {
            Mapping mapping = entry.getPartition().getMappingManager().getMapping(mappingName);

            for (MappingRule rule : mapping.getRules()) {

                String fieldName = rule.getName();

                String variable = rule.getVariable();
                if (variable == null) {
                    Expression expression = rule.getExpression();
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

        } else {
            for (EntryField field : source.getFields()) {

                Collection<String> operations = field.getOperations();
                if (!operations.isEmpty() && !operations.contains(EntryFieldConfig.SEARCH)) continue;

                String fieldName = field.getName();

                String variable = field.getVariable();
                if (variable == null) {
                    Expression expression = field.getExpression();
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
}
