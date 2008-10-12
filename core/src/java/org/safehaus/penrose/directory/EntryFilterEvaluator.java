package org.safehaus.penrose.directory;

import org.safehaus.penrose.schema.AttributeType;
import org.safehaus.penrose.schema.matchingRule.EqualityMatchingRule;
import org.safehaus.penrose.schema.matchingRule.OrderingMatchingRule;
import org.safehaus.penrose.schema.matchingRule.SubstringsMatchingRule;
import org.safehaus.penrose.filter.*;
import org.safehaus.penrose.mapping.Mapping;
import org.safehaus.penrose.mapping.MappingRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class EntryFilterEvaluator {

    public Logger log = LoggerFactory.getLogger(getClass());
    public boolean debug = log.isDebugEnabled();

    public Entry entry;

    public EntryFilterEvaluator(Entry entry) throws Exception {
        this.entry = entry;
    }

    public boolean eval(Filter filter) throws Exception {
        //log.debug("Checking filter "+filter);

        boolean result = false;

        if (filter == null) {
            result = true;

        } else if (filter instanceof NotFilter) {
            result = eval((NotFilter)filter);

        } else if (filter instanceof AndFilter) {
            result = eval((AndFilter)filter);

        } else if (filter instanceof OrFilter) {
            result = eval((OrFilter)filter);

        } else if (filter instanceof SimpleFilter) {
            result = eval((SimpleFilter)filter);

        } else if (filter instanceof PresentFilter) {
            result = eval((PresentFilter)filter);

        } else if (filter instanceof SubstringFilter) {
            result = eval((SubstringFilter)filter);
        }

        // log.debug("=> "+filter+" ("+filter.getClass().getName()+"): "+result);

        return result;
    }

    public boolean eval(NotFilter filter) throws Exception {
        Filter f = filter.getFilter();
        return eval(f);
    }

    public boolean eval(AndFilter filter) throws Exception {
        for (Filter f : filter.getFilters()) {
            boolean result = eval(f);
            if (!result) return false;
        }
        return true;
    }

    public boolean eval(OrFilter filter) throws Exception {
        for (Filter f : filter.getFilters()) {
            boolean result = eval(f);
            if (result) return true;
        }
        return false;
    }

    public boolean eval(SimpleFilter filter) throws Exception {

        String attributeName = filter.getAttribute();
        String operator = filter.getOperator();
        Object attributeValue = filter.getValue();

        AttributeType attributeType = entry.getAttributeType(attributeName);

        Mapping mapping = entry.getMapping();

        if (mapping != null) {

            if (debug) log.debug("Checking "+mapping.getName()+" mapping.");

            for (MappingRule rule : mapping.getRules(attributeName)) {

                Object constant = rule.getConstant();
                if (constant == null) return true;

                if ("=".equals(operator)) {
                    String equality = attributeType == null ? null : attributeType.getEquality();
                    EqualityMatchingRule equalityMatchingRule = EqualityMatchingRule.getInstance(equality);

                    boolean b = equalityMatchingRule.compare(constant, attributeValue);
                    if (debug) log.debug(" - ["+constant+"] => "+b);

                    if (b) return true;

                } else if ("<=".equals(operator) || ">=".equals(operator)) {
                    String ordering = attributeType == null ? null : attributeType.getOrdering();
                    OrderingMatchingRule orderingMatchingRule = OrderingMatchingRule.getInstance(ordering);

                    int c = orderingMatchingRule.compare(constant, attributeValue);
                    if (debug) log.debug(" - ["+constant+"] => "+c);

                    if ("<=".equals(operator) && c > 0) return true;
                    if (">=".equals(operator) && c < 0) return true;

                } else {
                    throw new Exception("Unsupported operator \""+operator+"\" in \""+filter+"\"");
                }
            }

            return false;
        }

        if (attributeName.equalsIgnoreCase("objectclass")) {
            if (debug) log.debug("Checking object classes: "+entry.getObjectClasses());
            return entry.containsObjectClass(attributeValue.toString());
        }

        EntryAttributeConfig attributeMapping = entry.getAttributeConfig(attributeName);
        if (attributeMapping == null) {
            if (debug) log.debug(attributeName+" attribute undefined.");
            return false;
        }

        Object constant = attributeMapping.getConstant();
        if (constant == null) {
            if (debug) log.debug(attributeName+" attribute is dynamic.");
            return true;
        }

        if ("=".equals(operator)) {
            String equality = attributeType == null ? null : attributeType.getEquality();
            EqualityMatchingRule equalityMatchingRule = EqualityMatchingRule.getInstance(equality);

            boolean b = equalityMatchingRule.compare(constant, attributeValue);
            log.debug(" - ["+constant+"] => "+b);

            if (b) return true;

        } else if ("<=".equals(operator) || ">=".equals(operator)) {
            String ordering = attributeType == null ? null : attributeType.getOrdering();
            OrderingMatchingRule orderingMatchingRule = OrderingMatchingRule.getInstance(ordering);

            int c = orderingMatchingRule.compare(constant, attributeValue);
            log.debug(" - ["+constant+"] => "+c);

            if ("<=".equals(operator) && c > 0) return true;
            if (">=".equals(operator) && c < 0) return true;

        } else {
            throw new Exception("Unsupported operator \""+operator+"\" in \""+filter+"\"");
        }

        return false;
    }

    public boolean eval(PresentFilter filter) throws Exception {

        String attributeName = filter.getAttribute();

        Mapping mapping = entry.getMapping();

        if (mapping != null) {

            if (debug) log.debug("Checking "+mapping.getName()+" mapping.");

            Collection<MappingRule> rules = mapping.getRules(attributeName);
            return !rules.isEmpty();
        }

        if (attributeName.equalsIgnoreCase("objectclass")) {
            if (debug) log.debug("Object class is always present.");
            return true;
        }

        if (entry.getAttributeConfig(attributeName) == null) {
            if (debug) log.debug(attributeName+" attribute undefined.");
            return false;
        }

        return true;
    }

    public boolean eval(SubstringFilter filter) throws Exception {

        String attributeName = filter.getAttribute();
        Collection<Object> substrings = filter.getSubstrings();

        AttributeType attributeType = entry.getAttributeType(attributeName);

        Mapping mapping = entry.getMapping();

        if (mapping != null) {

            if (debug) log.debug("Checking "+mapping.getName()+" mapping.");

            for (MappingRule rule : mapping.getRules(attributeName)) {

                Object constant = rule.getConstant();
                if (constant == null) return true;

                String substring = attributeType == null ? null : attributeType.getSubstring();
                SubstringsMatchingRule substringsMatchingRule = SubstringsMatchingRule.getInstance(substring);

                boolean b = substringsMatchingRule.compare(constant, substrings);
                if (debug) log.debug(" - ["+constant+"] => "+b);

                if (b) return true;
            }

            return false;
        }

        EntryAttributeConfig attributeMapping = entry.getAttributeConfig(attributeName);
        if (attributeMapping == null) return false;

        Object constant = attributeMapping.getConstant();
        if (constant == null) return true;

        String substring = attributeType == null ? null : attributeType.getSubstring();
        SubstringsMatchingRule substringsMatchingRule = SubstringsMatchingRule.getInstance(substring);

        boolean b = substringsMatchingRule.compare(constant, substrings);
        if (debug) log.debug(" - ["+constant+"] => "+b);

        return b;
    }
}