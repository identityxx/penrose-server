package org.safehaus.penrose.filter;

import org.safehaus.penrose.directory.AttributeMapping;
import org.safehaus.penrose.directory.Entry;
import org.safehaus.penrose.ldap.Attribute;
import org.safehaus.penrose.ldap.Attributes;
import org.safehaus.penrose.ldap.RDN;
import org.safehaus.penrose.ldap.SearchResult;
import org.safehaus.penrose.schema.AttributeType;
import org.safehaus.penrose.schema.SchemaManager;
import org.safehaus.penrose.schema.matchingRule.EqualityMatchingRule;
import org.safehaus.penrose.schema.matchingRule.OrderingMatchingRule;
import org.safehaus.penrose.schema.matchingRule.SubstringsMatchingRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class FilterEvaluator {

    public Logger log = LoggerFactory.getLogger(getClass());

    private SchemaManager schemaManager;

    public FilterEvaluator() throws Exception {
    }

    public boolean eval(SearchResult result, Filter filter) throws Exception {
        return eval(result.getAttributes(), filter);
    }
    
    public boolean eval(Attributes attributes, Filter filter) throws Exception {

        //log.debug("Checking filter "+filter);
        boolean result = false;

        if (filter == null) {
            result = true;

        } else if (filter instanceof NotFilter) {
            result = eval(attributes, (NotFilter)filter);

        } else if (filter instanceof AndFilter) {
            result = eval(attributes, (AndFilter)filter);

        } else if (filter instanceof OrFilter) {
            result = eval(attributes, (OrFilter)filter);

        } else if (filter instanceof SubstringFilter) {
            result = eval(attributes, (SubstringFilter)filter);

        } else if (filter instanceof PresentFilter) {
            result = eval(attributes, (PresentFilter)filter);

        } else if (filter instanceof SimpleFilter) {
            result = eval(attributes, (SimpleFilter)filter);
        }

        //log.debug(" - "+filter+" -> "+(result ? "ok" : "false"));

        return result;
    }

    public boolean eval(Attributes attributes, SubstringFilter filter) throws Exception {
        String attributeName = filter.getAttribute();
        Collection<Object> substrings = filter.getSubstrings();

        Attribute attribute = attributes.get(attributeName);
        if (attribute == null) return false;

        Collection<Object> set = attribute.getValues();

        AttributeType attributeType = schemaManager.getAttributeType(attributeName);

        String substring = attributeType == null ? null : attributeType.getSubstring();
        SubstringsMatchingRule substringsMatchingRule = SubstringsMatchingRule.getInstance(substring);

        for (Object object : set) {
            String value = object.toString();

            boolean b = substringsMatchingRule.compare(value, substrings);
            log.debug("Compare [" + value + "] with " + substrings + " => " + b);

            if (b) return true;
        }

        return false;
    }

    public boolean eval(Attributes attributes, PresentFilter filter) throws Exception {
        String attributeName = filter.getAttribute();
        if (attributeName.equalsIgnoreCase("objectclass")) {
            return true;
        } else {
            return attributes.get(attributeName) != null;
        }
    }

    public boolean eval(Attributes attributes, SimpleFilter filter) throws Exception {
        String attributeName = filter.getAttribute();
        String operator = filter.getOperator();
        Object attributeValue = filter.getValue();

        Attribute attribute = attributes.get(attributeName);
        if (attribute == null) return false;

        Collection<Object> set = attribute.getValues();
        AttributeType attributeType = schemaManager.getAttributeType(attributeName);

        if ("=".equals(operator)) {
            String equality = attributeType == null ? null : attributeType.getEquality();
            EqualityMatchingRule equalityMatchingRule = EqualityMatchingRule.getInstance(equality);

            for (Object value : set) {
                boolean b = equalityMatchingRule.compare(value, attributeValue);
                log.debug("Compare [" + value + "] "+operator+" [" + attributeValue + "] => " + b);

                if (b) return true;
            }

        } else if ("<=".equals(operator) || ">=".equals(operator)) {
            String ordering = attributeType == null ? null : attributeType.getOrdering();
            OrderingMatchingRule orderingMatchingRule = OrderingMatchingRule.getInstance(ordering);

            for (Object value : set) {
                int c = orderingMatchingRule.compare(value, attributeValue);
                boolean b = ("<=".equals(operator) && c <= 0) || (">=".equals(operator) && c >= 0);
                log.debug("Compare [" + value + "] "+operator+" [" + attributeValue + "] => " + b);

                if (b) return true;
            }

        } else {
            throw new Exception("Unsupported operator \""+operator+"\" in \""+filter+"\"");
        }

        return false;
    }

    public boolean eval(Attributes attributes, NotFilter filter) throws Exception {
        Filter f = filter.getFilter();
        boolean result = eval(attributes, f);
        return !result;
    }

    public boolean eval(Attributes attributes, AndFilter filter) throws Exception {
        for (Filter f : filter.getFilters()) {
            boolean result = eval(attributes, f);
            if (!result) return false;
        }
        return true;
    }

    public boolean eval(Attributes attributes, OrFilter filter) throws Exception {
        for (Filter f : filter.getFilters()) {
            boolean result = eval(attributes, f);
            if (result) return true;
        }
        return false;
    }

    public boolean eval(Entry entry, Filter filter) throws Exception {
        //log.debug("Checking filter "+filter);

        boolean result = false;

        if (filter == null) {
            result = true;

        } else if (filter instanceof NotFilter) {
            result = eval(entry, (NotFilter)filter);

        } else if (filter instanceof AndFilter) {
            result = eval(entry, (AndFilter)filter);

        } else if (filter instanceof OrFilter) {
            result = eval(entry, (OrFilter)filter);

        } else if (filter instanceof SimpleFilter) {
            result = eval(entry, (SimpleFilter)filter);

        } else if (filter instanceof PresentFilter) {
            result = eval(entry, (PresentFilter)filter);

        } else if (filter instanceof SubstringFilter) {
            result = eval(entry, (SubstringFilter)filter);
        }

        // log.debug("=> "+filter+" ("+filter.getClass().getName()+"): "+result);

        return result;
    }

    public boolean eval(Entry entry, SimpleFilter filter) throws Exception {
        String attributeName = filter.getAttribute();
        String operator = filter.getOperator();
        Object attributeValue = filter.getValue();

        if (attributeName.equalsIgnoreCase("objectclass")) {
            return entry.containsObjectClass(attributeValue.toString());
        }

        AttributeMapping attributeMapping = entry.getAttributeMapping(attributeName);
        if (attributeMapping == null) return false;

        Object value = attributeMapping.getConstant();
        if (value == null) return true;

        AttributeType attributeType = schemaManager.getAttributeType(attributeName);

        if ("=".equals(operator)) {
            String equality = attributeType == null ? null : attributeType.getEquality();
            EqualityMatchingRule equalityMatchingRule = EqualityMatchingRule.getInstance(equality);

            boolean b = equalityMatchingRule.compare(value, attributeValue);
            log.debug(" - ["+value+"] => "+b);

            if (b) return true;

        } else if ("<=".equals(operator) || ">=".equals(operator)) {
            String ordering = attributeType == null ? null : attributeType.getOrdering();
            OrderingMatchingRule orderingMatchingRule = OrderingMatchingRule.getInstance(ordering);

            int c = orderingMatchingRule.compare(value, attributeValue);
            log.debug(" - ["+value+"] => "+c);

            if ("<=".equals(operator) && c > 0) return true;
            if (">=".equals(operator) && c < 0) return true;

        } else {
            throw new Exception("Unsupported operator \""+operator+"\" in \""+filter+"\"");
        }

        return false;
    }

    public boolean eval(Entry entry, PresentFilter filter) throws Exception {
        String attributeName = filter.getAttribute();

        if (attributeName.equalsIgnoreCase("objectclass")) return true;

        return entry.getAttributeMapping(attributeName) != null;
    }

    public boolean eval(Entry entry, SubstringFilter filter) throws Exception {
        String attributeName = filter.getAttribute();
        Collection<Object> substrings = filter.getSubstrings();

        AttributeMapping attributeMapping = entry.getAttributeMapping(attributeName);
        if (attributeMapping == null) return false;

        Object value = attributeMapping.getConstant();
        if (value == null) return true;

        AttributeType attributeType = schemaManager.getAttributeType(attributeName);

        String substring = attributeType == null ? null : attributeType.getSubstring();
        SubstringsMatchingRule substringsMatchingRule = SubstringsMatchingRule.getInstance(substring);

        boolean b = substringsMatchingRule.compare(value, substrings);
        log.debug(" - ["+value+"] => "+b);

        return b;
    }

    public boolean eval(Entry entry, NotFilter filter) throws Exception {
        Filter f = filter.getFilter();
        return eval(entry, f);
    }

    public boolean eval(Entry entry, AndFilter filter) throws Exception {
        for (Filter f : filter.getFilters()) {
            boolean result = eval(entry, f);
            if (!result) return false;
        }
        return true;
    }

    public boolean eval(Entry entry, OrFilter filter) throws Exception {
        for (Filter f : filter.getFilters()) {
            boolean result = eval(entry, f);
            if (result) return true;
        }
        return false;
    }

    public boolean eval(RDN rdn, SimpleFilter filter) throws Exception {
        String attributeName = filter.getAttribute();
        String operator = filter.getOperator();
        Object attributeValue = filter.getValue();

        Object value = rdn.get(attributeName);
        if (value == null) return false;

        int c = attributeValue.toString().compareTo(value.toString());

        if ("=".equals(operator)) {
            if (c != 0) return false;

        } else if ("<".equals(operator)) {
            if (c >= 0) return false;

        } else if ("<=".equals(operator)) {
            if (c > 0) return false;

        } else if (">".equals(operator)) {
            if (c <= 0) return false;

        } else if (">=".equals(operator)) {
            if (c < 0) return false;

        } else {
            throw new Exception("Unsupported operator \""+operator+"\" in \""+filter+"\"");
        }

        return true;
    }

    public boolean eval(RDN rdn, PresentFilter filter) throws Exception {
        String attributeName = filter.getAttribute();

        if (attributeName.equalsIgnoreCase("objectclass")) return true;

        return rdn.contains(attributeName);
    }

    public boolean eval(RDN rdn, AndFilter filter) throws Exception {
        for (Filter f : filter.getFilters()) {
            boolean result = eval(rdn, f);
            if (!result) return false;
        }
        return true;
    }

    public boolean eval(RDN rdn, OrFilter filter) throws Exception {
        for (Filter f : filter.getFilters()) {
            boolean result = eval(rdn, f);
            if (result) return true;
        }
        return false;
    }

    public boolean eval(RDN rdn, NotFilter filter) throws Exception {
        Filter f = filter.getFilter();
        return eval(rdn, f);
    }

    public boolean eval(RDN rdn, Filter filter) throws Exception {
        //log.debug("Checking filter "+filter);

        boolean result = false;

        if (filter == null) {
            result = true;

        } else if (filter instanceof NotFilter) {
            result = eval(rdn, (NotFilter)filter);

        } else if (filter instanceof AndFilter) {
            result = eval(rdn, (AndFilter)filter);

        } else if (filter instanceof OrFilter) {
            result = eval(rdn, (OrFilter)filter);

        } else if (filter instanceof SimpleFilter) {
            result = eval(rdn, (SimpleFilter)filter);

        } else if (filter instanceof PresentFilter) {
            result = eval(rdn, (PresentFilter)filter);
        }

        // log.debug("=> "+filter+" ("+filter.getClass().getName()+"): "+result);

        return result;
    }

    public SchemaManager getSchemaManager() {
        return schemaManager;
    }

    public void setSchemaManager(SchemaManager schemaManager) {
        this.schemaManager = schemaManager;
    }
}
