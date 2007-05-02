package org.safehaus.penrose.filter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.schema.SchemaManager;
import org.safehaus.penrose.schema.AttributeType;
import org.safehaus.penrose.schema.matchingRule.SubstringsMatchingRule;
import org.safehaus.penrose.schema.matchingRule.EqualityMatchingRule;
import org.safehaus.penrose.schema.matchingRule.OrderingMatchingRule;
import org.safehaus.penrose.entry.Entry;
import org.safehaus.penrose.ldap.Attributes;
import org.safehaus.penrose.ldap.Attribute;
import org.safehaus.penrose.ldap.RDN;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.mapping.AttributeMapping;

import java.util.Collection;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class FilterEvaluator {

    public Logger log = LoggerFactory.getLogger(getClass());

    private SchemaManager schemaManager;

    public FilterEvaluator() throws Exception {
    }

    public boolean eval(Entry entry, Filter filter) throws Exception {
        return eval(entry.getAttributes(), filter);
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
        Collection substrings = filter.getSubstrings();

        Attribute attribute = attributes.get(attributeName);
        if (attribute == null) return false;

        Collection set = attribute.getValues();

        AttributeType attributeType = schemaManager.getAttributeType(attributeName);

        String substring = attributeType == null ? null : attributeType.getSubstring();
        SubstringsMatchingRule substringsMatchingRule = SubstringsMatchingRule.getInstance(substring);

        for (Iterator i=set.iterator(); i.hasNext(); ) {
            String value = i.next().toString();

            boolean b = substringsMatchingRule.compare(value, substrings);
            log.debug(" - ["+value+"] => "+b);

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

        Collection set = attribute.getValues();
        AttributeType attributeType = schemaManager.getAttributeType(attributeName);

        if ("=".equals(operator)) {
            String equality = attributeType == null ? null : attributeType.getEquality();
            EqualityMatchingRule equalityMatchingRule = EqualityMatchingRule.getInstance(equality);

            for (Iterator i=set.iterator(); i.hasNext(); ) {
                String value = i.next().toString();

                boolean b = equalityMatchingRule.compare(value, attributeValue);
                //log.debug(" - ["+value+"] => "+b);

                if (b) return true;
            }

        } else if ("<=".equals(operator) || ">=".equals(operator)) {
            String ordering = attributeType == null ? null : attributeType.getOrdering();
            OrderingMatchingRule orderingMatchingRule = OrderingMatchingRule.getInstance(ordering);

            for (Iterator i=set.iterator(); i.hasNext(); ) {
                String value = i.next().toString();

                int c = orderingMatchingRule.compare(value, attributeValue);
                log.debug(" - ["+value+"] => "+c);

                if ("<=".equals(operator) && c <= 0) return true;
                if (">=".equals(operator) && c >= 0) return true;
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
        for (Iterator i=filter.getFilters().iterator(); i.hasNext(); ) {
            Filter f = (Filter)i.next();
            boolean result = eval(attributes, f);
            if (!result) return false;
        }
        return true;
    }

    public boolean eval(Attributes attributes, OrFilter filter) throws Exception {
        for (Iterator i=filter.getFilters().iterator(); i.hasNext(); ) {
            Filter f = (Filter)i.next();
            boolean result = eval(attributes, f);
            if (result) return true;
        }
        return false;
    }

    public boolean eval(EntryMapping entryMapping, Filter filter) throws Exception {
        //log.debug("Checking filter "+filter);

        boolean result = false;

        if (filter == null) {
            result = true;

        } else if (filter instanceof NotFilter) {
            result = eval(entryMapping, (NotFilter)filter);

        } else if (filter instanceof AndFilter) {
            result = eval(entryMapping, (AndFilter)filter);

        } else if (filter instanceof OrFilter) {
            result = eval(entryMapping, (OrFilter)filter);

        } else if (filter instanceof SimpleFilter) {
            result = eval(entryMapping, (SimpleFilter)filter);

        } else if (filter instanceof PresentFilter) {
            result = eval(entryMapping, (PresentFilter)filter);

        } else if (filter instanceof SubstringFilter) {
            result = eval(entryMapping, (SubstringFilter)filter);
        }

        // log.debug("=> "+filter+" ("+filter.getClass().getName()+"): "+result);

        return result;
    }

    public boolean eval(EntryMapping entryMapping, SimpleFilter filter) throws Exception {
        String attributeName = filter.getAttribute();
        String operator = filter.getOperator();
        Object attributeValue = filter.getValue();

        if (attributeName.equalsIgnoreCase("objectclass")) {
            return entryMapping.containsObjectClass(attributeValue.toString());
        }

        AttributeMapping attributeMapping = entryMapping.getAttributeMapping(attributeName);
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

    public boolean eval(EntryMapping entryMapping, PresentFilter filter) throws Exception {
        String attributeName = filter.getAttribute();

        if (attributeName.equalsIgnoreCase("objectclass")) return true;

        return entryMapping.getAttributeMapping(attributeName) != null;
    }

    public boolean eval(EntryMapping entryMapping, SubstringFilter filter) throws Exception {
        String attributeName = filter.getAttribute();
        Collection substrings = filter.getSubstrings();

        AttributeMapping attributeMapping = entryMapping.getAttributeMapping(attributeName);
        if (attributeMapping == null) return false;

        Object value = attributeMapping.getConstant();
        if (value == null) return true;

        AttributeType attributeType = schemaManager.getAttributeType(attributeName);

        String substring = attributeType == null ? null : attributeType.getSubstring();
        SubstringsMatchingRule substringsMatchingRule = SubstringsMatchingRule.getInstance(substring);

        boolean b = substringsMatchingRule.compare(value, substrings);
        log.debug(" - ["+value+"] => "+b);

        if (b) return true;

        return false;
    }

    public boolean eval(EntryMapping entryMapping, NotFilter filter) throws Exception {
        Filter f = filter.getFilter();
        boolean result = eval(entryMapping, f);
        return result;
    }

    public boolean eval(EntryMapping entryMapping, AndFilter filter) throws Exception {
        for (Iterator i=filter.getFilters().iterator(); i.hasNext(); ) {
            Filter f = (Filter)i.next();
            boolean result = eval(entryMapping, f);
            if (!result) return false;
        }
        return true;
    }

    public boolean eval(EntryMapping entryMapping, OrFilter filter) throws Exception {
        for (Iterator i=filter.getFilters().iterator(); i.hasNext(); ) {
            Filter f = (Filter)i.next();
            boolean result = eval(entryMapping, f);
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
        for (Iterator i=filter.getFilters().iterator(); i.hasNext(); ) {
            Filter f = (Filter)i.next();
            boolean result = eval(rdn, f);
            if (!result) return false;
        }
        return true;
    }

    public boolean eval(RDN rdn, OrFilter filter) throws Exception {
        for (Iterator i=filter.getFilters().iterator(); i.hasNext(); ) {
            Filter f = (Filter)i.next();
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
