package org.safehaus.penrose.ldap.source;

import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.FilterProcessor;
import org.safehaus.penrose.filter.ItemFilter;
import org.safehaus.penrose.filter.SimpleFilter;
import org.safehaus.penrose.source.Field;

import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

/**
 * @author Endi Sukma Dewata
 */
public class LDAPSourceFilterProcessor extends FilterProcessor {

    LDAPSource source;

    Map<Field,ItemFilter> filters = new HashMap<Field,ItemFilter>();

    public LDAPSourceFilterProcessor(LDAPSource source) throws Exception {
        this.source = source;
    }

    public Filter process(Stack<Filter> path, Filter filter) throws Exception {
        if (!(filter instanceof SimpleFilter)) {
            return super.process(path, filter);
        }

        SimpleFilter sf = (SimpleFilter)filter;

        String attributeName = sf.getAttribute();

        Field field = source.getField(attributeName);
        if (field == null) return filter;

        filters.put(field, sf);

        return filter;
    }

    public LDAPSource getSource() {
        return source;
    }

    public void setSource(LDAPSource source) {
        this.source = source;
    }

    public ItemFilter getFilter(Field field) {
        return filters.get(field);
    }
}