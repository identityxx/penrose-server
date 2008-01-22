package org.safehaus.penrose.ldap.source;

import org.safehaus.penrose.filter.FilterProcessor;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.SimpleFilter;
import org.safehaus.penrose.source.Field;

import java.util.Stack;

/**
 * @author Endi Sukma Dewata
 */
public class ADGroupFilterProcessor extends FilterProcessor {

    ADGroupSource source;

    SimpleFilter cnFilter;
    SimpleFilter memberFilter;

    public ADGroupFilterProcessor(ADGroupSource source) throws Exception {
        this.source = source;
    }

    public Filter process(Stack<Filter> path, Filter filter) throws Exception {
        if (!(filter instanceof SimpleFilter)) {
            return super.process(path, filter);
        }

        SimpleFilter sf = (SimpleFilter)filter;

        String attributeName = sf.getAttribute();

        Field field = getSource().getField(attributeName);
        if (field == null) return filter;

        if (source.cnField.getName().equals(attributeName)) {
            cnFilter = sf;

        } else if (source.memberField.getName().equals(attributeName)) {
            memberFilter = sf;
        }

        return filter;
    }

    public ADGroupSource getSource() {
        return source;
    }

    public void setSource(ADGroupSource source) {
        this.source = source;
    }

    public SimpleFilter getCnFilter() {
        return cnFilter;
    }

    public void setCnFilter(SimpleFilter cnFilter) {
        this.cnFilter = cnFilter;
    }

    public SimpleFilter getMemberFilter() {
        return memberFilter;
    }

    public void setMemberFilter(SimpleFilter memberFilter) {
        this.memberFilter = memberFilter;
    }
}
