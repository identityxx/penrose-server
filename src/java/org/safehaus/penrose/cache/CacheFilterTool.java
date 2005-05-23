/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.cache;

import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.SimpleFilter;
import org.safehaus.penrose.filter.AndFilter;
import org.safehaus.penrose.filter.OrFilter;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.mapping.EntryDefinition;
import org.safehaus.penrose.mapping.Field;
import org.safehaus.penrose.mapping.Source;

import java.util.Iterator;
import java.util.Collection;
import java.util.Map;

/**
 * @author Endi S. Dewata
 */
public class CacheFilterTool {

    public CacheContext cacheContext;

    public CacheFilterTool(CacheContext cacheContext) {
        this.cacheContext = cacheContext;
    }

    /**
     * Get the filter from a given entry and Filter
     *
     * @param filter
     *            the filter (from config)
     * @return the filter string
     * @throws Exception
     */
    public String toSQLFilter(EntryDefinition entry, Filter filter) throws Exception {
        StringBuffer sb = new StringBuffer();
        boolean valid = toSQLFilter(entry, filter, sb);

        if (valid && sb.length() > 0) return sb.toString();

        return null;
    }

    /**
     * Get the filter for a given entry and Filter
     *
     * @param filter
     *            the filter (from config)
     * @param sb
     *            string buffer to be appended to
     * @return always true
     * @throws Exception
     */
    public boolean toSQLFilter(
            EntryDefinition entry,
            Filter filter,
            StringBuffer sb)
            throws Exception {

        if (filter instanceof SimpleFilter) {
            return toSQLFilter(entry, (SimpleFilter) filter, sb);

        } else if (filter instanceof AndFilter) {
            return toSQLFilter(entry, (AndFilter) filter, sb);

        } else if (filter instanceof OrFilter) {
            return toSQLFilter(entry, (OrFilter) filter, sb);
        }

        return true;
    }

    /**
     * Get the filter for a given entry and SimpleFilter
     *
     * @param filter
     *            the filter (from config) - a SimpleFilter
     * @param sb
     *            string buffer to be appended to
     * @return always true
     * @throws Exception
     */
    public boolean toSQLFilter(EntryDefinition entry, SimpleFilter filter, StringBuffer sb)
            throws Exception {
        String name = filter.getAttr();
        String value = filter.getValue();

        if (name.toLowerCase().equals("objectclass")) {
            return true;
        }

        Map attributes = entry.getAttributes();
        if (!attributes.containsKey(name)) return false;

        String lhs = name;
        String rhs = "'" + value + "'";

        sb.append("lower(");
        sb.append(lhs);
        sb.append(")=lower(");
        sb.append(rhs);
        sb.append(")");

        return true;
    }

    /**
     * Get the filter for a given entry and AndFilter
     *
     * @param filter
     *            the filter (from config) - an AndFilter
     * @param sb
     *            string buffer to be appended to
     * @return always true
     * @throws Exception
     */
    public boolean toSQLFilter(EntryDefinition entry, AndFilter filter, StringBuffer sb)
            throws Exception {
        StringBuffer sb2 = new StringBuffer();
        for (Iterator i = filter.getFilterList().iterator(); i.hasNext();) {
            Filter f = (Filter) i.next();

            StringBuffer sb3 = new StringBuffer();
            toSQLFilter(entry, f, sb3);

            if (sb2.length() > 0 && sb3.length() > 0) {
                sb2.append(" and ");
            }

            sb2.append(sb3);
        }

        if (sb2.length() == 0)
            return true;

        sb.append("(");
        sb.append(sb2);
        sb.append(")");

        return true;
    }

    /**
     * Get the filter for a given entry and OrFilter
     *
     * @param filter
     *            the filter (from config) - an OrFilter
     * @param sb
     *            string buffer to be appended to
     * @return always true
     * @throws Exception
     */
    public boolean toSQLFilter(EntryDefinition entry, OrFilter filter, StringBuffer sb)
            throws Exception {
        StringBuffer sb2 = new StringBuffer();
        for (Iterator i = filter.getFilterList().iterator(); i.hasNext();) {
            Filter f = (Filter) i.next();

            StringBuffer sb3 = new StringBuffer();
            toSQLFilter(entry, f, sb3);

            if (sb2.length() > 0 && sb3.length() > 0) {
                sb2.append(" or ");
            }

            sb2.append(sb3);
        }

        if (sb2.length() == 0)
            return true;

        sb.append("(");
        sb.append(sb2);
        sb.append(")");

        return true;
    }

    /**
     * Convert an LDAP filter into an SQL filter for a particular source.
     *
     * @param source
     * @param filter
     * @return
     * @throws Exception
     */
    public Filter toSourceFilter(Source source, Filter filter) throws Exception {

        if (filter instanceof SimpleFilter) {
            return toSourceFilter(source, (SimpleFilter) filter);

        } else if (filter instanceof AndFilter) {
            return toSourceFilter(source, (AndFilter) filter);

        } else if (filter instanceof OrFilter) {
            return toSourceFilter(source, (OrFilter) filter);
        }

        return null;
    }

    public Filter toSourceFilter(Source source, SimpleFilter filter)
            throws Exception {

        String name = filter.getAttr();
        String value = filter.getValue();

        if (name.equals("objectClass")) {
            if (value.equals("*"))
                return null;
        }

        Interpreter interpreter = cacheContext.newInterpreter();
        interpreter.set(name, value);

        Collection fields = source.getFields();
        Filter newFilter = null;

        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            Field field = (Field)i.next();

            String expression = field.getExpression();
            if (expression == null) continue;

            // this assumes that the field's value can be computed using the attribute value in the filter
            String v = (String)interpreter.eval(expression);
            if (v == null) continue;

            SimpleFilter f = new SimpleFilter(field.getName(), "=", v);

            if (newFilter == null) {
                newFilter = f;

            } else if (newFilter instanceof SimpleFilter) {
                AndFilter af = new AndFilter();
                af.addFilterList(newFilter);
                af.addFilterList(f);
                newFilter = af;

            } else { // newFilter instanceof AndFilter
                AndFilter af = (AndFilter)newFilter;
                af.addFilterList(f);
            }
        }

        return newFilter;
    }

    public Filter toSourceFilter(Source source, AndFilter filter)
            throws Exception {

        Collection filters = filter.getFilterList();

        AndFilter af = new AndFilter();
        for (Iterator i=filters.iterator(); i.hasNext(); ) {
            Filter f = (Filter)i.next();

            Filter nf = toSourceFilter(source, f);
            af.addFilterList(nf);
        }

        return af;
    }

    public Filter toSourceFilter(Source source, OrFilter filter)
            throws Exception {

        Collection filters = filter.getFilterList();

        OrFilter of = new OrFilter();
        for (Iterator i=filters.iterator(); i.hasNext(); ) {
            Filter f = (Filter)i.next();

            Filter nf = toSourceFilter(source, f);
            of.addFilterList(nf);
        }

        return of;
    }

}
