package org.safehaus.penrose.connection;

import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.SimpleFilter;
import org.safehaus.penrose.filter.AndFilter;
import org.safehaus.penrose.filter.OrFilter;
import org.safehaus.penrose.mapping.Source;
import org.safehaus.penrose.mapping.Field;
import org.safehaus.penrose.mapping.FieldDefinition;

import java.util.Iterator;
import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class JDBCFilterTool {

    /**
     * Convert parsed SQL filter into string to be used in SQL queries.
     *
     * @param filter
     * @return string SQL filter
     * @throws Exception
     */
    public String convert(
            Source source,
            Filter filter,
            Collection parameters)
            throws Exception {

        StringBuffer sb = new StringBuffer();
        boolean valid = convert(source, filter, parameters, sb);

        if (valid && sb.length() > 0) return sb.toString();

        return null;
    }

    boolean convert(
            Source source,
            Filter filter,
            Collection parameters,
            StringBuffer sb)
            throws Exception {

        if (filter instanceof SimpleFilter) {
            return convert(source, (SimpleFilter) filter, parameters, sb);

        } else if (filter instanceof AndFilter) {
            return convert(source, (AndFilter) filter, parameters, sb);

        } else if (filter instanceof OrFilter) {
            return convert(source, (OrFilter) filter, parameters, sb);
        }

        return true;
    }

    boolean convert(
            Source source,
            SimpleFilter filter,
            Collection parameters,
            StringBuffer sb)
            throws Exception {

        String name = filter.getAttr();
        String value = filter.getValue();

        if (name.equals("objectClass")) {
            if (value.equals("*"))
                return true;
        }

        Field field = source.getField(name);

        if ("VARCHAR".equals(field.getType())) {
            sb.append("lower(");
            sb.append(field.getOriginalName());
            sb.append(")=lower(?)");

        } else {
            sb.append(field.getOriginalName());
            sb.append("=?");
        }

        parameters.add(value);

        return true;
    }

    boolean convert(
            Source source,
            AndFilter filter,
            Collection parameters,
            StringBuffer sb)
            throws Exception {

        StringBuffer sb2 = new StringBuffer();
        for (Iterator i = filter.getFilterList().iterator(); i.hasNext();) {
            Filter f = (Filter) i.next();

            StringBuffer sb3 = new StringBuffer();
            convert(source, f, parameters, sb3);

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

    boolean convert(
            Source source,
            OrFilter filter,
            Collection parameters,
            StringBuffer sb)
            throws Exception {

        StringBuffer sb2 = new StringBuffer();
        for (Iterator i = filter.getFilterList().iterator(); i.hasNext();) {
            Filter f = (Filter) i.next();

            StringBuffer sb3 = new StringBuffer();
            convert(source, f, parameters, sb3);

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

}
