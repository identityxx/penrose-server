package org.safehaus.penrose.connection;

import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.SimpleFilter;
import org.safehaus.penrose.filter.AndFilter;
import org.safehaus.penrose.filter.OrFilter;

import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class JDBCFilterTool {

    /**
     * Convert parsed SQL filter into string to be used in SQL queries.
     *
     * @param filter
     * @return
     * @throws Exception
     */
    public String convert(Filter filter) throws Exception {
        StringBuffer sb = new StringBuffer();
        boolean valid = convert(filter, sb);

        if (valid && sb.length() > 0) return sb.toString();

        return null;
    }

    boolean convert(
            Filter filter,
            StringBuffer sb)
            throws Exception {

        if (filter instanceof SimpleFilter) {
            return convert((SimpleFilter) filter, sb);

        } else if (filter instanceof AndFilter) {
            return convert((AndFilter) filter, sb);

        } else if (filter instanceof OrFilter) {
            return convert((OrFilter) filter, sb);
        }

        return true;
    }

    boolean convert(SimpleFilter filter, StringBuffer sb)
            throws Exception {
        String name = filter.getAttr();
        String value = filter.getValue();

        if (name.equals("objectClass")) {
            if (value.equals("*"))
                return true;
        }

        String lhs = name;
        String rhs = "'" + value + "'";

        sb.append("lower(");
        sb.append(lhs);
        sb.append(")=lower(");
        sb.append(rhs);
        sb.append(")");

        return true;
    }

    boolean convert(AndFilter filter, StringBuffer sb)
            throws Exception {
        StringBuffer sb2 = new StringBuffer();
        for (Iterator i = filter.getFilterList().iterator(); i.hasNext();) {
            Filter f = (Filter) i.next();

            StringBuffer sb3 = new StringBuffer();
            convert(f, sb3);

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

    boolean convert(OrFilter filter, StringBuffer sb)
            throws Exception {
        StringBuffer sb2 = new StringBuffer();
        for (Iterator i = filter.getFilterList().iterator(); i.hasNext();) {
            Filter f = (Filter) i.next();

            StringBuffer sb3 = new StringBuffer();
            convert(f, sb3);

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
