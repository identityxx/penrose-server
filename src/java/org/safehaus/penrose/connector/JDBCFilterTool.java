package org.safehaus.penrose.connector;

import org.safehaus.penrose.filter.*;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.partition.FieldConfig;
import org.safehaus.penrose.partition.SourceConfig;
import org.apache.log4j.Logger;

import java.util.Iterator;
import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class JDBCFilterTool {

    Logger log = Logger.getLogger(getClass());

    /**
     * Convert parsed SQL filter into string to be used in SQL queries.
     *
     * @param filter
     * @return string SQL filter
     * @throws Exception
     */
    public String convert(
            SourceConfig sourceConfig,
            Filter filter,
            Collection parameters)
            throws Exception {

        StringBuffer sb = new StringBuffer();
        boolean valid = convert(sourceConfig, filter, parameters, sb);

        if (valid && sb.length() > 0) return sb.toString();

        return null;
    }

    boolean convert(
            SourceConfig sourceConfig,
            Filter filter,
            Collection parameters,
            StringBuffer sb)
            throws Exception {

        if (filter instanceof NotFilter) {
            return convert(sourceConfig, (NotFilter) filter, parameters, sb);

        } else if (filter instanceof AndFilter) {
            return convert(sourceConfig, (AndFilter) filter, parameters, sb);

        } else if (filter instanceof OrFilter) {
            return convert(sourceConfig, (OrFilter) filter, parameters, sb);

        } else if (filter instanceof SimpleFilter) {
            return convert(sourceConfig, (SimpleFilter) filter, parameters, sb);
        }

        return true;
    }

    boolean convert(
            SourceConfig sourceConfig,
            SimpleFilter filter,
            Collection parameters,
            StringBuffer sb)
            throws Exception {

        String name = filter.getAttribute();
        String operator = filter.getOperator();
        String value = filter.getValue();

        //log.debug("Converting "+name+" "+operator+" "+value);

        if (name.equals("objectClass")) {
            if (value.equals("*"))
                return true;
        }

        int i = name.indexOf(".");
        if (i >= 0) name = name.substring(i+1);

        if (value.startsWith("'") && value.endsWith("'")) {
            value = value.substring(1, value.length()-1);
        }

        FieldConfig fieldConfig = sourceConfig.getFieldConfig(name);

        if ("VARCHAR".equals(fieldConfig.getType())) {
            sb.append("lower(");
            sb.append(fieldConfig.getOriginalName());
            sb.append(") ");
            sb.append(operator);
            sb.append(" lower(?)");

        } else {
            sb.append(fieldConfig.getOriginalName());
            sb.append(" ");
            sb.append(operator);
            sb.append(" ?");
        }

        parameters.add(value);

        return true;
    }

    boolean convert(
            SourceConfig sourceConfig,
            NotFilter filter,
            Collection parameters,
            StringBuffer sb)
            throws Exception {

        StringBuffer sb2 = new StringBuffer();

        Filter f = filter.getFilter();
        convert(sourceConfig, f, parameters, sb2);

        sb.append("not (");
        sb.append(sb2);
        sb.append(")");

        return true;
    }

    boolean convert(
            SourceConfig sourceConfig,
            AndFilter filter,
            Collection parameters,
            StringBuffer sb)
            throws Exception {

        StringBuffer sb2 = new StringBuffer();
        for (Iterator i = filter.getFilters().iterator(); i.hasNext();) {
            Filter f = (Filter) i.next();

            StringBuffer sb3 = new StringBuffer();
            convert(sourceConfig, f, parameters, sb3);

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
            SourceConfig sourceConfig,
            OrFilter filter,
            Collection parameters,
            StringBuffer sb)
            throws Exception {

        StringBuffer sb2 = new StringBuffer();
        for (Iterator i = filter.getFilters().iterator(); i.hasNext();) {
            Filter f = (Filter) i.next();

            StringBuffer sb3 = new StringBuffer();
            convert(sourceConfig, f, parameters, sb3);

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
