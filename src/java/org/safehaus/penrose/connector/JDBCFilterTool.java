package org.safehaus.penrose.connector;

import org.safehaus.penrose.filter.*;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.config.Config;
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
            SourceDefinition sourceDefinition,
            Filter filter,
            Collection parameters)
            throws Exception {

        StringBuffer sb = new StringBuffer();
        boolean valid = convert(sourceDefinition, filter, parameters, sb);

        if (valid && sb.length() > 0) return sb.toString();

        return null;
    }

    boolean convert(
            SourceDefinition sourceDefinition,
            Filter filter,
            Collection parameters,
            StringBuffer sb)
            throws Exception {

        if (filter instanceof NotFilter) {
            return convert(sourceDefinition, (NotFilter) filter, parameters, sb);

        } else if (filter instanceof AndFilter) {
            return convert(sourceDefinition, (AndFilter) filter, parameters, sb);

        } else if (filter instanceof OrFilter) {
            return convert(sourceDefinition, (OrFilter) filter, parameters, sb);

        } else if (filter instanceof SimpleFilter) {
            return convert(sourceDefinition, (SimpleFilter) filter, parameters, sb);
        }

        return true;
    }

    boolean convert(
            SourceDefinition sourceDefinition,
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

        FieldDefinition fieldDefinition = sourceDefinition.getFieldDefinition(name);

        if ("VARCHAR".equals(fieldDefinition.getType())) {
            sb.append("lower(");
            sb.append(fieldDefinition.getOriginalName());
            sb.append(") ");
            sb.append(operator);
            sb.append(" lower(?)");

        } else {
            sb.append(fieldDefinition.getOriginalName());
            sb.append(" ");
            sb.append(operator);
            sb.append(" ?");
        }

        parameters.add(value);

        return true;
    }

    boolean convert(
            SourceDefinition sourceDefinition,
            NotFilter filter,
            Collection parameters,
            StringBuffer sb)
            throws Exception {

        StringBuffer sb2 = new StringBuffer();

        Filter f = filter.getFilter();
        convert(sourceDefinition, f, parameters, sb2);

        sb.append("not (");
        sb.append(sb2);
        sb.append(")");

        return true;
    }

    boolean convert(
            SourceDefinition sourceDefinition,
            AndFilter filter,
            Collection parameters,
            StringBuffer sb)
            throws Exception {

        StringBuffer sb2 = new StringBuffer();
        for (Iterator i = filter.getFilters().iterator(); i.hasNext();) {
            Filter f = (Filter) i.next();

            StringBuffer sb3 = new StringBuffer();
            convert(sourceDefinition, f, parameters, sb3);

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
            SourceDefinition sourceDefinition,
            OrFilter filter,
            Collection parameters,
            StringBuffer sb)
            throws Exception {

        StringBuffer sb2 = new StringBuffer();
        for (Iterator i = filter.getFilters().iterator(); i.hasNext();) {
            Filter f = (Filter) i.next();

            StringBuffer sb3 = new StringBuffer();
            convert(sourceDefinition, f, parameters, sb3);

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
