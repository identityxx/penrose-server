package org.safehaus.penrose.connection;

import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.SimpleFilter;
import org.safehaus.penrose.filter.AndFilter;
import org.safehaus.penrose.filter.OrFilter;
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

    private AdapterContext adapterContext;

    public JDBCFilterTool(AdapterContext adapterContext) {
        this.adapterContext = adapterContext;
    }

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

        Config config = getAdapterContext().getConfig(source);
        ConnectionConfig connectionConfig = config.getConnectionConfig(source.getConnectionName());
        SourceDefinition sourceDefinition = connectionConfig.getSourceDefinition(source.getSourceName());

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
            Source source,
            AndFilter filter,
            Collection parameters,
            StringBuffer sb)
            throws Exception {

        StringBuffer sb2 = new StringBuffer();
        for (Iterator i = filter.getFilters().iterator(); i.hasNext();) {
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
        for (Iterator i = filter.getFilters().iterator(); i.hasNext();) {
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

    public AdapterContext getAdapterContext() {
        return adapterContext;
    }

    public void setAdapterContext(AdapterContext adapterContext) {
        this.adapterContext = adapterContext;
    }
}
