package org.safehaus.penrose.cache;

import org.safehaus.penrose.filter.*;
import org.safehaus.penrose.mapping.*;
import org.apache.log4j.Logger;

import java.util.Iterator;
import java.util.Collection;
import java.util.TreeSet;

/**
 * @author Endi S. Dewata
 */
public class JDBCCacheTool {

    Logger log = Logger.getLogger(getClass());

    public String getTableName(SourceDefinition sourceDefinition) {
        return sourceDefinition.getConnectionName()+"_"+sourceDefinition.getName();
    }

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

        String tableName = getTableName(sourceDefinition);

        Collection tables = new TreeSet();
        StringBuffer columns = new StringBuffer();
        StringBuffer whereClause = new StringBuffer();

        convert(sourceDefinition, filter, parameters, whereClause, tables);

        Collection pkFields = sourceDefinition.getPrimaryKeyFieldDefinitions();
        for (Iterator j=pkFields.iterator(); j.hasNext(); ) {
            FieldDefinition fieldDefinition = (FieldDefinition)j.next();
            String fieldName = fieldDefinition.getName();

            if (columns.length() > 0) columns.append(", ");
            columns.append(tableName);
            columns.append(".");
            columns.append(fieldName);
        }

        StringBuffer join = new StringBuffer();
        join.append(tableName);

        for (Iterator i=tables.iterator(); i.hasNext(); ) {
            String t = (String)i.next();

            StringBuffer sb = new StringBuffer();
            for (Iterator j=pkFields.iterator(); j.hasNext(); ) {
                FieldDefinition fieldDefinition = (FieldDefinition)j.next();
                String fieldName = fieldDefinition.getName();

                if (sb.length() > 0) sb.append(" and ");

                sb.append(tableName);
                sb.append(".");
                sb.append(fieldName);
                sb.append(" = ");
                sb.append(t);
                sb.append(".");
                sb.append(fieldName);
            }

            join.append(" join ");
            join.append(t);
            join.append(" on ");
            join.append(sb);
        }

        StringBuffer sb = new StringBuffer();
        sb.append("select ");
        sb.append(columns);
        sb.append(" from ");
        sb.append(join);

        if (whereClause.length() > 0) {
            sb.append(" where ");
            sb.append(whereClause);
        }

        return sb.toString();
    }

    boolean convert(
            SourceDefinition sourceDefinition,
            Filter filter,
            Collection parameters,
            StringBuffer sb,
            Collection tables)
            throws Exception {

        if (filter instanceof NotFilter) {
            return convert(sourceDefinition, (NotFilter) filter, parameters, sb, tables);

        } else if (filter instanceof AndFilter) {
            return convert(sourceDefinition, (AndFilter) filter, parameters, sb, tables);

        } else if (filter instanceof OrFilter) {
            return convert(sourceDefinition, (OrFilter) filter, parameters, sb, tables);

        } else if (filter instanceof SimpleFilter) {
            return convert(sourceDefinition, (SimpleFilter) filter, parameters, sb, tables);
        }

        return true;
    }

    boolean convert(
            SourceDefinition sourceDefinition,
            SimpleFilter filter,
            Collection parameters,
            StringBuffer sb,
            Collection tables)
            throws Exception {

        String name = filter.getAttribute();
        String operator = filter.getOperator();
        String value = filter.getValue();

        int i = name.indexOf(".");
        if (i >= 0) name = name.substring(i+1);

        if (value.startsWith("'") && value.endsWith("'")) {
            value = value.substring(1, value.length()-1);
        }

        FieldDefinition fieldDefinition = sourceDefinition.getFieldDefinition(name);
        String fieldName = fieldDefinition.getName();

        String tableName;

        if (fieldDefinition.isPrimaryKey()) {
            tableName = getTableName(sourceDefinition);
        } else {
            tableName = getTableName(sourceDefinition)+"_"+fieldName;
            tables.add(tableName);
        }

        if ("VARCHAR".equals(fieldDefinition.getType())) {
            sb.append("lower(");
            sb.append(tableName);
            sb.append(".");
            sb.append(fieldName);
            sb.append(") ");
            sb.append(operator);
            sb.append(" lower(?)");

        } else {
            sb.append(tableName);
            sb.append(".");
            sb.append(fieldName);
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
            StringBuffer sb,
            Collection tables)
            throws Exception {

        StringBuffer sb2 = new StringBuffer();

        Filter f = filter.getFilter();
        convert(sourceDefinition, f, parameters, sb2, tables);

        sb.append("not (");
        sb.append(sb2);
        sb.append(")");

        return true;
    }

    boolean convert(
            SourceDefinition sourceDefinition,
            AndFilter filter,
            Collection parameters,
            StringBuffer sb,
            Collection tables)
            throws Exception {

        StringBuffer sb2 = new StringBuffer();
        for (Iterator i = filter.getFilters().iterator(); i.hasNext();) {
            Filter f = (Filter) i.next();

            StringBuffer sb3 = new StringBuffer();
            convert(sourceDefinition, f, parameters, sb3, tables);

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
            StringBuffer sb,
            Collection tables)
            throws Exception {

        StringBuffer sb2 = new StringBuffer();
        for (Iterator i = filter.getFilters().iterator(); i.hasNext();) {
            Filter f = (Filter) i.next();

            StringBuffer sb3 = new StringBuffer();
            convert(sourceDefinition, f, parameters, sb3, tables);

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
