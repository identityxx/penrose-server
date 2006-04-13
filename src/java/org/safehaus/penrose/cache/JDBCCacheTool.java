/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.safehaus.penrose.cache;

import org.safehaus.penrose.filter.*;
import org.safehaus.penrose.partition.FieldConfig;
import org.safehaus.penrose.partition.SourceConfig;
import org.apache.log4j.Logger;

import java.util.Iterator;
import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class JDBCCacheTool {

    Logger log = Logger.getLogger(getClass());

    boolean convert(
            String tableName,
            SourceConfig sourceConfig,
            Filter filter,
            Collection parameters,
            StringBuffer sb,
            Collection tables)
            throws Exception {

        if (filter instanceof NotFilter) {
            return convert(tableName, sourceConfig, (NotFilter) filter, parameters, sb, tables);

        } else if (filter instanceof AndFilter) {
            return convert(tableName, sourceConfig, (AndFilter) filter, parameters, sb, tables);

        } else if (filter instanceof OrFilter) {
            return convert(tableName, sourceConfig, (OrFilter) filter, parameters, sb, tables);

        } else if (filter instanceof SimpleFilter) {
            return convert(tableName, sourceConfig, (SimpleFilter) filter, parameters, sb, tables);
        }

        return true;
    }

    boolean convert(
            String tableName,
            SourceConfig sourceConfig,
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

        FieldConfig fieldConfig = sourceConfig.getFieldConfig(name);
        String fieldName = fieldConfig.getName();

        String t;

        if (fieldConfig.isPrimaryKey()) {
            t = tableName;
        } else {
            t = tableName+"_"+fieldName;
            tables.add(t);
        }

        if ("VARCHAR".equals(fieldConfig.getType())) {
            if (!fieldConfig.isCaseSensitive()) sb.append("lower(");
            sb.append(t);
            sb.append(".");
            sb.append(fieldName);
            if (!fieldConfig.isCaseSensitive()) sb.append(")");
            sb.append(" ");
            sb.append(operator);
            sb.append(" ");
            if (!fieldConfig.isCaseSensitive()) sb.append("lower(");
            sb.append("?");
            if (!fieldConfig.isCaseSensitive()) sb.append(")");

        } else {
            sb.append(t);
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
            String tableName,
            SourceConfig sourceConfig,
            NotFilter filter,
            Collection parameters,
            StringBuffer sb,
            Collection tables)
            throws Exception {

        StringBuffer sb2 = new StringBuffer();

        Filter f = filter.getFilter();
        convert(tableName, sourceConfig, f, parameters, sb2, tables);

        sb.append("not (");
        sb.append(sb2);
        sb.append(")");

        return true;
    }

    boolean convert(
            String tableName,
            SourceConfig sourceConfig,
            AndFilter filter,
            Collection parameters,
            StringBuffer sb,
            Collection tables)
            throws Exception {

        StringBuffer sb2 = new StringBuffer();
        for (Iterator i = filter.getFilters().iterator(); i.hasNext();) {
            Filter f = (Filter) i.next();

            StringBuffer sb3 = new StringBuffer();
            convert(tableName, sourceConfig, f, parameters, sb3, tables);

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
            String tableName,
            SourceConfig sourceConfig,
            OrFilter filter,
            Collection parameters,
            StringBuffer sb,
            Collection tables)
            throws Exception {

        StringBuffer sb2 = new StringBuffer();
        for (Iterator i = filter.getFilters().iterator(); i.hasNext();) {
            Filter f = (Filter) i.next();

            StringBuffer sb3 = new StringBuffer();
            convert(tableName, sourceConfig, f, parameters, sb3, tables);

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
