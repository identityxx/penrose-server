/**
 * Copyright (c) 2000-2006, Identyx Corporation.
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
package org.safehaus.penrose.jdbc;

import org.safehaus.penrose.filter.*;
import org.safehaus.penrose.partition.FieldConfig;
import org.safehaus.penrose.partition.SourceConfig;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.Iterator;
import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class JDBCFilterTool {

    Logger log = LoggerFactory.getLogger(getClass());

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
            Collection parameters,
            Collection fieldConfigs)
            throws Exception {

        boolean debug = log.isDebugEnabled();
        if (debug) log.debug("Converting source filter "+filter+" to JDBC filter for source "+sourceConfig.getName());

        StringBuilder sb = new StringBuilder();
        boolean valid = convert(sourceConfig, filter, parameters, fieldConfigs, sb);

        String jdbcFilter = null;

        if (valid && sb.length() > 0) jdbcFilter = sb.toString();

        if (debug) log.debug("JDBC filter: "+jdbcFilter);

        return jdbcFilter;
    }

    boolean convert(
            SourceConfig sourceConfig,
            Filter filter,
            Collection parameters,
            Collection fieldConfigs,
            StringBuilder sb)
            throws Exception {

        if (filter instanceof NotFilter) {
            return convert(sourceConfig, (NotFilter) filter, parameters, fieldConfigs, sb);

        } else if (filter instanceof AndFilter) {
            return convert(sourceConfig, (AndFilter) filter, parameters, fieldConfigs, sb);

        } else if (filter instanceof OrFilter) {
            return convert(sourceConfig, (OrFilter) filter, parameters, fieldConfigs, sb);

        } else if (filter instanceof SimpleFilter) {
            return convert(sourceConfig, (SimpleFilter) filter, parameters, fieldConfigs, sb);
        }

        return true;
    }

    protected boolean convert(
            SourceConfig sourceConfig,
            SimpleFilter filter,
            Collection parameters,
            Collection fieldConfigs,
            StringBuilder sb)
            throws Exception {

        String name = filter.getAttribute();
        String operator = filter.getOperator();
        String value = filter.getValue();

        boolean debug = log.isDebugEnabled();
        if (debug) log.debug("Converting simple filter "+name+" "+operator+" "+value);

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
        if (fieldConfig == null) {
            if (debug) log.debug("Unknown field: "+name);
            return false;
        }

        if ("VARCHAR".equals(fieldConfig.getType())) {
            if (!fieldConfig.isCaseSensitive()) sb.append("lower(");
            sb.append(fieldConfig.getOriginalName());
            if (!fieldConfig.isCaseSensitive()) sb.append(")");
            sb.append(" ");
            sb.append(operator);
            sb.append(" ");
            if (!fieldConfig.isCaseSensitive()) sb.append("lower(");
            sb.append("?");
            if (!fieldConfig.isCaseSensitive()) sb.append(")");

        } else {
            sb.append(fieldConfig.getOriginalName());
            sb.append(" ");
            sb.append(operator);
            sb.append(" ?");
        }

        parameters.add(value);
        fieldConfigs.add(fieldConfig);
        return true;
    }

    boolean convert(
            SourceConfig sourceConfig,
            NotFilter filter,
            Collection parameters,
            Collection fieldConfigs,
            StringBuilder sb)
            throws Exception {

        StringBuilder sb2 = new StringBuilder();

        Filter f = filter.getFilter();
        convert(sourceConfig, f, parameters, fieldConfigs, sb2);

        sb.append("not (");
        sb.append(sb2);
        sb.append(")");

        return true;
    }

    boolean convert(
            SourceConfig sourceConfig,
            AndFilter filter,
            Collection parameters,
            Collection fieldConfigs,
            StringBuilder sb)
            throws Exception {

        StringBuilder sb2 = new StringBuilder();
        for (Iterator i = filter.getFilters().iterator(); i.hasNext();) {
            Filter f = (Filter) i.next();

            StringBuilder sb3 = new StringBuilder();
            convert(sourceConfig, f, parameters, fieldConfigs, sb3);

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
            Collection fieldConfigs,
            StringBuilder sb)
            throws Exception {

        StringBuilder sb2 = new StringBuilder();
        for (Iterator i = filter.getFilters().iterator(); i.hasNext();) {
            Filter f = (Filter) i.next();

            StringBuilder sb3 = new StringBuilder();
            convert(sourceConfig, f, parameters, fieldConfigs, sb3);

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
