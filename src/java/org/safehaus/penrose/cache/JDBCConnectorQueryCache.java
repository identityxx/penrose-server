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

import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.mapping.Row;
import org.safehaus.penrose.mapping.SourceDefinition;
import org.safehaus.penrose.mapping.FieldDefinition;
import org.safehaus.penrose.mapping.AttributeValues;
import org.ietf.ldap.LDAPException;

import java.util.*;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Connection;
import java.sql.DriverManager;

/**
 * @author Endi S. Dewata
 */
public class JDBCConnectorQueryCache extends ConnectorQueryCache {

    JDBCCacheTool tool = new JDBCCacheTool();

    String driver;
    String url;
    String user;
    String password;

    public void init() throws Exception {
        super.init();

        driver = getCacheConfig().getParameter("driver");
        url = getCacheConfig().getParameter("url");
        user = getCacheConfig().getParameter("user");
        password = getCacheConfig().getParameter("password");

        Class.forName(driver);
    }

    public Connection getConnection() throws Exception {
        return DriverManager.getConnection(url, user, password);
    }

    public Row getPrimaryKey(ResultSet rs) throws Exception {

        Row row = new Row();
        int c = 1;

        Collection fields = getSourceDefinition().getPrimaryKeyFieldDefinitions();
        for (Iterator i=fields.iterator(); i.hasNext(); c++) {
            FieldDefinition fieldDefinition = (FieldDefinition)i.next();

            Object value = rs.getObject(c);
            if (value == null) continue;

            row.set(fieldDefinition.getName(), value);
        }

        return row;
    }

    public Collection get(Filter filter) throws Exception {

        Collection parameters = new ArrayList();
        String sql = tool.convert(getSourceDefinition(), filter, parameters);

        java.sql.Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        Collection pks = new ArrayList();

        try {
            con = getConnection();

            log.debug(Formatter.displaySeparator(80));
            Collection lines = Formatter.split(sql, 80);
            for (Iterator i=lines.iterator(); i.hasNext(); ) {
                String line = (String)i.next();
                log.debug(Formatter.displayLine(line, 80));
            }
            log.debug(Formatter.displaySeparator(80));

            ps = con.prepareStatement(sql);

            log.debug(Formatter.displayLine("Parameters:", 80));

            int counter = 0;
            for (Iterator i=parameters.iterator(); i.hasNext(); ) {
                Object param = i.next();
                ps.setObject(++counter, param);
                log.debug(Formatter.displayLine(" - "+counter+" = "+param, 80));
            }

            log.debug(Formatter.displaySeparator(80));

            rs = ps.executeQuery();

            int width = 0;
            boolean first = true;

            for (int i=0; rs.next(); i++) {
                Row pk = getPrimaryKey(rs);
                pks.add(pk);

                if (first) {
                    width = printHeader();
                    first = false;
                }

                printValues(pk);
            }

            if (width > 0) printFooter(width);

        } finally {
            if (rs != null) try { rs.close(); } catch (Exception e) {}
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }

        return pks;
    }

    public void put(Filter filter, Collection pks) throws Exception {
    }

    public void invalidate() throws Exception {
    }

    public int printHeader() throws Exception {

        StringBuffer resultHeader = new StringBuffer();
        resultHeader.append("|");

        Collection fields = getSourceDefinition().getFieldDefinitions();
        for (Iterator j=fields.iterator(); j.hasNext(); ) {
            FieldDefinition fieldDefinition = (FieldDefinition)j.next();

            String name = fieldDefinition.getName();
            int length = fieldDefinition.getLength() > 15 ? 15 : fieldDefinition.getLength();

            resultHeader.append(" ");
            resultHeader.append(Formatter.rightPad(name, length));
            resultHeader.append(" |");
        }

        int width = resultHeader.length();

        log.debug("Results:");
        log.debug(Formatter.displaySeparator(width));
        log.debug(resultHeader.toString());
        log.debug(Formatter.displaySeparator(width));

        return width;
    }

    public void printValues(Row row) throws Exception {
        StringBuffer resultFields = new StringBuffer();
        resultFields.append("|");

        Collection fields = getSourceDefinition().getPrimaryKeyFieldDefinitions();
        for (Iterator j=fields.iterator(); j.hasNext(); ) {
            FieldDefinition fieldDefinition = (FieldDefinition)j.next();

            Object value = row.get(fieldDefinition.getName());
            int length = fieldDefinition.getLength() > 15 ? 15 : fieldDefinition.getLength();

            resultFields.append(" ");
            resultFields.append(Formatter.rightPad(value == null ? "null" : value.toString(), length));
            resultFields.append(" |");
        }

        log.debug(resultFields.toString());
    }

    public void printFooter(int width) throws Exception {
        log.debug(Formatter.displaySeparator(width));
    }
}
