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

import org.safehaus.penrose.mapping.*;

import java.util.*;
import java.sql.*;

/**
 * @author Endi S. Dewata
 */
public class JDBCEntryDataCache extends EntryDataCache {

    public Connection getConnection() throws Exception {
        String url = cacheConfig.getParameter("url");
        String user = cacheConfig.getParameter("user");
        String password = cacheConfig.getParameter("password");

        return DriverManager.getConnection(url, user, password);
    }

    public String getTableName() {
        String key = entryDefinition.getRdn()+","+parent.getDn();
        key = key.replace('=', '_');
        key = key.replace(',', '_');
        key = key.replace('.', '_');
        key = key.replace(' ', '_');
        return key;
    }

    public Collection getPrimaryColumns() {
        Collection attributes = entryDefinition.getAttributeDefinitions();
        Collection results = new ArrayList();

        for (Iterator i=attributes.iterator(); i.hasNext(); ) {
            AttributeDefinition attributeDefinition = (AttributeDefinition)i.next();
            if (!attributeDefinition.isRdn()) continue;
            results.add(attributeDefinition);
        }

        return results;
    }

    public Collection getNonPrimaryColumns() {
        Collection attributes = entryDefinition.getAttributeDefinitions();
        Collection results = new ArrayList();

        for (Iterator i=attributes.iterator(); i.hasNext(); ) {
            AttributeDefinition attributeDefinition = (AttributeDefinition)i.next();
            if (attributeDefinition.isRdn()) continue;
            results.add(attributeDefinition);
        }

        return results;
    }

    public void init() throws Exception {
        String driver = cacheConfig.getParameter("driver");
        Class.forName(driver);

        dropMainTable();
        createMainTable();

        Collection attributes = getNonPrimaryColumns();
        for (Iterator i=attributes.iterator(); i.hasNext(); ) {
            AttributeDefinition attributeDefinition = (AttributeDefinition)i.next();

            dropFieldTable(attributeDefinition);
            createFieldTable(attributeDefinition);
        }
    }

    public String getColumnDeclaration(AttributeDefinition attributeDefinition) {
        StringBuffer sb = new StringBuffer();
        sb.append(attributeDefinition.getName());
        sb.append(" ");
        sb.append(attributeDefinition.getType());

        if (attributeDefinition.getLength() > 0) {
            sb.append("(");
            sb.append(attributeDefinition.getLength());

            if ("DOUBLE".equals(attributeDefinition.getType())) {
                sb.append(", ");
                sb.append(attributeDefinition.getPrecision());
            }

            sb.append(")");
        }

        return sb.toString();
    }

    public void dropMainTable() throws Exception {
        StringBuffer sb = new StringBuffer();
        sb.append("drop table ");
        sb.append(getTableName());

        String sql = sb.toString();

        Connection con = null;
        PreparedStatement ps = null;

        try {
            con = getConnection();

            log.debug("Executing "+sql);
            ps = con.prepareStatement(sql);
            ps.execute();

        } catch (Exception e) {
            //log.error(e.getMessage(), e);

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }
    }

    public void createMainTable() throws Exception {
        StringBuffer columns = new StringBuffer();
        StringBuffer primaryKeys = new StringBuffer();

        Collection fields = getPrimaryColumns();
        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            AttributeDefinition attributeDefinition = (AttributeDefinition)i.next();

            if (columns.length() > 0) columns.append(", ");
            columns.append(getColumnDeclaration(attributeDefinition));

            if (primaryKeys.length() > 0) primaryKeys.append(", ");
            primaryKeys.append(attributeDefinition.getName());
        }

        columns.append(", expiration DATETIME");

        StringBuffer sb = new StringBuffer();
        sb.append("create table ");
        sb.append(getTableName());
        sb.append(" (");
        sb.append(columns);
        sb.append(", primary key (");
        sb.append(primaryKeys);
        sb.append("))");

        String sql = sb.toString();

        Connection con = null;
        PreparedStatement ps = null;

        try {
            con = getConnection();

            log.debug("Executing "+sql);
            ps = con.prepareStatement(sql);
            ps.execute();

        } catch (Exception e) {
            log.error(e.getMessage(), e);

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }
    }

    public void dropFieldTable(AttributeDefinition attributeDefinition) throws Exception {
        StringBuffer sb = new StringBuffer();
        sb.append("drop table ");
        sb.append(getTableName());
        sb.append("_");
        sb.append(attributeDefinition.getName());

        String sql = sb.toString();

        Connection con = null;
        PreparedStatement ps = null;

        try {
            con = getConnection();

            log.debug("Executing "+sql);
            ps = con.prepareStatement(sql);
            ps.execute();

        } catch (Exception e) {
            //log.error(e.getMessage(), e);

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }
    }

    public void createFieldTable(AttributeDefinition attributeDefinition) throws Exception {
        StringBuffer columns = new StringBuffer();
        StringBuffer primaryKeys = new StringBuffer();

        Collection attributes = getPrimaryColumns();
        for (Iterator i=attributes.iterator(); i.hasNext(); ) {
            AttributeDefinition attribute = (AttributeDefinition)i.next();

            if (columns.length() > 0) columns.append(", ");
            columns.append(getColumnDeclaration(attribute));

            if (primaryKeys.length() > 0) primaryKeys.append(", ");
            primaryKeys.append(attribute.getName());
        }

        columns.append(", ");
        columns.append(getColumnDeclaration(attributeDefinition));

        primaryKeys.append(", ");
        primaryKeys.append(attributeDefinition.getName());

        StringBuffer sb = new StringBuffer();
        sb.append("create table ");
        sb.append(getTableName());
        sb.append("_");
        sb.append(attributeDefinition.getName());
        sb.append(" (");
        sb.append(columns);
        sb.append(", primary key (");
        sb.append(primaryKeys);
        sb.append("))");

        String sql = sb.toString();

        Connection con = null;
        PreparedStatement ps = null;

        try {
            con = getConnection();

            log.debug("Executing "+sql);
            ps = con.prepareStatement(sql);
            ps.execute();

        } catch (Exception e) {
            log.error(e.getMessage(), e);

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }
    }

    public Object get(Row pk) throws Exception {
        Collection pks = new ArrayList();
        pks.add(pk);

        Map values = new TreeMap();

        Collection fields = getNonPrimaryColumns();
        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            AttributeDefinition attributeDefinition = (AttributeDefinition)i.next();
            getColumnValues(attributeDefinition, pks, values);
        }

        AttributeValues av = (AttributeValues)values.get(pk);

        return av;
    }

    public Map search(Collection filters) throws Exception {

        Collection pks = findPks(filters);
        Map values = new TreeMap();

        Collection fields = getNonPrimaryColumns();
        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            AttributeDefinition attributeDefinition = (AttributeDefinition)i.next();
            getColumnValues(attributeDefinition, pks, values);
        }

        return values;
    }

    public Collection findPks(Collection filters) throws Exception {

        StringBuffer columns = new StringBuffer();

        Collection primaryColumns = getPrimaryColumns();
        for (Iterator i=primaryColumns.iterator(); i.hasNext(); ) {
            AttributeDefinition attributeDefinition = (AttributeDefinition)i.next();

            if (columns.length() > 0) columns.append(", ");
            columns.append(getTableName());
            columns.append(".");
            columns.append(attributeDefinition.getName());
        }

        Collection tables = new LinkedHashSet();

        StringBuffer where = new StringBuffer();
        Collection parameters = new ArrayList();

        for (Iterator i=filters.iterator(); i.hasNext(); ) {
            Row filter = (Row)i.next();

            StringBuffer sb = new StringBuffer();
            for (Iterator j=filter.getNames().iterator(); j.hasNext(); ) {
                String name = (String)j.next();
                Object value = filter.get(name);

                AttributeDefinition attributeDefinition = entryDefinition.getAttributeDefinition(name);

                String tableName;
                if (attributeDefinition.isRdn()) {
                    tableName = getTableName();
                } else {
                    tableName = getTableName()+"_"+name;
                    tables.add(tableName);
                }

                if (sb.length() > 0) sb.append(" and ");
                sb.append(tableName);
                sb.append(".");
                sb.append(name);
                sb.append(" = ?");

                parameters.add(value);
            }

            if (where.length() > 0) where.append(" or ");
            where.append("(");
            where.append(sb);
            where.append(")");
        }

        StringBuffer tableNames = new StringBuffer();
        tableNames.append(getTableName());

        for (Iterator i=tables.iterator(); i.hasNext(); ) {
            String tableName = (String)i.next();
            tableNames.append(" left join ");
            tableNames.append(tableName);
            tableNames.append(" on ");

            StringBuffer sb = new StringBuffer();
            for (Iterator j=primaryColumns.iterator(); j.hasNext(); ) {
                AttributeDefinition attributeDefinition = (AttributeDefinition)j.next();

                if (columns.length() > 0) columns.append(" and ");
                sb.append(getTableName());
                sb.append(".");
                sb.append(attributeDefinition.getName());
                sb.append("=");
                sb.append(tableName);
                sb.append(".");
                sb.append(attributeDefinition.getName());
            }

            tableNames.append(sb);
        }

        StringBuffer sb = new StringBuffer();
        sb.append("select ");
        sb.append(columns);
        sb.append(" from ");
        sb.append(tableNames);
        sb.append(" where ");

        if (where.length() > 0) {
            sb.append("(");
            sb.append(where);
            sb.append(") and ");
        }

        sb.append(getTableName());
        sb.append(".expiration >= ?");

        parameters.add(new Timestamp(System.currentTimeMillis()));

        String sql = sb.toString();

        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        Collection pks = new ArrayList();

        try {
            con = getConnection();

            log.debug("Executing "+sql);
            ps = con.prepareStatement(sql);

            int counter = 1;
            for (Iterator i=parameters.iterator(); i.hasNext(); counter++) {
                Object v = i.next();
                ps.setObject(counter, v);
                log.debug(" "+counter+" = "+v);
            }

            rs = ps.executeQuery();

            log.debug("Results:");
            while (rs.next()) {

                counter = 1;
                sb = new StringBuffer();

                Row pk = new Row();
                for (Iterator i=primaryColumns.iterator(); i.hasNext(); ) {
                    AttributeDefinition attributeDefinition = (AttributeDefinition)i.next();

                    Object value = rs.getObject(counter);

                    if (sb.length() > 0) sb.append(", ");
                    sb.append(attributeDefinition.getName());
                    sb.append("=");
                    sb.append(value);

                    pk.set(attributeDefinition.getName(), value);
                    counter++;
                }

                log.debug(" - "+sb);

                pks.add(pk);
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);

        } finally {
            if (rs != null) try { rs.close(); } catch (Exception e) {}
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }

        return pks;
    }

    public Map getColumnValues(AttributeDefinition attributeDefinition, Collection pks, Map values) throws Exception {

        StringBuffer columns = new StringBuffer();

        Collection fields = getPrimaryColumns();
        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            AttributeDefinition field = (AttributeDefinition)i.next();

            if (columns.length() > 0) columns.append(", ");
            columns.append(field.getName());
        }

        columns.append(", ");
        columns.append(attributeDefinition.getName());

        StringBuffer where = new StringBuffer();
        Collection parameters = new ArrayList();

        for (Iterator i=pks.iterator(); i.hasNext(); ) {
            Row pk = (Row)i.next();

            StringBuffer sb = new StringBuffer();
            for (Iterator j=pk.getNames().iterator(); j.hasNext(); ) {
                String name = (String)j.next();
                Object value = pk.get(name);

                if (sb.length() > 0) sb.append(" and ");
                sb.append(name);
                sb.append(" = ?");

                parameters.add(value);
            }

            if (where.length() > 0) where.append(" or ");
            where.append("(");
            where.append(sb);
            where.append(")");
        }

        StringBuffer sb = new StringBuffer();
        sb.append("select ");
        sb.append(columns);
        sb.append(" from ");
        sb.append(getTableName());
        sb.append("_");
        sb.append(attributeDefinition.getName());

        if (where.length() > 0) {
            sb.append(" where ");
            sb.append(where);
        }

        String sql = sb.toString();

        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            con = getConnection();

            log.debug("Executing "+sql);
            ps = con.prepareStatement(sql);

            int counter = 1;
            for (Iterator i=parameters.iterator(); i.hasNext(); counter++) {
                Object v = i.next();
                ps.setObject(counter, v);
                log.debug(" "+counter+" = "+v);
            }

            rs = ps.executeQuery();

            log.debug("Results:");
            while (rs.next()) {

                counter = 1;
                sb = new StringBuffer();

                Row pk = new Row();
                for (Iterator i=fields.iterator(); i.hasNext(); ) {
                    AttributeDefinition field = (AttributeDefinition)i.next();

                    Object value = rs.getObject(counter);
                    sb.append(field.getName());
                    sb.append("=");
                    sb.append(value);
                    sb.append(", ");

                    pk.set(field.getName(), value);
                    counter++;
                }

                Object value = rs.getObject(counter);
                sb.append(attributeDefinition.getName());
                sb.append("=");
                sb.append(value);

                log.debug(" - "+sb);

                AttributeValues av = (AttributeValues)values.get(pk);
                if (av == null) {
                    av = new AttributeValues();
                    av.add(pk);
                    values.put(pk, av);
                }

                av.add(attributeDefinition.getName(), value);
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);

        } finally {
            if (rs != null) try { rs.close(); } catch (Exception e) {}
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }

        return values;
    }

    public void put(Row pk, Object object) throws Exception {
        AttributeValues sourceValues = (AttributeValues)object;

        remove(pk);

        insertEntry(sourceValues);

        Collection fields = getNonPrimaryColumns();
        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            AttributeDefinition attributeDefinition = (AttributeDefinition)i.next();

            Collection values = sourceValues.get(attributeDefinition.getName());
            if (values == null) continue;

            Iterator iterator = values.iterator();
            if (!iterator.hasNext()) continue;

            Object value = iterator.next();
            if (value == null) continue;

            insertColumnValue(attributeDefinition, sourceValues, value);
        }
    }

    public void insertEntry(AttributeValues sourceValues) throws Exception {

        StringBuffer columns = new StringBuffer();
        StringBuffer questionMarks = new StringBuffer();
        Collection parameters = new ArrayList();

        Collection fields = getPrimaryColumns();
        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            AttributeDefinition field = (AttributeDefinition)i.next();

            Collection values = sourceValues.get(field.getName());
            if (values == null) continue;

            Iterator iterator = values.iterator();
            if (!iterator.hasNext()) continue;

            Object v = iterator.next();

            if (columns.length() > 0) columns.append(", ");
            columns.append(field.getName());

            if (questionMarks.length() > 0) questionMarks.append(", ");
            questionMarks.append("?");

            parameters.add(v);
        }

        columns.append(", expiration");
        questionMarks.append(", ?");
        parameters.add(new Timestamp(System.currentTimeMillis() + expiration * 60 * 1000));

        StringBuffer sb = new StringBuffer();
        sb.append("insert into ");
        sb.append(getTableName());
        sb.append(" (");
        sb.append(columns);
        sb.append(") values (");
        sb.append(questionMarks);
        sb.append(")");

        String sql = sb.toString();

        Connection con = null;
        PreparedStatement ps = null;

        try {
            con = getConnection();

            log.debug("Executing "+sql);
            ps = con.prepareStatement(sql);

            int counter = 1;
            for (Iterator i=parameters.iterator(); i.hasNext(); counter++) {
                Object v = i.next();
                ps.setObject(counter, v);
                log.debug(" "+counter+" = "+v);
            }

            ps.execute();

        } catch (Exception e) {
            //log.error(e.getMessage(), e);

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }
    }

    public void insertColumnValue(AttributeDefinition attributeDefinition, AttributeValues sourceValues, Object value) throws Exception {

        StringBuffer columns = new StringBuffer();
        StringBuffer questionMarks = new StringBuffer();
        Collection parameters = new ArrayList();

        Collection fields = getPrimaryColumns();
        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            AttributeDefinition attribute = (AttributeDefinition)i.next();

            Collection values = sourceValues.get(attribute.getName());
            if (values == null) continue;

            Iterator iterator = values.iterator();
            if (!iterator.hasNext()) continue;

            Object v = iterator.next();

            if (columns.length() > 0) columns.append(", ");
            columns.append(attribute.getName());

            if (questionMarks.length() > 0) questionMarks.append(", ");
            questionMarks.append("?");

            parameters.add(v);
        }

        columns.append(", ");
        columns.append(attributeDefinition.getName());

        questionMarks.append(", ?");

        parameters.add(value);

        StringBuffer sb = new StringBuffer();
        sb.append("insert into ");
        sb.append(getTableName());
        sb.append("_");
        sb.append(attributeDefinition.getName());
        sb.append(" (");
        sb.append(columns);
        sb.append(") values (");
        sb.append(questionMarks);
        sb.append(")");

        String sql = sb.toString();

        Connection con = null;
        PreparedStatement ps = null;

        try {
            con = getConnection();

            log.debug("Executing "+sql);
            ps = con.prepareStatement(sql);

            int counter = 1;
            for (Iterator i=parameters.iterator(); i.hasNext(); counter++) {
                Object v = i.next();
                ps.setObject(counter, v);
                log.debug(" "+counter+" = "+v);
            }

            ps.execute();

        } catch (Exception e) {
            //log.error(e.getMessage(), e);

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }
    }

    public void remove(Row pk) throws Exception {

        deleteEntry(pk);

        Collection fields = getNonPrimaryColumns();
        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            AttributeDefinition attributeDefinition = (AttributeDefinition)i.next();
            deleteColumnValue(attributeDefinition, pk);
        }
    }

    public void deleteEntry(Row pk) throws Exception {

        StringBuffer where = new StringBuffer();
        Collection parameters = new ArrayList();

        for (Iterator i=pk.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Object value = pk.get(name);

            if (where.length() > 0) where.append(" and ");
            where.append(name);
            where.append("=?");

            parameters.add(value);
        }

        StringBuffer sb = new StringBuffer();
        sb.append("delete from ");
        sb.append(getTableName());
        sb.append(" where ");
        sb.append(where);

        String sql = sb.toString();

        Connection con = null;
        PreparedStatement ps = null;

        try {
            con = getConnection();

            log.debug("Executing "+sql);
            ps = con.prepareStatement(sql);

            int counter = 1;
            for (Iterator i=parameters.iterator(); i.hasNext(); counter++) {
                Object v = i.next();
                ps.setObject(counter, v);
                log.debug(" "+counter+" = "+v);
            }

            ps.execute();

        } catch (Exception e) {
            log.error(e.getMessage(), e);

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }
    }

    public void deleteColumnValue(AttributeDefinition attributeDefinition, Row pk) throws Exception {

        StringBuffer where = new StringBuffer();
        Collection parameters = new ArrayList();

        for (Iterator i=pk.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Object value = pk.get(name);

            if (where.length() > 0) where.append(" and ");
            where.append(name);
            where.append("=?");

            parameters.add(value);
        }

        StringBuffer sb = new StringBuffer();
        sb.append("delete from ");
        sb.append(getTableName());
        sb.append("_");
        sb.append(attributeDefinition.getName());
        sb.append(" where ");
        sb.append(where);

        String sql = sb.toString();

        Connection con = null;
        PreparedStatement ps = null;

        try {
            con = getConnection();

            log.debug("Executing "+sql);
            ps = con.prepareStatement(sql);

            int counter = 1;
            for (Iterator i=parameters.iterator(); i.hasNext(); counter++) {
                Object v = i.next();
                ps.setObject(counter, v);
                log.debug(" "+counter+" = "+v);
            }

            ps.execute();

        } catch (Exception e) {
            log.error(e.getMessage(), e);

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }
    }
}
