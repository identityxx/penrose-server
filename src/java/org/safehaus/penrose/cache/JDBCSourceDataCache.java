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
public class JDBCSourceDataCache extends SourceDataCache {

    public JDBCSourceDataCache(Cache cache, SourceDefinition sourceDefinition) {
        super(cache, sourceDefinition);
    }

    public Connection getConnection() throws Exception {
        String driver = cache.getParameter("driver");
        String url = cache.getParameter("url");
        String user = cache.getParameter("user");
        String password = cache.getParameter("password");

        Class.forName(driver);
        return DriverManager.getConnection(url, user, password);
    }

    public String getTableName() {
        return sourceDefinition.getConnectionName()+"_"+sourceDefinition.getName();
    }

    public void init() throws Exception {
        dropMainTable();
        createMainTable();

        Collection fields = sourceDefinition.getFields();
        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            FieldDefinition fieldDefinition = (FieldDefinition)i.next();
            if (fieldDefinition.isPrimaryKey()) continue;

            dropFieldTable(fieldDefinition);
            createFieldTable(fieldDefinition);
        }
    }

    public String getFieldDeclaration(FieldDefinition fieldDefinition) {
        StringBuffer sb = new StringBuffer();
        sb.append(fieldDefinition.getName());
        sb.append(" ");
        sb.append(fieldDefinition.getType());

        if (fieldDefinition.getLength() != FieldDefinition.DEFAULT_LENGTH) {
            sb.append("(");
            sb.append(fieldDefinition.getLength());

            if ("DOUBLE".equals(fieldDefinition.getType())) {
                sb.append(", ");
                sb.append(fieldDefinition.getPrecision());
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

        Collection fields = sourceDefinition.getFields();
        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            FieldDefinition fieldDefinition = (FieldDefinition)i.next();
            if (!fieldDefinition.isPrimaryKey()) continue;

            if (columns.length() > 0) columns.append(", ");
            columns.append(getFieldDeclaration(fieldDefinition));

            if (primaryKeys.length() > 0) primaryKeys.append(", ");
            primaryKeys.append(fieldDefinition.getName());
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

    public void dropFieldTable(FieldDefinition fieldDefinition) throws Exception {
        StringBuffer sb = new StringBuffer();
        sb.append("drop table ");
        sb.append(getTableName());
        sb.append("_");
        sb.append(fieldDefinition.getName());

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

    public void createFieldTable(FieldDefinition fieldDefinition) throws Exception {
        StringBuffer columns = new StringBuffer();
        StringBuffer primaryKeys = new StringBuffer();

        Collection fields = sourceDefinition.getFields();
        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            FieldDefinition field = (FieldDefinition)i.next();
            if (!field.isPrimaryKey()) continue;

            if (columns.length() > 0) columns.append(", ");
            columns.append(getFieldDeclaration(field));

            if (primaryKeys.length() > 0) primaryKeys.append(", ");
            primaryKeys.append(field.getName());
        }

        columns.append(", ");
        columns.append(getFieldDeclaration(fieldDefinition));

        primaryKeys.append(", ");
        primaryKeys.append(fieldDefinition.getName());

        StringBuffer sb = new StringBuffer();
        sb.append("create table ");
        sb.append(getTableName());
        sb.append("_");
        sb.append(fieldDefinition.getName());
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

    public Map get(Collection filters) throws Exception {

        Collection pks = search(filters);
        Map values = new TreeMap();

        Collection fields = sourceDefinition.getFields();
        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            FieldDefinition fieldDefinition = (FieldDefinition)i.next();
            if (fieldDefinition.isPrimaryKey()) continue;

            getFieldValues(fieldDefinition, pks, values);
        }

        return values;
    }

    public Collection search(Collection filters) throws Exception {

        StringBuffer columns = new StringBuffer();

        Collection fields = sourceDefinition.getFields();
        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            FieldDefinition fieldDefinition = (FieldDefinition)i.next();
            if (!fieldDefinition.isPrimaryKey()) continue;

            if (columns.length() > 0) columns.append(", ");
            columns.append(getTableName());
            columns.append(".");
            columns.append(fieldDefinition.getName());
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

                FieldDefinition fieldDefinition = sourceDefinition.getFieldDefinition(name);

                String tableName;
                if (fieldDefinition.isPrimaryKey()) {
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
            for (Iterator j=fields.iterator(); j.hasNext(); ) {
                FieldDefinition fieldDefinition = (FieldDefinition)j.next();
                if (!fieldDefinition.isPrimaryKey()) continue;

                if (columns.length() > 0) columns.append(" and ");
                sb.append(getTableName());
                sb.append(".");
                sb.append(fieldDefinition.getName());
                sb.append("=");
                sb.append(tableName);
                sb.append(".");
                sb.append(fieldDefinition.getName());
            }

            tableNames.append(sb);
        }

        StringBuffer sb = new StringBuffer();
        sb.append("select ");
        sb.append(columns);
        sb.append(" from ");
        sb.append(tableNames);
        sb.append(" where (");
        sb.append(where);
        sb.append(") and ");
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
                for (Iterator i=fields.iterator(); i.hasNext(); ) {
                    FieldDefinition field = (FieldDefinition)i.next();
                    if (!field.isPrimaryKey()) continue;

                    Object value = rs.getObject(counter);
                    sb.append(field.getName());
                    sb.append("=");
                    sb.append(value);
                    sb.append(", ");

                    pk.set(field.getName(), value);
                    counter++;
                }

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

    public Map getFieldValues(FieldDefinition fieldDefinition, Collection pks, Map values) throws Exception {

        StringBuffer columns = new StringBuffer();

        Collection fields = sourceDefinition.getFields();
        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            FieldDefinition field = (FieldDefinition)i.next();
            if (!field.isPrimaryKey()) continue;

            if (columns.length() > 0) columns.append(", ");
            columns.append(field.getName());
        }

        columns.append(", ");
        columns.append(fieldDefinition.getName());

        StringBuffer where = new StringBuffer();
        Collection parameters = new ArrayList();

        for (Iterator i=pks.iterator(); i.hasNext(); ) {
            Row filter = (Row)i.next();

            StringBuffer sb = new StringBuffer();
            for (Iterator j=filter.getNames().iterator(); j.hasNext(); ) {
                String name = (String)j.next();
                Object value = filter.get(name);

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
        sb.append(fieldDefinition.getName());
        sb.append(" where ");
        sb.append(where);

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
                    FieldDefinition field = (FieldDefinition)i.next();
                    if (!field.isPrimaryKey()) continue;

                    Object value = rs.getObject(counter);
                    sb.append(field.getName());
                    sb.append("=");
                    sb.append(value);
                    sb.append(", ");

                    pk.set(field.getName(), value);
                    counter++;
                }

                Object value = rs.getObject(counter);
                sb.append(fieldDefinition.getName());
                sb.append("=");
                sb.append(value);

                log.debug(" - "+sb);

                AttributeValues av = (AttributeValues)values.get(pk);
                if (av == null) {
                    av = new AttributeValues();
                    av.add(pk);
                    values.put(pk, av);
                }

                av.add(fieldDefinition.getName(), value);
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

    public void put(Row pk, AttributeValues sourceValues) throws Exception {

        remove(pk);

        insertEntry(sourceValues);

        Collection fields = sourceDefinition.getFields();
        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            FieldDefinition fieldDefinition = (FieldDefinition)i.next();
            if (fieldDefinition.isPrimaryKey()) continue;

            Collection values = sourceValues.get(fieldDefinition.getName());
            if (values == null) continue;

            Iterator iterator = values.iterator();
            if (!iterator.hasNext()) continue;

            Object value = iterator.next();
            if (value == null) continue;

            insertFieldValue(fieldDefinition, sourceValues, value);
        }
    }

    public void insertEntry(AttributeValues sourceValues) throws Exception {

        StringBuffer columns = new StringBuffer();
        StringBuffer questionMarks = new StringBuffer();
        Collection parameters = new ArrayList();

        Collection fields = sourceDefinition.getFields();
        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            FieldDefinition field = (FieldDefinition)i.next();
            if (!field.isPrimaryKey()) continue;

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

    public void insertFieldValue(FieldDefinition fieldDefinition, AttributeValues sourceValues, Object value) throws Exception {

        StringBuffer columns = new StringBuffer();
        StringBuffer questionMarks = new StringBuffer();
        Collection parameters = new ArrayList();

        Collection fields = sourceDefinition.getFields();
        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            FieldDefinition field = (FieldDefinition)i.next();
            if (!field.isPrimaryKey()) continue;

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

        columns.append(", ");
        columns.append(fieldDefinition.getName());

        questionMarks.append(", ?");

        parameters.add(value);

        StringBuffer sb = new StringBuffer();
        sb.append("insert into ");
        sb.append(getTableName());
        sb.append("_");
        sb.append(fieldDefinition.getName());
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

        Collection fields = sourceDefinition.getFields();
        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            FieldDefinition fieldDefinition = (FieldDefinition)i.next();
            if (fieldDefinition.isPrimaryKey()) continue;

            deleteFieldValue(fieldDefinition, pk);
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

    public void deleteFieldValue(FieldDefinition fieldDefinition, Row pk) throws Exception {

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
        sb.append(fieldDefinition.getName());
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
