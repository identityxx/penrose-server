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
public class JDBCEntryCache extends EntryCache {

    private String driver;
    private String url;
    private String user;
    private String password;

    Hashtable env;

    public void init() throws Exception {
        super.init();

        driver = cacheConfig.getParameter("driver");
        url = cacheConfig.getParameter("url");
        user = cacheConfig.getParameter("user");
        password = cacheConfig.getParameter("password");

    }

    public void create() throws Exception {
    }

    public void load() throws Exception {
    }

    public void initDatabase() throws Exception {
        //Class.forName(driver);

        dropMainTable();
        createMainTable();

        Collection attributes = getNonPrimaryColumns();
        for (Iterator i=attributes.iterator(); i.hasNext(); ) {
            AttributeMapping attributeMapping = (AttributeMapping)i.next();

            dropFieldTable(attributeMapping);
            createFieldTable(attributeMapping);
        }
    }

    public Connection getConnection() throws Exception {
        return null;//DriverManager.getConnection(url, user, password);
    }

    public String getTableName() {
        String key = entryMapping.getRdn()+","+parentDn;
        key = key.replace('=', '_');
        key = key.replace(',', '_');
        key = key.replace('.', '_');
        key = key.replace(' ', '_');
        return key;
    }

    public Collection getPrimaryColumns() {
        Collection attributes = entryMapping.getAttributeMappings();
        Collection results = new ArrayList();

        for (Iterator i=attributes.iterator(); i.hasNext(); ) {
            AttributeMapping attributeMapping = (AttributeMapping)i.next();
            if (!attributeMapping.isRdn()) continue;
            results.add(attributeMapping);
        }

        return results;
    }

    public Collection getNonPrimaryColumns() {
        Collection attributes = entryMapping.getAttributeMappings();
        Collection results = new ArrayList();

        for (Iterator i=attributes.iterator(); i.hasNext(); ) {
            AttributeMapping attributeMapping = (AttributeMapping)i.next();
            if (attributeMapping.isRdn()) continue;
            results.add(attributeMapping);
        }

        return results;
    }

    public String getColumnDeclaration(AttributeMapping attributeMapping) {
        StringBuffer sb = new StringBuffer();
        sb.append(attributeMapping.getName());
        sb.append(" ");
        sb.append(attributeMapping.getType());

        if ("VARCHAR".equals(attributeMapping.getType()) && attributeMapping.getLength() > 0) {
            sb.append("(");
            sb.append(attributeMapping.getLength());
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
            AttributeMapping attributeMapping = (AttributeMapping)i.next();

            if (columns.length() > 0) columns.append(", ");
            columns.append(getColumnDeclaration(attributeMapping));

            if (primaryKeys.length() > 0) primaryKeys.append(", ");
            primaryKeys.append(attributeMapping.getName());
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

    public void dropFieldTable(AttributeMapping attributeMapping) throws Exception {
        StringBuffer sb = new StringBuffer();
        sb.append("drop table ");
        sb.append(getTableName());
        sb.append("_");
        sb.append(attributeMapping.getName());

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

    public void createFieldTable(AttributeMapping attributeMapping) throws Exception {
        StringBuffer columns = new StringBuffer();
        StringBuffer primaryKeys = new StringBuffer();

        Collection attributes = getPrimaryColumns();
        for (Iterator i=attributes.iterator(); i.hasNext(); ) {
            AttributeMapping attribute = (AttributeMapping)i.next();

            if (columns.length() > 0) columns.append(", ");
            columns.append(getColumnDeclaration(attribute));

            if (primaryKeys.length() > 0) primaryKeys.append(", ");
            primaryKeys.append(attribute.getName());
        }

        columns.append(", ");
        columns.append(getColumnDeclaration(attributeMapping));

        primaryKeys.append(", ");
        primaryKeys.append(attributeMapping.getName());

        StringBuffer sb = new StringBuffer();
        sb.append("create table ");
        sb.append(getTableName());
        sb.append("_");
        sb.append(attributeMapping.getName());
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

    public Object get(Object pk) throws Exception {
        Collection pks = new ArrayList();
        pks.add(pk);

        Map values = new TreeMap();

        Collection fields = getNonPrimaryColumns();
        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            AttributeMapping attributeMapping = (AttributeMapping)i.next();
            getColumnValues(attributeMapping, pks, values);
        }

        AttributeValues av = (AttributeValues)values.get(pk);

        return av;
    }

    public Map getExpired() throws Exception {
        Map results = new TreeMap();
        return results;
    }

    public Map search(Collection filters) throws Exception {

        Collection pks = findPks(filters);
        Map values = new TreeMap();

        Collection fields = getNonPrimaryColumns();
        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            AttributeMapping attributeMapping = (AttributeMapping)i.next();
            getColumnValues(attributeMapping, pks, values);
        }

        return values;
    }

    public Collection findPks(Collection filters) throws Exception {

        StringBuffer columns = new StringBuffer();

        Collection primaryColumns = getPrimaryColumns();
        for (Iterator i=primaryColumns.iterator(); i.hasNext(); ) {
            AttributeMapping attributeMapping = (AttributeMapping)i.next();

            if (columns.length() > 0) columns.append(", ");
            columns.append(getTableName());
            columns.append(".");
            columns.append(attributeMapping.getName());
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

                AttributeMapping attributeMapping = entryMapping.getAttributeMapping(name);

                String tableName;
                if (attributeMapping.isRdn()) {
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
                AttributeMapping attributeMapping = (AttributeMapping)j.next();

                if (columns.length() > 0) columns.append(" and ");
                sb.append(getTableName());
                sb.append(".");
                sb.append(attributeMapping.getName());
                sb.append("=");
                sb.append(tableName);
                sb.append(".");
                sb.append(attributeMapping.getName());
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
                    AttributeMapping attributeMapping = (AttributeMapping)i.next();

                    Object value = rs.getObject(counter);

                    if (sb.length() > 0) sb.append(", ");
                    sb.append(attributeMapping.getName());
                    sb.append("=");
                    sb.append(value);

                    pk.set(attributeMapping.getName(), value);
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

    public Map getColumnValues(AttributeMapping attributeMapping, Collection pks, Map values) throws Exception {

        StringBuffer columns = new StringBuffer();

        Collection fields = getPrimaryColumns();
        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            AttributeMapping field = (AttributeMapping)i.next();

            if (columns.length() > 0) columns.append(", ");
            columns.append(field.getName());
        }

        columns.append(", ");
        columns.append(attributeMapping.getName());

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
        sb.append(attributeMapping.getName());

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
                    AttributeMapping field = (AttributeMapping)i.next();

                    Object value = rs.getObject(counter);
                    sb.append(field.getName());
                    sb.append("=");
                    sb.append(value);
                    sb.append(", ");

                    pk.set(field.getName(), value);
                    counter++;
                }

                Object value = rs.getObject(counter);
                sb.append(attributeMapping.getName());
                sb.append("=");
                sb.append(value);

                log.debug(" - "+sb);

                AttributeValues av = (AttributeValues)values.get(pk);
                if (av == null) {
                    av = new AttributeValues();
                    av.add(pk);
                    values.put(pk, av);
                }

                av.add(attributeMapping.getName(), value);
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

    public void put(Object key, Object object) throws Exception {

        AttributeValues sourceValues = (AttributeValues)object;

        remove(key);

        insertEntry(sourceValues);

        Collection fields = getNonPrimaryColumns();
        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            AttributeMapping attributeMapping = (AttributeMapping)i.next();

            Collection values = sourceValues.get(attributeMapping.getName());
            if (values == null) continue;

            Iterator iterator = values.iterator();
            if (!iterator.hasNext()) continue;

            Object value = iterator.next();
            if (value == null) continue;

            insertColumnValue(attributeMapping, sourceValues, value);
        }
    }

    public void insertEntry(AttributeValues sourceValues) throws Exception {

        StringBuffer columns = new StringBuffer();
        StringBuffer questionMarks = new StringBuffer();
        Collection parameters = new ArrayList();

        Collection fields = getPrimaryColumns();
        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            AttributeMapping field = (AttributeMapping)i.next();

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

    public void insertColumnValue(AttributeMapping attributeMapping, AttributeValues sourceValues, Object value) throws Exception {

        StringBuffer columns = new StringBuffer();
        StringBuffer questionMarks = new StringBuffer();
        Collection parameters = new ArrayList();

        Collection fields = getPrimaryColumns();
        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            AttributeMapping attribute = (AttributeMapping)i.next();

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
        columns.append(attributeMapping.getName());

        questionMarks.append(", ?");

        parameters.add(value);

        StringBuffer sb = new StringBuffer();
        sb.append("insert into ");
        sb.append(getTableName());
        sb.append("_");
        sb.append(attributeMapping.getName());
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

    public void remove(Object pk) throws Exception {

        deleteEntry(pk);

        Collection fields = getNonPrimaryColumns();
        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            AttributeMapping attributeMapping = (AttributeMapping)i.next();
            deleteColumnValue(attributeMapping, pk);
        }
    }

    public void deleteEntry(Object key) throws Exception {
        Row pk = (Row)key;

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

    public void deleteColumnValue(AttributeMapping attributeMapping, Object key) throws Exception {
        Row pk = (Row)key;

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
        sb.append(attributeMapping.getName());
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

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }
/*
    public String getDriver() {
        return driver;
    }

    public void setDriver(String driver) {
        this.driver = driver;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
*/
}
