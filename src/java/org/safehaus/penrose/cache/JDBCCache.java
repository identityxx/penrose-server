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

import org.safehaus.penrose.config.ServerConfigReader;
import org.safehaus.penrose.config.ServerConfig;
import org.safehaus.penrose.config.ConfigReader;
import org.safehaus.penrose.config.Config;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.connection.AdapterConfig;
import org.safehaus.penrose.connection.Adapter;
import org.safehaus.penrose.SearchResults;
import org.apache.log4j.Logger;

import java.io.File;
import java.sql.*;
import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class JDBCCache {

    static Logger log = Logger.getLogger(JDBCCache.class);

    CacheConfig cacheConfig;
    SourceDefinition sourceDefinition;

    private String driver;
    private String url;
    private String user;
    private String password;

    private int size;
    private int expiration;

    public JDBCCache(
            CacheConfig cacheConfig,
            SourceDefinition sourceDefinition) {

        this.cacheConfig = cacheConfig;
        this.sourceDefinition = sourceDefinition;
    }

    public void init() throws Exception {

        log.debug("Cache parameters:");
        for (Iterator i=cacheConfig.getParameterNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            String value = cacheConfig.getParameter(name);
            log.debug(" - "+name+": "+value);
        }

        driver = cacheConfig.getParameter("driver");
        url = cacheConfig.getParameter("url");
        user = cacheConfig.getParameter("user");
        password = cacheConfig.getParameter("password");

        log.debug("Source parameters:");
        for (Iterator i=sourceDefinition.getParameterNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            String value = sourceDefinition.getParameter(name);
            log.debug(" - "+name+": "+value);
        }

        String s = sourceDefinition.getParameter(SourceDefinition.DATA_CACHE_SIZE);
        if (s != null) size = Integer.parseInt(s);

        s = sourceDefinition.getParameter(SourceDefinition.DATA_CACHE_EXPIRATION);
        if (s != null) expiration = Integer.parseInt(s);

        Class.forName(driver);
    }

    public Connection getConnection() throws Exception {
        return DriverManager.getConnection(url, user, password);
    }

    public String getTableName() {
        return sourceDefinition.getConnectionName()+"_"+sourceDefinition.getName();
    }

    public String getColumnDeclaration(FieldDefinition fieldDefinition) {
        StringBuffer sb = new StringBuffer();
        sb.append(fieldDefinition.getName());
        sb.append(" ");
        sb.append(fieldDefinition.getType());

        if ("VARCHAR".equals(fieldDefinition.getType()) && fieldDefinition.getLength() > 0) {
            sb.append("(");
            sb.append(fieldDefinition.getLength());
            sb.append(")");
        }

        return sb.toString();
    }

    public void create() throws Exception {
        log.info("Creating cache tables for "+sourceDefinition.getName()+" ...");

        createMainTable();

        Collection fields = sourceDefinition.getNonPrimaryKeyFieldDefinitions();
        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            FieldDefinition fieldDefinition = (FieldDefinition)i.next();
            createFieldTable(fieldDefinition);
        }

        createChangesTable();
    }

    public void drop() throws Exception {
        log.info("Dropping cache tables for "+sourceDefinition.getName()+" ...");

        dropMainTable();

        Collection fields = sourceDefinition.getNonPrimaryKeyFieldDefinitions();
        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            FieldDefinition fieldDefinition = (FieldDefinition)i.next();
            dropFieldTable(fieldDefinition);
        }

        dropChangesTable();
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

            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine(sql, 80));
            log.debug(Formatter.displaySeparator(80));

            ps = con.prepareStatement(sql);
            ps.execute();

        } catch (Exception e) {
            log.error(e.getMessage());

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }
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

            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine(sql, 80));
            log.debug(Formatter.displaySeparator(80));

            ps = con.prepareStatement(sql);
            ps.execute();

        } catch (Exception e) {
            log.error(e.getMessage());

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }
    }

    public void dropChangesTable() throws Exception {
        StringBuffer sb = new StringBuffer();
        sb.append("drop table ");
        sb.append(getTableName()+"_changes");

        String sql = sb.toString();

        Connection con = null;
        PreparedStatement ps = null;

        try {
            con = getConnection();

            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine(sql, 80));
            log.debug(Formatter.displaySeparator(80));

            ps = con.prepareStatement(sql);
            ps.execute();

        } catch (Exception e) {
            log.error(e.getMessage());

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }
    }

    public void createChangesTable() throws Exception {
        StringBuffer create = new StringBuffer();
        create.append("create table ");
        create.append(getTableName()+"_changes");

        StringBuffer columns = new StringBuffer();
        columns.append("lastChangeNumber INTEGER");

        String sql = create+" ("+columns+")";

        Connection con = null;
        PreparedStatement ps = null;

        try {
            con = getConnection();

            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine(create+" (", 80));
            log.debug(Formatter.displayLine("    "+columns.toString(), 80));
            log.debug(Formatter.displayLine(")", 80));
            log.debug(Formatter.displaySeparator(80));

            ps = con.prepareStatement(sql);
            ps.execute();

        } catch (Exception e) {
            log.error(e.getMessage(), e);

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }
    }

    public void createMainTable() throws Exception {
        StringBuffer columns = new StringBuffer();
        StringBuffer primaryKeys = new StringBuffer();

        Collection fields = sourceDefinition.getPrimaryKeyFieldDefinitions();
        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            FieldDefinition fieldDefinition = (FieldDefinition)i.next();

            if (columns.length() > 0) columns.append(", ");
            columns.append(getColumnDeclaration(fieldDefinition));

            if (primaryKeys.length() > 0) primaryKeys.append(", ");
            primaryKeys.append(fieldDefinition.getName());
        }

        columns.append(", expiration DATETIME");

        columns.append(", primary key (");
        columns.append(primaryKeys);
        columns.append(")");

        StringBuffer create = new StringBuffer();
        create.append("create table ");
        create.append(getTableName());

        String sql = create+" ("+columns+")";

        Connection con = null;
        PreparedStatement ps = null;

        try {
            con = getConnection();

            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine(create+" (", 80));
            log.debug(Formatter.displayLine("    "+columns.toString(), 80));
            log.debug(Formatter.displayLine(")", 80));
            log.debug(Formatter.displaySeparator(80));

            ps = con.prepareStatement(sql);
            ps.execute();

        } catch (Exception e) {
            log.error(e.getMessage(), e);

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }
    }

    public void createFieldTable(FieldDefinition fieldDefinition) throws Exception {
        StringBuffer columns = new StringBuffer();
        StringBuffer primaryKeys = new StringBuffer();

        Collection fields = sourceDefinition.getPrimaryKeyFieldDefinitions();
        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            FieldDefinition field = (FieldDefinition)i.next();

            if (columns.length() > 0) columns.append(", ");
            columns.append(getColumnDeclaration(field));

            if (primaryKeys.length() > 0) primaryKeys.append(", ");
            primaryKeys.append(field.getName());
        }

        columns.append(", ");
        columns.append(getColumnDeclaration(fieldDefinition));

        primaryKeys.append(", ");
        primaryKeys.append(fieldDefinition.getName());

        columns.append(", primary key (");
        columns.append(primaryKeys);
        columns.append(")");

        StringBuffer create = new StringBuffer();
        create.append("create table ");
        create.append(getTableName());
        create.append("_");
        create.append(fieldDefinition.getName());

        String sql = create+" ("+columns+")";

        Connection con = null;
        PreparedStatement ps = null;

        try {
            con = getConnection();

            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine(create+" (", 80));
            log.debug(Formatter.displayLine("    "+columns.toString(), 80));
            log.debug(Formatter.displayLine(")", 80));
            log.debug(Formatter.displaySeparator(80));

            ps = con.prepareStatement(sql);
            ps.execute();

        } catch (Exception e) {
            log.error(e.getMessage(), e);

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }
    }

    public AttributeValues get(Row pk) throws Exception {
        Collection pks = new ArrayList();
        pks.add(pk);

        Map values = new TreeMap();

        Collection fields = sourceDefinition.getNonPrimaryKeyFieldDefinitions();
        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            FieldDefinition fieldDefinition = (FieldDefinition)i.next();
            getColumnValues(fieldDefinition, pks, values);
        }

        return (AttributeValues)values.get(pk);
    }

    public Map search(Collection keys, Collection missingKeys) throws Exception {

        Map values = new TreeMap();
        if (keys.isEmpty()) return values;

        Collection pks = findPks(keys, missingKeys);

        Collection fields = sourceDefinition.getNonPrimaryKeyFieldDefinitions();
        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            FieldDefinition fieldDefinition = (FieldDefinition)i.next();
            getColumnValues(fieldDefinition, pks, values);
        }

        return values;
    }

    public Collection findPks(Collection keys, Collection missingKeys) throws Exception {

        log.debug("Searching for keys:");

        for (Iterator i=keys.iterator(); i.hasNext(); ) {
            Row pk = (Row)i.next();
            for (Iterator j=pk.getNames().iterator(); j.hasNext(); ) {
                String name = (String)j.next();
                Object value = pk.get(name);
                log.debug(" - "+name+": "+value+" ("+value.getClass().getName()+")");
            }
            missingKeys.add(pk);
        }

        StringBuffer columns = new StringBuffer();

        Collection pkFields = sourceDefinition.getPrimaryKeyFieldDefinitions();
        for (Iterator i=pkFields.iterator(); i.hasNext(); ) {
            FieldDefinition fieldDefinition = (FieldDefinition)i.next();

            if (columns.length() > 0) columns.append(", ");
            columns.append(getTableName());
            columns.append(".");
            columns.append(fieldDefinition.getName());
        }

        Collection tables = new LinkedHashSet();

        StringBuffer where = new StringBuffer();
        Collection parameters = new ArrayList();

        for (Iterator i=keys.iterator(); i.hasNext(); ) {
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
            for (Iterator j=pkFields.iterator(); j.hasNext(); ) {
                FieldDefinition fieldDefinition = (FieldDefinition)j.next();

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
                for (Iterator i=pkFields.iterator(); i.hasNext(); ) {
                    FieldDefinition field = (FieldDefinition)i.next();

                    Object value = rs.getObject(counter);

                    if (sb.length() > 0) sb.append(", ");
                    sb.append(field.getName());
                    sb.append("=");
                    sb.append(value+" ("+value.getClass().getName()+")");

                    pk.set(field.getName(), value);
                    counter++;
                }

                log.debug(" - "+sb);

                pks.add(pk);
                missingKeys.remove(pk);
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

    public Map getColumnValues(FieldDefinition fieldDefinition, Collection pks, Map values) throws Exception {

        if (pks.isEmpty()) return values;

        StringBuffer columns = new StringBuffer();

        Collection fields = sourceDefinition.getPrimaryKeyFieldDefinitions();
        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            FieldDefinition field = (FieldDefinition)i.next();

            if (columns.length() > 0) columns.append(", ");
            columns.append(field.getName());
        }

        columns.append(", ");
        columns.append(fieldDefinition.getName());

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
        sb.append(fieldDefinition.getName());

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
                    FieldDefinition field = (FieldDefinition)i.next();

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
        insertEntry(pk);

        Collection fields = sourceDefinition.getNonPrimaryKeyFieldDefinitions();
        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            FieldDefinition fieldDefinition = (FieldDefinition)i.next();

            Collection values = sourceValues.get(fieldDefinition.getName());
            if (values == null) continue;

            for (Iterator iterator = values.iterator(); iterator.hasNext(); ) {
                Object value = iterator.next();
                insertColumnValue(pk, fieldDefinition, value);
            }
        }
    }

    public void insertEntry(Row pk) throws Exception {

        StringBuffer columns = new StringBuffer();
        StringBuffer questionMarks = new StringBuffer();
        Collection parameters = new ArrayList();

        Collection fields = sourceDefinition.getPrimaryKeyFieldDefinitions();
        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            FieldDefinition field = (FieldDefinition)i.next();

            if (columns.length() > 0) columns.append(", ");
            columns.append(field.getName());

            if (questionMarks.length() > 0) questionMarks.append(", ");
            questionMarks.append("?");

            Object value = pk.get(field.getName());
            parameters.add(value);
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
                log.debug(" "+counter+" = "+v+" ("+(v == null ? null : v.getClass().getName())+")");
            }

            ps.execute();

        } catch (Exception e) {
            //log.error(e.getMessage(), e);

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }
    }

    public void insertColumnValue(Row pk, FieldDefinition fieldDefinition, Object value) throws Exception {

        StringBuffer columns = new StringBuffer();
        StringBuffer questionMarks = new StringBuffer();
        Collection parameters = new ArrayList();

        Collection fields = sourceDefinition.getPrimaryKeyFieldDefinitions();
        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            FieldDefinition field = (FieldDefinition)i.next();

            if (columns.length() > 0) columns.append(", ");
            columns.append(field.getName());

            if (questionMarks.length() > 0) questionMarks.append(", ");
            questionMarks.append("?");

            Object v = pk.get(field.getName());
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

        Collection fields = sourceDefinition.getNonPrimaryKeyFieldDefinitions();
        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            FieldDefinition fieldDefinition = (FieldDefinition)i.next();
            deleteColumnValue(fieldDefinition, pk);
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

    public void deleteColumnValue(FieldDefinition fieldDefinition, Object key) throws Exception {
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

    public int getLastChangeNumber() throws Exception {

        StringBuffer sb = new StringBuffer();
        sb.append("select lastChangeNumber from ");
        sb.append(getTableName()+"_changes");

        String sql = sb.toString();

        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            con = getConnection();

            log.debug("Executing "+sql);
            ps = con.prepareStatement(sql);

            rs = ps.executeQuery();

            log.debug("Results:");
            if (rs.next()) {
                Integer value = (Integer)rs.getObject(1);
                return value.intValue();
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);

        } finally {
            if (rs != null) try { rs.close(); } catch (Exception e) {}
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }

        return 0;
    }

    public void setLastChangeNumber(int lastChangeNumber) throws Exception {

        String sql = "update "+getTableName()+"_changes"+" set lastChangeNumber = ?";

        Collection parameters = new ArrayList();
        parameters.add(new Integer(lastChangeNumber));

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

            int count = ps.executeUpdate();

            if (count > 0) return;

        } catch (Exception e) {
            //log.error(e.getMessage(), e);

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }

        sql = "insert into "+getTableName()+"_changes"+" values (?)";

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

    public static void main(String args[]) throws Exception {

        if (args.length == 0) {
            System.out.println("Usage: JDBCCache [command]");
            System.out.println();
            System.out.println("Commands:");
            System.out.println("    create - create database tables");
            System.out.println("    load   - load data source");
            System.out.println("    drop   - drop database tables");
            System.exit(0);
        }

        String command = args[0];

        String homeDirectory = System.getProperty("penrose.home");

        ServerConfigReader serverConfigReader = new ServerConfigReader();
        ServerConfig serverCfg = serverConfigReader.read((homeDirectory == null ? "" : homeDirectory+File.separator)+"conf"+File.separator+"server.xml");

        CacheConfig cacheCfg = serverCfg.getCacheConfig(SourceDefinition.DEFAULT_CACHE);

        ConfigReader configReader = new ConfigReader();
        Config config = configReader.read((homeDirectory == null ? "" : homeDirectory+File.separator)+"conf");

        Collection connectionConfigs = config.getConnectionConfigs();
        for (Iterator i=connectionConfigs.iterator(); i.hasNext(); ) {
            ConnectionConfig conCfg = (ConnectionConfig)i.next();

            String adapterName = conCfg.getAdapterName();
            if (adapterName == null) throw new Exception("Missing adapter name");

            AdapterConfig adapterConfig = serverCfg.getAdapterConfig(adapterName);
            if (adapterConfig == null) throw new Exception("Undefined adapter "+adapterName);

            String adapterClass = adapterConfig.getAdapterClass();
            Class clazz = Class.forName(adapterClass);
            Adapter adapter = (Adapter)clazz.newInstance();

            org.safehaus.penrose.connection.Connection connection = new org.safehaus.penrose.connection.Connection();
            connection.init(conCfg, adapter);

            adapter.init(adapterConfig, connection);

            Collection sourceDefinitions = conCfg.getSourceDefinitions();
            for (Iterator j=sourceDefinitions.iterator(); j.hasNext(); ) {
                SourceDefinition srcDef = (SourceDefinition)j.next();

                JDBCCache cache = new JDBCCache(cacheCfg, srcDef);
                cache.init();

                if ("create".equals(command)) {
                    cache.create();

                } else if ("load".equals(command)) {
                    load(adapter, cache, srcDef);

                } else if ("drop".equals(command)) {
                    cache.drop();

                }
            }
        }
    }

    public static void load(Adapter adapter, JDBCCache cache, SourceDefinition srcDef) throws Exception {
        SearchResults sr = adapter.load(srcDef, null, 100);

        //log.debug("Results:");
        while (sr.hasNext()) {
            AttributeValues sourceValues = (AttributeValues)sr.next();
            Row pk = Adapter.getPrimaryKeyValues(srcDef, sourceValues);
            //log.debug(" - "+pk+": "+sourceValues);

            cache.put(pk, sourceValues);
        }

        int lastChangeNumber = adapter.getLastChangeNumber(srcDef);
        cache.setLastChangeNumber(lastChangeNumber);
    }

    public String getDriver() {
        return driver;
    }

    public void setDriver(String driver) {
        this.driver = driver;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
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

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public int getExpiration() {
        return expiration;
    }

    public void setExpiration(int expiration) {
        this.expiration = expiration;
    }
}
