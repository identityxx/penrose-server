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
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.filter.Filter;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class JDBCCache {

    static Logger log = Logger.getLogger(JDBCCache.class);

    JDBCCacheTool tool = new JDBCCacheTool();

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

        //log.debug("Cache parameters:");
        for (Iterator i=cacheConfig.getParameterNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            String value = cacheConfig.getParameter(name);
            //log.debug(" - "+name+": "+value);
        }

        driver = cacheConfig.getParameter("driver");
        url = cacheConfig.getParameter("url");
        user = cacheConfig.getParameter("user");
        password = cacheConfig.getParameter("password");

        //log.debug("Source parameters:");
        for (Iterator i=sourceDefinition.getParameterNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            String value = sourceDefinition.getParameter(name);
            //log.debug(" - "+name+": "+value);
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

    public void clean() throws Exception {
        log.info("Cleaning cache tables for "+sourceDefinition.getName()+" ...");

        cleanMainTable();

        Collection fields = sourceDefinition.getNonPrimaryKeyFieldDefinitions();
        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            FieldDefinition fieldDefinition = (FieldDefinition)i.next();
            cleanFieldTable(fieldDefinition);
        }

        cleanChangesTable();
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

            if (log.isDebugEnabled()) {
                log.debug(Formatter.displaySeparator(80));
                log.debug(Formatter.displayLine(sql, 80));
                log.debug(Formatter.displaySeparator(80));
            }

            ps = con.prepareStatement(sql);
            ps.execute();

        } catch (Exception e) {
            log.error(e.getMessage());

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }
    }

    public void cleanMainTable() throws Exception {
        StringBuffer sb = new StringBuffer();
        sb.append("delete from ");
        sb.append(getTableName());

        String sql = sb.toString();

        Connection con = null;
        PreparedStatement ps = null;

        try {
            con = getConnection();

            if (log.isDebugEnabled()) {
                log.debug(Formatter.displaySeparator(80));
                log.debug(Formatter.displayLine(sql, 80));
                log.debug(Formatter.displaySeparator(80));
            }

            ps = con.prepareStatement(sql);
            ps.execute();

        } catch (Exception e) {
            log.error(e.getMessage());

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }
    }

    public void cleanFieldTable(FieldDefinition fieldDefinition) throws Exception {
        StringBuffer sb = new StringBuffer();
        sb.append("delete from ");
        sb.append(getTableName());
        sb.append("_");
        sb.append(fieldDefinition.getName());

        String sql = sb.toString();

        Connection con = null;
        PreparedStatement ps = null;

        try {
            con = getConnection();

            if (log.isDebugEnabled()) {
                log.debug(Formatter.displaySeparator(80));
                log.debug(Formatter.displayLine(sql, 80));
                log.debug(Formatter.displaySeparator(80));
            }

            ps = con.prepareStatement(sql);
            ps.execute();

        } catch (Exception e) {
            log.error(e.getMessage());

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }
    }

    public void cleanChangesTable() throws Exception {
        StringBuffer sb = new StringBuffer();
        sb.append("delete from ");
        sb.append(getTableName()+"_changes");

        String sql = sb.toString();

        Connection con = null;
        PreparedStatement ps = null;

        try {
            con = getConnection();

            if (log.isDebugEnabled()) {
                log.debug(Formatter.displaySeparator(80));
                log.debug(Formatter.displayLine(sql, 80));
                log.debug(Formatter.displaySeparator(80));
            }

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

            if (log.isDebugEnabled()) {
                log.debug(Formatter.displaySeparator(80));
                log.debug(Formatter.displayLine(sql, 80));
                log.debug(Formatter.displaySeparator(80));
            }

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

            if (log.isDebugEnabled()) {
                log.debug(Formatter.displaySeparator(80));
                log.debug(Formatter.displayLine(sql, 80));
                log.debug(Formatter.displaySeparator(80));
            }

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

            if (log.isDebugEnabled()) {
                log.debug(Formatter.displaySeparator(80));
                log.debug(Formatter.displayLine(create+" (", 80));
                log.debug(Formatter.displayLine("    "+columns.toString(), 80));
                log.debug(Formatter.displayLine(")", 80));
                log.debug(Formatter.displaySeparator(80));
            }

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

            if (log.isDebugEnabled()) {
                log.debug(Formatter.displaySeparator(80));
                log.debug(Formatter.displayLine(create+" (", 80));
                log.debug(Formatter.displayLine("    "+columns.toString(), 80));
                log.debug(Formatter.displayLine(")", 80));
                log.debug(Formatter.displaySeparator(80));
            }

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

            if (log.isDebugEnabled()) {
                log.debug(Formatter.displaySeparator(80));
                log.debug(Formatter.displayLine(create+" (", 80));
                log.debug(Formatter.displayLine("    "+columns.toString(), 80));
                log.debug(Formatter.displayLine(")", 80));
                log.debug(Formatter.displaySeparator(80));
            }

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

    public Map load(Collection keys, Collection missingKeys) throws Exception {

        Map values = new TreeMap();
        if (keys.isEmpty()) return values;

        Collection pks = searchPrimaryKeys(keys, missingKeys);

        Collection fields = sourceDefinition.getNonPrimaryKeyFieldDefinitions();
        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            FieldDefinition fieldDefinition = (FieldDefinition)i.next();
            getColumnValues(fieldDefinition, pks, values);
        }

        return values;
    }

    public String createSearchQuery(Collection keys, Collection parameters) throws Exception {

        Collection tables = new LinkedHashSet();
        String tableName = getTableName();

        StringBuffer columns = new StringBuffer();
        Collection pkFields = sourceDefinition.getPrimaryKeyFieldDefinitions();
        for (Iterator i=pkFields.iterator(); i.hasNext(); ) {
            FieldDefinition fieldDefinition = (FieldDefinition)i.next();
            String fieldName = fieldDefinition.getName();

            if (columns.length() > 0) columns.append(", ");
            columns.append(tableName);
            columns.append(".");
            columns.append(fieldName);
        }

        Collection uniqueFields = sourceDefinition.getUniqueFieldDefinitions();
        for (Iterator i=uniqueFields.iterator(); i.hasNext(); ) {
            FieldDefinition fieldDefinition = (FieldDefinition)i.next();
            String fieldName = fieldDefinition.getName();

            String t = getTableName()+"_"+fieldName;
            tables.add(t);

            if (columns.length() > 0) columns.append(", ");
            columns.append(t);
            columns.append(".");
            columns.append(fieldName);
        }

        StringBuffer filter = new StringBuffer();
        for (Iterator i=keys.iterator(); i.hasNext(); ) {
            Row key = (Row)i.next();

            StringBuffer sb = new StringBuffer();
            for (Iterator j=key.getNames().iterator(); j.hasNext(); ) {
                String name = (String)j.next();
                Object value = key.get(name);

                FieldDefinition fieldDefinition = sourceDefinition.getFieldDefinition(name);
                String t;

                if (fieldDefinition.isPrimaryKey()) {
                    t = getTableName();
                } else {
                    t = getTableName()+"_"+name;
                    tables.add(t);
                }

                if (sb.length() > 0) sb.append(" and ");
                sb.append(t);
                sb.append(".");
                sb.append(name);
                sb.append(" = ?");

                parameters.add(value);
            }

            if (filter.length() > 0) filter.append(" or ");
            filter.append("(");
            filter.append(sb);
            filter.append(")");
        }

        StringBuffer whereClause = new StringBuffer();

        if (filter.length() > 0) {
            whereClause.append("(");
            whereClause.append(filter);
            whereClause.append(")");
        }

        String s = sourceDefinition.getParameter(SourceDefinition.REFRESH_METHOD);
        String refreshMethod = s == null ? SourceDefinition.DEFAULT_REFRESH_METHOD : s;

        if (!SourceDefinition.POLL_CHANGES.equals(refreshMethod)) {
            if (whereClause.length() > 0) whereClause.append(" and ");
            whereClause.append(tableName);
            whereClause.append(".expiration >= ?");

            parameters.add(new Timestamp(System.currentTimeMillis()));
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
                sb.append("=");
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

    public Collection searchPrimaryKeys(Collection keys, Collection missingKeys) throws Exception {

        missingKeys.addAll(keys);

        Collection parameters = new ArrayList();
        String sql = createSearchQuery(keys, parameters);

        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        Collection pks = new ArrayList();

        try {
            con = getConnection();

            if (log.isDebugEnabled()) {
                log.debug(Formatter.displaySeparator(80));
                Collection lines = Formatter.split(sql, 80);
                for (Iterator i=lines.iterator(); i.hasNext(); ) {
                    String line = (String)i.next();
                    log.debug(Formatter.displayLine(line, 80));
                }
                log.debug(Formatter.displaySeparator(80));
            }

            ps = con.prepareStatement(sql);

            int counter = 1;
            for (Iterator i=parameters.iterator(); i.hasNext(); counter++) {
                Object param = i.next();
                ps.setObject(counter, param);
            }

            if (log.isDebugEnabled()) {
                log.debug(Formatter.displayLine("Parameters:", 80));
                counter = 1;
                for (Iterator i=parameters.iterator(); i.hasNext(); counter++) {
                    Object param = i.next();
                    log.debug(Formatter.displayLine(" - "+counter+" = "+param, 80));
                }
                log.debug(Formatter.displaySeparator(80));
            }

            rs = ps.executeQuery();

            int width = 0;
            boolean first = true;

            log.debug("Results:");
            while (rs.next()) {
                Row pk = getPrimaryKey(rs);

                pks.add(pk);
                missingKeys.remove(pk);

                Collection uniqueKeys = getUniqueKeys(rs);
                missingKeys.removeAll(uniqueKeys);

                if (log.isDebugEnabled()) {
                    if (first) {
                        width = printPrimaryKeyHeader();
                        first = false;
                    }

                    printPrimaryKey(pk);
                }
            }

            if (log.isDebugEnabled()) {
                if (width > 0) printFooter(width);
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

    public Row getPrimaryKey(ResultSet rs) throws Exception {
        Row pk = new Row();

        Collection pkFields = sourceDefinition.getPrimaryKeyFieldDefinitions();
        for (Iterator i=pkFields.iterator(); i.hasNext(); ) {
            FieldDefinition fieldDefinition = (FieldDefinition)i.next();
            Object value = rs.getObject(fieldDefinition.getName());

            pk.set(fieldDefinition.getName(), value);
        }

        return pk;
    }

    public Collection getUniqueKeys(ResultSet rs) throws Exception {
        Collection uniqueKeys = new ArrayList();

        Collection uniqueFields = sourceDefinition.getUniqueFieldDefinitions();
        for (Iterator i=uniqueFields.iterator(); i.hasNext(); ) {
            FieldDefinition fieldDefinition = (FieldDefinition)i.next();
            Object value = rs.getObject(fieldDefinition.getName());

            Row key = new Row();
            key.set(fieldDefinition.getName(), value);
            uniqueKeys.add(key);
        }

        return uniqueKeys;
    }

    public String createLoadQuery(FieldDefinition fieldDefinition, Collection pks, Collection parameters) throws Exception {
        StringBuffer columns = new StringBuffer();

        Collection pkFields = sourceDefinition.getPrimaryKeyFieldDefinitions();
        for (Iterator i=pkFields.iterator(); i.hasNext(); ) {
            FieldDefinition field = (FieldDefinition)i.next();

            if (columns.length() > 0) columns.append(", ");
            columns.append(field.getName());
        }

        columns.append(", ");
        columns.append(fieldDefinition.getName());

        StringBuffer where = new StringBuffer();

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

        return sb.toString();
    }

    public Map getColumnValues(FieldDefinition fieldDefinition, Collection pks, Map values) throws Exception {

        if (pks.isEmpty()) return values;

        Collection parameters = new ArrayList();
        String sql = createLoadQuery(fieldDefinition, pks, parameters);

        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            con = getConnection();

            if (log.isDebugEnabled()) {
                log.debug(Formatter.displaySeparator(80));
                Collection lines = Formatter.split(sql, 80);
                for (Iterator i=lines.iterator(); i.hasNext(); ) {
                    String line = (String)i.next();
                    log.debug(Formatter.displayLine(line, 80));
                }
                log.debug(Formatter.displaySeparator(80));
            }

            ps = con.prepareStatement(sql);

            int counter = 1;
            for (Iterator i=parameters.iterator(); i.hasNext(); counter++) {
                Object param = i.next();
                ps.setObject(counter, param);
            }

            if (log.isDebugEnabled()) {
                log.debug(Formatter.displayLine("Parameters:", 80));
                counter = 1;
                for (Iterator i=parameters.iterator(); i.hasNext(); counter++) {
                    Object param = i.next();
                    log.debug(Formatter.displayLine(" - "+counter+" = "+param, 80));
                }
                log.debug(Formatter.displaySeparator(80));
            }

            rs = ps.executeQuery();

            int width = 0;
            boolean first = true;

            log.debug("Results:");
            while (rs.next()) {
                Row pk = getPrimaryKey(rs);

                Object value = rs.getObject(fieldDefinition.getName());

                AttributeValues av = (AttributeValues)values.get(pk);
                if (av == null) {
                    av = new AttributeValues();
                    av.add(pk);
                    values.put(pk, av);
                }

                av.add(fieldDefinition.getName(), value);

                if (first) {
                    width = printPrimaryKeyHeader();
                    first = false;
                }

                printPrimaryKey(pk);
            }

            if (width > 0) printFooter(width);

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
        if (!insertEntry(pk)) updateEntry(pk);

        Collection fields = sourceDefinition.getNonPrimaryKeyFieldDefinitions();
        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            FieldDefinition fieldDefinition = (FieldDefinition)i.next();

            deleteColumnValue(fieldDefinition, pk);

            Collection values = sourceValues.get(fieldDefinition.getName());
            if (values == null) continue;

            for (Iterator iterator = values.iterator(); iterator.hasNext(); ) {
                Object value = iterator.next();
                insertColumnValue(pk, fieldDefinition, value);
            }
        }
    }

    public boolean insertEntry(Row pk) throws Exception {

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

            if (log.isDebugEnabled()) {
                log.debug(Formatter.displaySeparator(80));
                Collection lines = Formatter.split(sql, 80);
                for (Iterator i=lines.iterator(); i.hasNext(); ) {
                    String line = (String)i.next();
                    log.debug(Formatter.displayLine(line, 80));
                }
                log.debug(Formatter.displaySeparator(80));
            }

            ps = con.prepareStatement(sql);

            int counter = 1;
            for (Iterator i=parameters.iterator(); i.hasNext(); counter++) {
                Object v = i.next();
                ps.setObject(counter, v);
                log.debug(" "+counter+" = "+v+" ("+(v == null ? null : v.getClass().getName())+")");
            }

            ps.execute();
            return true;

        } catch (Exception e) {
            return false;
            //log.error(e.getMessage(), e);

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }
    }

    public void updateEntry(Row pk) throws Exception {

        Collection parameters = new ArrayList();
        parameters.add(new Timestamp(System.currentTimeMillis() + expiration * 60 * 1000));

        StringBuffer whereClause = new StringBuffer();
        Collection pkFields = sourceDefinition.getPrimaryKeyFieldDefinitions();
        for (Iterator i=pkFields.iterator(); i.hasNext(); ) {
            FieldDefinition field = (FieldDefinition)i.next();

            if (whereClause.length() > 0) whereClause.append(" and ");

            whereClause.append(field.getName());
            whereClause.append(" = ?");

            Object value = pk.get(field.getName());
            parameters.add(value);
        }

        StringBuffer sb = new StringBuffer();
        sb.append("update ");
        sb.append(getTableName());
        sb.append(" set expiration = ? where ");
        sb.append(whereClause);

        String sql = sb.toString();

        Connection con = null;
        PreparedStatement ps = null;

        try {
            con = getConnection();

            if (log.isDebugEnabled()) {
                log.debug(Formatter.displaySeparator(80));
                Collection lines = Formatter.split(sql, 80);
                for (Iterator i=lines.iterator(); i.hasNext(); ) {
                    String line = (String)i.next();
                    log.debug(Formatter.displayLine(line, 80));
                }
                log.debug(Formatter.displaySeparator(80));
            }

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

            if (log.isDebugEnabled()) {
                log.debug(Formatter.displaySeparator(80));
                Collection lines = Formatter.split(sql, 80);
                for (Iterator i=lines.iterator(); i.hasNext(); ) {
                    String line = (String)i.next();
                    log.debug(Formatter.displayLine(line, 80));
                }
                log.debug(Formatter.displaySeparator(80));
            }

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

            if (log.isDebugEnabled()) {
                log.debug(Formatter.displaySeparator(80));
                Collection lines = Formatter.split(sql, 80);
                for (Iterator i=lines.iterator(); i.hasNext(); ) {
                    String line = (String)i.next();
                    log.debug(Formatter.displayLine(line, 80));
                }
                log.debug(Formatter.displaySeparator(80));
            }

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

            if (log.isDebugEnabled()) {
                log.debug(Formatter.displaySeparator(80));
                Collection lines = Formatter.split(sql, 80);
                for (Iterator i=lines.iterator(); i.hasNext(); ) {
                    String line = (String)i.next();
                    log.debug(Formatter.displayLine(line, 80));
                }
                log.debug(Formatter.displaySeparator(80));
            }

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

            if (log.isDebugEnabled()) {
                log.debug(Formatter.displaySeparator(80));
                Collection lines = Formatter.split(sql, 80);
                for (Iterator i=lines.iterator(); i.hasNext(); ) {
                    String line = (String)i.next();
                    log.debug(Formatter.displayLine(line, 80));
                }
                log.debug(Formatter.displaySeparator(80));
            }

            ps = con.prepareStatement(sql);

            rs = ps.executeQuery();

            if (!rs.next()) return 0;

            Integer value = (Integer)rs.getObject(1);

            if (log.isDebugEnabled()) {
                log.debug(Formatter.displaySeparator(80));
                log.debug(Formatter.displayLine("Results:", 80));
                log.debug(Formatter.displayLine(" - "+value, 80));
                log.debug(Formatter.displaySeparator(80));
            }

            return value.intValue();

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

            if (log.isDebugEnabled()) {
                log.debug(Formatter.displaySeparator(80));
                Collection lines = Formatter.split(sql, 80);
                for (Iterator i=lines.iterator(); i.hasNext(); ) {
                    String line = (String)i.next();
                    log.debug(Formatter.displayLine(line, 80));
                }
                log.debug(Formatter.displaySeparator(80));
            }

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

            if (log.isDebugEnabled()) {
                log.debug(Formatter.displaySeparator(80));
                Collection lines = Formatter.split(sql, 80);
                for (Iterator i=lines.iterator(); i.hasNext(); ) {
                    String line = (String)i.next();
                    log.debug(Formatter.displayLine(line, 80));
                }
                log.debug(Formatter.displaySeparator(80));
            }

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

    public Collection search(Filter filter) throws Exception {

        Collection parameters = new ArrayList();
        String sql = tool.convert(sourceDefinition, filter, parameters);

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

            int counter = 1;
            for (Iterator i=parameters.iterator(); i.hasNext(); counter++) {
                Object param = i.next();
                ps.setObject(counter, param);
            }

            if (log.isDebugEnabled()) {
                log.debug(Formatter.displayLine("Parameters:", 80));
                counter = 1;
                for (Iterator i=parameters.iterator(); i.hasNext(); counter++) {
                    Object param = i.next();
                    log.debug(Formatter.displayLine(" - "+counter+" = "+param, 80));
                }
                log.debug(Formatter.displaySeparator(80));
            }

            rs = ps.executeQuery();

            int width = 0;
            boolean first = true;

            log.debug("Results:");
            for (int i=0; rs.next(); i++) {
                Row pk = getPrimaryKey(rs);
                pks.add(pk);

                if (log.isDebugEnabled()) {
                    if (first) {
                        width = printPrimaryKeyHeader();
                        first = false;
                    }

                    printPrimaryKey(pk);
                }
            }

            if (log.isDebugEnabled()) {
                if (width > 0) printFooter(width);
            }

        } finally {
            if (rs != null) try { rs.close(); } catch (Exception e) {}
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }

        return pks;
    }

    public int printPrimaryKeyHeader() throws Exception {

        StringBuffer resultHeader = new StringBuffer();
        resultHeader.append("|");

        Collection pkFields = sourceDefinition.getPrimaryKeyFieldDefinitions();
        for (Iterator j=pkFields.iterator(); j.hasNext(); ) {
            FieldDefinition fieldDefinition = (FieldDefinition)j.next();

            String name = fieldDefinition.getName();
            int length = fieldDefinition.getLength() > 15 ? 15 : fieldDefinition.getLength();

            resultHeader.append(" ");
            resultHeader.append(Formatter.rightPad(name, length));
            resultHeader.append(" |");
        }

        int width = resultHeader.length();

        log.debug(Formatter.displaySeparator(width));
        log.debug(resultHeader.toString());
        log.debug(Formatter.displaySeparator(width));

        return width;
    }

    public void printPrimaryKey(Row row) throws Exception {
        StringBuffer resultFields = new StringBuffer();
        resultFields.append("|");

        Collection fields = sourceDefinition.getPrimaryKeyFieldDefinitions();
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
