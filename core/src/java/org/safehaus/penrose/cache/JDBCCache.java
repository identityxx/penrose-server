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
package org.safehaus.penrose.cache;

import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.connection.ConnectionManager;
import org.safehaus.penrose.source.FieldConfig;
import org.safehaus.penrose.source.SourceConfig;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.ldap.RDNBuilder;
import org.safehaus.penrose.entry.SourceValues;
import org.safehaus.penrose.ldap.RDN;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.sql.*;
import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class JDBCCache {

    public static Logger log = LoggerFactory.getLogger(JDBCCache.class);

    JDBCCacheTool tool = new JDBCCacheTool();

    private ConnectionManager connectionManager;
    private String connectionName;
    SourceConfig sourceConfig;

    private String tableName;

    private int size;
    private int expiration;

    public JDBCCache(
            String tableName,
            SourceConfig sourceConfig) {

        this.tableName = tableName;
        this.sourceConfig = sourceConfig;
    }

    public void init() throws Exception {

    }

    public Connection getConnection() throws Exception {
        return (Connection)connectionManager.openConnection((Partition)null, connectionName);
    }

    public String getTableName() {
        return tableName;
    }

    public void clean() throws Exception {
        log.info("Cleaning cache tables for "+sourceConfig.getName()+" ...");

        cleanMainTable();

        Collection fields = sourceConfig.getNonPrimaryKeyFieldConfigs();
        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            FieldConfig fieldConfig = (FieldConfig)i.next();
            cleanFieldTable(fieldConfig);
        }

        cleanChangesTable();
    }

    public void drop() throws Exception {
        log.info("Dropping cache tables for "+sourceConfig.getName()+" ...");

        dropMainTable();

        Collection fields = sourceConfig.getNonPrimaryKeyFieldConfigs();
        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            FieldConfig fieldConfig = (FieldConfig)i.next();
            dropFieldTable(fieldConfig);
        }

        dropChangesTable();
    }

    public void dropFieldTable(FieldConfig fieldConfig) throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("drop table ");
        sb.append(tableName);
        sb.append("_");
        sb.append(fieldConfig.getName());

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
            log.error(e.getMessage(), e);

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }
    }

    public void cleanMainTable() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("delete from ");
        sb.append(tableName);

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
            log.error(e.getMessage(), e);

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }
    }

    public void cleanFieldTable(FieldConfig fieldConfig) throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("delete from ");
        sb.append(tableName);
        sb.append("_");
        sb.append(fieldConfig.getName());

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
            log.error(e.getMessage(), e);

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }
    }

    public void cleanChangesTable() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("delete from ");
        sb.append(tableName+"_changes");

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
            log.error(e.getMessage(), e);

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }
    }

    public void dropMainTable() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("drop table ");
        sb.append(tableName);

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
            log.error(e.getMessage(), e);

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }
    }

    public void dropChangesTable() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("drop table ");
        sb.append(tableName+"_changes");

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
            log.error(e.getMessage(), e);

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }
    }

    public void createChangesTable() throws Exception {
        StringBuilder create = new StringBuilder();
        create.append("create table ");
        create.append(tableName+"_changes");

        StringBuilder columns = new StringBuilder();
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

    public String createSearchQuery(Collection keys, Collection parameters) throws Exception {

        Collection tables = new LinkedHashSet();

        StringBuilder columns = new StringBuilder();
        Collection pkFields = sourceConfig.getPrimaryKeyFieldConfigs();
        for (Iterator i=pkFields.iterator(); i.hasNext(); ) {
            FieldConfig fieldConfig = (FieldConfig)i.next();
            String fieldName = fieldConfig.getName();

            if (columns.length() > 0) columns.append(", ");
            columns.append(tableName);
            columns.append(".");
            columns.append(fieldName);
        }

        Collection uniqueFields = sourceConfig.getUniqueFieldConfigs();
        for (Iterator i=uniqueFields.iterator(); i.hasNext(); ) {
            FieldConfig fieldConfig = (FieldConfig)i.next();
            String fieldName = fieldConfig.getName();

            String t = tableName+"_"+fieldName;
            tables.add(t);

            if (columns.length() > 0) columns.append(", ");
            columns.append(t);
            columns.append(".");
            columns.append(fieldName);
        }

        StringBuilder filter = new StringBuilder();
        for (Iterator i=keys.iterator(); i.hasNext(); ) {
            RDN key = (RDN)i.next();

            StringBuilder sb = new StringBuilder();
            for (Iterator j=key.getNames().iterator(); j.hasNext(); ) {
                String name = (String)j.next();
                Object value = key.get(name);

                FieldConfig fieldConfig = sourceConfig.getFieldConfig(name);
                String t;

                if (fieldConfig.isPrimaryKey()) {
                    t = tableName;
                } else {
                    t = tableName+"_"+name;
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

        StringBuilder whereClause = new StringBuilder();

        if (filter.length() > 0) {
            whereClause.append("(");
            whereClause.append(filter);
            whereClause.append(")");
        }

        String s = sourceConfig.getParameter(SourceConfig.REFRESH_METHOD);
        String refreshMethod = s == null ? SourceConfig.DEFAULT_REFRESH_METHOD : s;

        if (!SourceConfig.POLL_CHANGES.equals(refreshMethod)) {
            if (whereClause.length() > 0) whereClause.append(" and ");
            whereClause.append(tableName);
            whereClause.append(".expiration >= ?");

            parameters.add(new Timestamp(System.currentTimeMillis()));
        }

        StringBuilder join = new StringBuilder();
        join.append(tableName);

        for (Iterator i=tables.iterator(); i.hasNext(); ) {
            String t = (String)i.next();

            StringBuilder sb = new StringBuilder();
            for (Iterator j=pkFields.iterator(); j.hasNext(); ) {
                FieldConfig fieldConfig = (FieldConfig)j.next();
                String fieldName = fieldConfig.getName();

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

        StringBuilder sb = new StringBuilder();
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

    public RDN getPrimaryKey(ResultSet rs) throws Exception {
        RDNBuilder rb = new RDNBuilder();

        Collection pkFields = sourceConfig.getPrimaryKeyFieldConfigs();
        for (Iterator i=pkFields.iterator(); i.hasNext(); ) {
            FieldConfig fieldConfig = (FieldConfig)i.next();
            Object value = rs.getObject(fieldConfig.getName());

            rb.set(fieldConfig.getName(), value);
        }

        return rb.toRdn();
    }

    public Collection getUniqueKeys(ResultSet rs) throws Exception {
        Collection uniqueKeys = new ArrayList();

        RDNBuilder rb = new RDNBuilder();
        Collection uniqueFields = sourceConfig.getUniqueFieldConfigs();
        for (Iterator i=uniqueFields.iterator(); i.hasNext(); ) {
            FieldConfig fieldConfig = (FieldConfig)i.next();
            Object value = rs.getObject(fieldConfig.getName());

            rb.clear();
            rb.set(fieldConfig.getName(), value);
            uniqueKeys.add(rb.toRdn());
        }

        return uniqueKeys;
    }

    public String createLoadQuery(FieldConfig fieldConfig, Collection pks, Collection parameters) throws Exception {
        StringBuilder columns = new StringBuilder();

        Collection pkFields = sourceConfig.getPrimaryKeyFieldConfigs();
        for (Iterator i=pkFields.iterator(); i.hasNext(); ) {
            FieldConfig field = (FieldConfig)i.next();

            if (columns.length() > 0) columns.append(", ");
            columns.append(field.getName());
        }

        columns.append(", ");
        columns.append(fieldConfig.getName());

        StringBuilder where = new StringBuilder();

        for (Iterator i=pks.iterator(); i.hasNext(); ) {
            RDN pk = (RDN)i.next();

            StringBuilder sb = new StringBuilder();
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

        StringBuilder sb = new StringBuilder();
        sb.append("select ");
        sb.append(columns);
        sb.append(" from ");
        sb.append(tableName);
        sb.append("_");
        sb.append(fieldConfig.getName());

        if (where.length() > 0) {
            sb.append(" where ");
            sb.append(where);
        }

        return sb.toString();
    }

    public void put(RDN pk, SourceValues sourceValues) throws Exception {
        if (!insertEntry(pk)) updateEntry(pk);

        Collection fields = sourceConfig.getNonPrimaryKeyFieldConfigs();
        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            FieldConfig fieldConfig = (FieldConfig)i.next();

            deleteColumnValue(fieldConfig, pk);
/*
            Collection values = sourceValues.get(fieldConfig.getName());
            if (values == null) continue;

            for (Iterator iterator = values.iterator(); iterator.hasNext(); ) {
                Object value = iterator.next();
                insertColumnValue(pk, fieldConfig, value);
            }
*/
        }
    }

    public boolean insertEntry(RDN pk) throws Exception {

        StringBuilder columns = new StringBuilder();
        StringBuilder questionMarks = new StringBuilder();
        Collection parameters = new ArrayList();

        Collection fields = sourceConfig.getPrimaryKeyFieldConfigs();
        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            FieldConfig field = (FieldConfig)i.next();

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

        StringBuilder sb = new StringBuilder();
        sb.append("insert into ");
        sb.append(tableName);
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

            log.debug("Parameters:");
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

    public void updateEntry(RDN pk) throws Exception {

        Collection parameters = new ArrayList();
        parameters.add(new Timestamp(System.currentTimeMillis() + expiration * 60 * 1000));

        StringBuilder whereClause = new StringBuilder();
        Collection pkFields = sourceConfig.getPrimaryKeyFieldConfigs();
        for (Iterator i=pkFields.iterator(); i.hasNext(); ) {
            FieldConfig field = (FieldConfig)i.next();

            if (whereClause.length() > 0) whereClause.append(" and ");

            whereClause.append(field.getName());
            whereClause.append(" = ?");

            Object value = pk.get(field.getName());
            parameters.add(value);
        }

        StringBuilder sb = new StringBuilder();
        sb.append("update ");
        sb.append(tableName);
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

    public void insertColumnValue(RDN pk, FieldConfig fieldConfig, Object value) throws Exception {

        StringBuilder columns = new StringBuilder();
        StringBuilder questionMarks = new StringBuilder();
        Collection parameters = new ArrayList();

        Collection fields = sourceConfig.getPrimaryKeyFieldConfigs();
        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            FieldConfig field = (FieldConfig)i.next();

            if (columns.length() > 0) columns.append(", ");
            columns.append(field.getName());

            if (questionMarks.length() > 0) questionMarks.append(", ");
            questionMarks.append("?");

            Object v = pk.get(field.getName());
            parameters.add(v);
        }

        columns.append(", ");
        columns.append(fieldConfig.getName());

        questionMarks.append(", ?");

        parameters.add(value);

        StringBuilder sb = new StringBuilder();
        sb.append("insert into ");
        sb.append(tableName);
        sb.append("_");
        sb.append(fieldConfig.getName());
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

    public void remove(RDN pk) throws Exception {
        deleteEntry(pk);

        Collection fields = sourceConfig.getNonPrimaryKeyFieldConfigs();
        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            FieldConfig fieldConfig = (FieldConfig)i.next();
            deleteColumnValue(fieldConfig, pk);
        }
    }

    public void deleteEntry(Object key) throws Exception {
        RDN pk = (RDN)key;

        StringBuilder where = new StringBuilder();
        Collection parameters = new ArrayList();

        for (Iterator i=pk.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Object value = pk.get(name);

            if (where.length() > 0) where.append(" and ");
            where.append(name);
            where.append("=?");

            parameters.add(value);
        }

        StringBuilder sb = new StringBuilder();
        sb.append("delete from ");
        sb.append(tableName);
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

    public void deleteColumnValue(FieldConfig fieldConfig, Object key) throws Exception {
        RDN pk = (RDN)key;

        StringBuilder where = new StringBuilder();
        Collection parameters = new ArrayList();

        for (Iterator i=pk.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Object value = pk.get(name);

            if (where.length() > 0) where.append(" and ");
            where.append(name);
            where.append("=?");

            parameters.add(value);
        }

        StringBuilder sb = new StringBuilder();
        sb.append("delete from ");
        sb.append(tableName);
        sb.append("_");
        sb.append(fieldConfig.getName());
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

            log.debug(Formatter.displayLine("Parameters:", 80));

            int counter = 1;
            for (Iterator i=parameters.iterator(); i.hasNext(); counter++) {
                Object v = i.next();
                ps.setObject(counter, v);
                log.debug(Formatter.displayLine(" - "+counter+" = "+v, 80));
            }

            log.debug(Formatter.displaySeparator(80));

            ps.execute();

        } catch (Exception e) {
            log.error(e.getMessage(), e);

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }
    }

    public int getLastChangeNumber() throws Exception {

        StringBuilder sb = new StringBuilder();
        sb.append("select lastChangeNumber from ");
        sb.append(tableName+"_changes");

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

            log.debug("Results: lastChangeNumber = "+value);

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

        String sql = "update "+tableName+"_changes"+" set lastChangeNumber = ?";

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

            log.debug(Formatter.displayLine("Parameters:", 80));

            int counter = 1;
            for (Iterator i=parameters.iterator(); i.hasNext(); counter++) {
                Object v = i.next();
                ps.setObject(counter, v);
                log.debug(Formatter.displayLine(" - "+counter+" = "+v, 80));
            }

            log.debug(Formatter.displaySeparator(80));

            int count = ps.executeUpdate();

            if (count > 0) return;

        } catch (Exception e) {
            //log.error(e.getMessage(), e);

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }

        sql = "insert into "+tableName+"_changes"+" values (?)";

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

    public String createSearchQuery(
            SourceConfig sourceConfig,
            Filter filter,
            Collection parameters)
            throws Exception {

        Collection tables = new TreeSet();
        StringBuilder columns = new StringBuilder();
        StringBuilder whereClause = new StringBuilder();

        tool.convert(tableName, sourceConfig, filter, parameters, whereClause, tables);

        Collection pkFields = sourceConfig.getPrimaryKeyFieldConfigs();

        for (Iterator i=pkFields.iterator(); i.hasNext(); ) {
            FieldConfig fieldConfig = (FieldConfig)i.next();
            String fieldName = fieldConfig.getName();

            if (columns.length() > 0) columns.append(", ");
            columns.append(tableName);
            columns.append(".");
            columns.append(fieldName);
        }

        StringBuilder join = new StringBuilder();
        join.append(tableName);

        for (Iterator i=tables.iterator(); i.hasNext(); ) {
            String t = (String)i.next();

            StringBuilder sb = new StringBuilder();
            for (Iterator j=pkFields.iterator(); j.hasNext(); ) {
                FieldConfig fieldConfig = (FieldConfig)j.next();
                String fieldName = fieldConfig.getName();

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

        StringBuilder sb = new StringBuilder();
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

    public ConnectionManager getConnectionManager() {
        return connectionManager;
    }

    public void setConnectionManager(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    public String getConnectionName() {
        return connectionName;
    }

    public void setConnectionName(String connectionName) {
        this.connectionName = connectionName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
}
