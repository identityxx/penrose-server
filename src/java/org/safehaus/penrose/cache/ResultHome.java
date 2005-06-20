/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.cache;

import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.mapping.Row;
import org.safehaus.penrose.mapping.EntryDefinition;
import org.safehaus.penrose.mapping.AttributeDefinition;
import org.apache.log4j.Logger;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class ResultHome {

    protected Logger log = Logger.getLogger(Penrose.CACHE_LOGGER);

    public final static String MODIFY_TIME_FIELD = "__modifyTime";

    private DataSource ds;
    public EntryDefinition entry;
    public String tableName;

    public ResultHome(DataSource ds, EntryDefinition entry, String tableName) throws Exception {

        this.ds = ds;
        this.entry = entry;
        this.tableName = tableName;

        try {
            drop();
        } catch (Exception e) {
            // ignore
        }

        create();
    }

    public void create() throws Exception {
        Connection con = null;
        PreparedStatement ps = null;

        try {
            con = ds.getConnection();
            StringBuffer sb = new StringBuffer();

            Set set = new HashSet();
            Map attributes = entry.getAttributes();

            for (Iterator i = attributes.keySet().iterator(); i.hasNext();) {
                String name = (String) i.next();

                // TODO need to handle multiple attribute definitions
                if (set.contains(name)) continue;
                set.add(name);

                if (sb.length() > 0) sb.append(", ");

                sb.append(name);
                sb.append(" varchar(255)");
            }

            sb.append(", "+MODIFY_TIME_FIELD+" datetime");

            String sql = "create table " + tableName + " (" + sb + ")";

            log.debug("Executing " + sql);
            ps = con.prepareStatement(sql);
            ps.execute();

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception ex) {}
        }
    }

    public void drop() throws Exception {

        Connection con = null;
        PreparedStatement ps = null;

        try {
            con = ds.getConnection();
            String sql = "drop table " + tableName;
            log.debug("Executing " + sql);

            ps = con.prepareStatement(sql);
            ps.execute();

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception ex) {}
        }
    }

    public String getPkAttributeNames() {
        StringBuffer sb = new StringBuffer();

        Map attributes = entry.getAttributes();
        Set set = new HashSet();

        for (Iterator j = attributes.values().iterator(); j.hasNext();) {
            AttributeDefinition attribute = (AttributeDefinition) j.next();
            if (!attribute.isRdn()) continue;

            String name = attribute.getName();

            if (set.contains(name)) continue;
            set.add(name);

            if (sb.length() > 0) sb.append(", ");
            sb.append(name);
        }

        return sb.toString();
    }

    public String getAttributeNames() {
        StringBuffer sb = new StringBuffer();

        Map attributes = entry.getAttributes();
        Set set = new HashSet();

        for (Iterator j = attributes.values().iterator(); j.hasNext();) {
            AttributeDefinition attribute = (AttributeDefinition) j.next();
            String name = attribute.getName();

            // TODO need to handle multiple attribute definitions
            if (set.contains(name)) continue;
            set.add(name);

            if (sb.length() > 0) sb.append(", ");
            sb.append(name);
        }

        return sb.toString();
    }

    public Row getRow(ResultSet rs) throws Exception {
        Row values = new Row();

        Set set = new HashSet();

        Map attributes = entry.getAttributes();
        int c = 1;
        for (Iterator j = attributes.values().iterator(); j.hasNext(); c++) {
            AttributeDefinition attribute = (AttributeDefinition) j.next();
            String name = attribute.getName();

            // TODO need to handle multiple attribute definitions
            if (set.contains(name)) continue;
            set.add(name);

            Object value = rs.getObject(c);
            if (value != null) values.set(name, value);
        }

        return values;
    }

    public Collection search(Row pk) throws Exception {
        List pks = new ArrayList();
        pks.add(pk);

        return search(pks);
    }

    public String getFilter(Collection pks) throws Exception {

        if (pks == null || pks.isEmpty()) return null;

        StringBuffer sb = new StringBuffer();

        for (Iterator i = pks.iterator(); i.hasNext();) {
            Row pk = (Row) i.next();

            StringBuffer sb2 = new StringBuffer();
            for (Iterator j = pk.getNames().iterator(); j.hasNext();) {
                String name = (String) j.next();
                String value = (String) pk.get(name);

                if (sb2.length() > 0) sb2.append(" and ");

                sb2.append("lower(");
                sb2.append(name);
                sb2.append(")=lower('");
                sb2.append(value);
                sb2.append("')");
            }

            if (sb.length() > 0) sb.append(" or ");

            sb.append(sb2);
        }

        return sb.toString();
    }

    public Collection search(Collection pks) throws Exception {

        if (pks == null || pks.isEmpty()) return new ArrayList();

        String attributeNames = getAttributeNames();
        String filter = getFilter(pks);

        String sql = "select " + attributeNames + " from " + tableName
                + " where " + filter;

        sql += " order by "+getPkAttributeNames();

        List results = new ArrayList();

        log.debug("Executing " + sql);

        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            con = ds.getConnection();
            ps = con.prepareStatement(sql);
            rs = ps.executeQuery();

            while (rs.next()) {
                Row row = getRow(rs);
                results.add(row);
            }

        } finally {
            if (rs != null) try { rs.close(); } catch (Exception e) {}
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception ex) {}
        }

        return results;
    }

    public void insert(Row row, Date date) throws Exception {

        Connection con = null;
        PreparedStatement ps = null;

        try {
            con = ds.getConnection();
            StringBuffer sb = new StringBuffer();
            StringBuffer sb2 = new StringBuffer();

            Map attributes = entry.getAttributes();
            Set set = new HashSet();

            for (Iterator i = attributes.keySet().iterator(); i.hasNext();) {
                String name = (String) i.next();

                // TODO need to handle multiple attribute definitions
                if (set.contains(name)) continue;
                set.add(name);

                if (sb.length() > 0) {
                    sb.append(", ");
                    sb2.append(", ");
                }
                sb.append(name);
                sb2.append("?");
            }

            sb.append(", ");
            sb.append(MODIFY_TIME_FIELD);
            sb2.append(", ?");

            String sql = "insert into " + tableName + " (" + sb + ") values (" + sb2 + ")";

            log.debug("Executing " + sql);
            ps = con.prepareStatement(sql);

            set = new HashSet();
            int index = 1;
            for (Iterator i = attributes.keySet().iterator(); i.hasNext(); index++) {
                String name = (String) i.next();

                // TODO need to handle multiple attribute definitions
                if (set.contains(name)) continue;
                set.add(name);

                Object value = row.get(name);
                String string;
                if (value == null) {
                    string = null;
                } else if (value instanceof byte[]) {
                    string = new String((byte[]) value);
                } else if (value instanceof String) {
                    string = (String) value;
                } else {
                    string = value.toString();
                }

                ps.setString(index, string);
                //log.debug("- " + index + " = " + string);
            }

            ps.setTimestamp(index, new Timestamp(date.getTime()));

            ps.execute();

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception ex) {}
        }
    }
/*
    public void insertOrUpdateRow(EntryDefinition entry,
            Row row, boolean temporary) throws Exception {

        Connection con = null;
        PreparedStatement ps = null;

        try {
            con = ds.getConnection();
            StringBuffer sb = new StringBuffer();
            StringBuffer sb2 = new StringBuffer();
            StringBuffer sb3 = new StringBuffer();
            StringBuffer sb4 = new StringBuffer();

            Map attributes = entry.getAttributeValues();
            Collection rdnAttributes = entry.getRdnAttributes();
            Set set = new HashSet();

            for (Iterator iter = attributes.keySet().iterator(); iter.hasNext();) {

                String name = (String) iter.next();

                if (set.contains(name))
                    continue;
                set.add(name);

                if (sb.length() > 0) {
                    sb.append(", ");
                    sb2.append(", ");
                    sb4.append(", ");
                }
                sb.append(name);
                sb2.append("?");
                sb4.append(name + "=?");
            }

            set = new HashSet();
            for (Iterator iter = rdnAttributes.iterator(); iter.hasNext();) {

                AttributeDefinition attr = (AttributeDefinition) iter.next();
                String name = attr.getName();
                if (set.contains(name))
                    continue;
                set.add(name);

                if (sb3.length() > 0) {
                    sb3.append(" and ");
                }
                sb3.append(name + "=?");
            }

            String selectSql = "select * from " + tableName + " where " + sb3;
            String updateSql = "update " + tableName + " set " + sb4
                    + " where " + sb3;
            String insertSql = "insert into " + tableName + " (" + sb
                    + ","+MODIFY_TIME_FIELD+") values (" + sb2 + ",'1')";

            // Check if such the row already exist
            log
                    .debug("********************************************************");
            log.debug("Checking if row already exist");
            log.debug("Executing " + selectSql);
            ps = con.prepareStatement(selectSql);

            set = new HashSet();
            int i = 1;
            for (Iterator iter = rdnAttributes.iterator(); iter.hasNext(); i++) {

                AttributeDefinition attr = (AttributeDefinition) iter.next();
                String name = attr.getName();

                if (set.contains(name))
                    continue;
                set.add(name);

                Object value = row.get(name);
                String string;
                if (value instanceof byte[]) {
                    string = new String((byte[]) value);
                } else {
                    string = (String) value;
                }

                ps.setString(i, string);
            }

            ResultSet rs = ps.executeQuery();

            if (rs.next()) {
                // has result, means the row already exists
                // So, we update the row
                PreparedStatement ps2 = null;
                try {
                    log.debug("********************************************************");
                    log.debug("Update the row");
                    log.debug("Executing " + updateSql);
                    ps2 = con.prepareStatement(updateSql);
                    Set set2 = new HashSet();
                    int j = 1;
                    for (Iterator iter = attributes.keySet().iterator(); iter
                            .hasNext(); j++) {

                        String name = (String) iter.next();

                        Object value = row.get(name);
                        String string;
                        if (value instanceof byte[]) {
                            string = new String((byte[]) value);
                        } else {
                            string = (String) value;
                        }

                        ps2.setString(j, string);
                        log.debug("- " + j + " = " + string);
                    }

                    for (Iterator iter = rdnAttributes.iterator(); iter
                            .hasNext(); j++) {

                        AttributeDefinition attr = (AttributeDefinition) iter.next();
                        String name = attr.getName();

                        Object value = row.get(name);
                        String string;
                        if (value instanceof byte[]) {
                            string = new String((byte[]) value);
                        } else {
                            string = (String) value;
                        }

                        ps2.setString(j, string);
                        log.debug("- " + j + " = " + string);
                    }

                    ps2.execute();

                } catch (Exception ex) {
                    log.error(ex.getMessage(), ex);
                }

            } else {
                // no result, means the row has not already exist
                // So, we insert the row
                PreparedStatement ps2 = null;
                try {
                    log.debug("********************************************************");
                    log.debug("Insert the row");
                    log.debug("Executing " + insertSql);
                    ps2 = con.prepareStatement(insertSql);
                    int j = 1;
                    for (Iterator iter = attributes.keySet().iterator(); iter
                            .hasNext(); j++) {

                        String name = (String) iter.next();

                        Object value = row.get(name);
                        String string;
                        if (value instanceof byte[]) {
                            string = new String((byte[]) value);
                        } else {
                            string = (String) value;
                        }

                        ps2.setString(j, string);
                        log.debug("- " + j + " = " + string);
                    }

                    ps2.execute();

                } catch (Exception ex) {
                    log.error(ex.getMessage(), ex);
                }
            }

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception ex) {}
        }
    }
*/
    public void delete(Row row, Date date) throws Exception {

        Connection con = null;
        PreparedStatement ps = null;

        try {
            con = ds.getConnection();
            StringBuffer sb = new StringBuffer();

            Collection attributes = entry.getRdnAttributes();
            Set set = new HashSet();

            for (Iterator i = attributes.iterator(); i.hasNext();) {
                AttributeDefinition attribute = (AttributeDefinition)i.next();
                String name = attribute.getName();

                // TODO need to handle multiple attribute definitions
                if (set.contains(name)) continue;
                set.add(name);


                if (sb.length() > 0) sb.append(" and ");

                sb.append(name + "=?");
            }

            String sql = "delete from " + tableName + " where " + sb;

            log.debug("Executing " + sql);
            ps = con.prepareStatement(sql);

            set = new HashSet();
            int index = 1;

            for (Iterator i = attributes.iterator(); i.hasNext();) {
                AttributeDefinition attribute = (AttributeDefinition)i.next();
                String name = attribute.getName();

                if (set.contains(name)) continue;
                set.add(name);

                Object value = row.get(name);
                String string;

                if (value instanceof byte[]) {
                    string = new String((byte[]) value);
                } else {
                    string = (String)value;
                }

                ps.setString(index, string);
                log.debug("- " + index + " = " + string);
            }

            ps.execute();

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception ex) {}
        }
    }
/*
    public void setValidity(Row row, boolean validity) throws Exception {

        Connection con = null;
        PreparedStatement ps = null;

        try {
            con = ds.getConnection();
            StringBuffer sb = new StringBuffer();

            Map attributes = entry.getAttributeValues();
            Set set = new HashSet();

            for (Iterator i = attributes.keySet().iterator(); i.hasNext();) {

                String name = (String) i.next();

                // TODO need to handle multiple attribute definitions
                if (set.contains(name)) continue;
                set.add(name);

                if (sb.length() > 0) sb.append(" and ");

                sb.append(name + "=?");
            }

            String sql = "update " + tableName + " set "+MODIFY_TIME_FIELD+"='"+(validity?1:0)+"' where " + sb;

            log.debug("Executing " + sql);
            ps = con.prepareStatement(sql);

            set = new HashSet();
            int index = 1;
            for (Iterator i = attributes.keySet().iterator(); i.hasNext();) {

                String name = (String) i.next();

                // TODO need to handle multiple attribute definitions
                if (set.contains(name)) continue;
                set.add(name);

                Object value = row.get(name);
                String string;
                if (value instanceof byte[]) {
                    string = new String((byte[]) value);
                } else {
                    string = (String) value;
                }

                ps.setString(index, string);
                log.debug("- " + index + " = " + string);
                index++;
            }

            ps.execute();

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception ex) {}
        }
    }
*/
    public void delete(String filter, Date date) throws Exception {

        Connection con = null;
        PreparedStatement ps = null;

        try {
            con = ds.getConnection();
            String sql = "delete from " + tableName;

            if (filter != null) {
                sql += " where "+filter;
            }

            log.debug("Executing " + sql);
            ps = con.prepareStatement(sql);
            ps.execute();

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception ex) {}
        }
    }

    public void copy(ResultHome resultHome, String filter) throws Exception {

        String sql = "insert into " + tableName + " select * from " + resultHome.tableName;

        if (filter != null) {
            sql += " where "+filter;
        }

        log.debug("Executing " + sql);

        Connection con = null;
        PreparedStatement ps = null;

        try {
            con = ds.getConnection();
            ps = con.prepareStatement(sql);
            ps.execute();
        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception ex) {}
        }
    }

    public Date getModifyTime(
            Collection pks)
            throws Exception {

        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            con = ds.getConnection();
            String sql = "select "+MODIFY_TIME_FIELD+" from "+tableName;

            String filter = getFilter(pks);
            if (filter != null) {
                sql += " where "+filter;
            }

            sql += " order by "+MODIFY_TIME_FIELD;

            log.debug("Executing " + sql);

            ps = con.prepareStatement(sql);
            rs = ps.executeQuery();

            if (!rs.next()) return null;

            return new Date(rs.getTimestamp(1).getTime());

        } finally {
            if (rs != null) try { rs.close(); } catch (Exception e) {}
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception ex) {}
        }
    }
}
