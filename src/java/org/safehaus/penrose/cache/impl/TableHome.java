/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.cache.impl;

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
public class TableHome {

    protected Logger log = Logger.getLogger(Penrose.CACHE_LOGGER);

    private DataSource ds;
    public EntryDefinition entry;
    public String tableName = "tables";

    public TableHome(DataSource ds, EntryDefinition entry) throws Exception {

        this.ds = ds;
        this.entry = entry;

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
            Collection rdnAttributes = entry.getRdnAttributes();

            for (Iterator i = rdnAttributes.iterator(); i.hasNext();) {
                AttributeDefinition attribute = (AttributeDefinition) i.next();
                String name = attribute.getName();

                // TODO need to handle multiple attribute definitions
                if (set.contains(name)) continue;
                set.add(name);

                if (sb.length() > 0) sb.append(", ");

                sb.append(name);
                sb.append(" varchar(255)");
            }

            sb.append("dn varchar(255)");
            sb.append(", tableName varchar(255)");

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

        Collection rdnAttributes = entry.getRdnAttributes();
        Set set = new HashSet();

        for (Iterator j = rdnAttributes.iterator(); j.hasNext();) {
            AttributeDefinition attribute = (AttributeDefinition) j.next();
            String name = attribute.getName();

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

        Collection rdnAttributes = entry.getRdnAttributes();
        int c = 0;
        for (Iterator j = rdnAttributes.iterator(); j.hasNext();) {
            AttributeDefinition attribute = (AttributeDefinition) j.next();
            String name = attribute.getName();

            // TODO need to handle multiple attribute definitions
            if (set.contains(name)) continue;
            set.add(name);

            Object value = rs.getObject(++c);
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

        String attributeNames = getPkAttributeNames();
        String filter = getFilter(pks);

        String sql = "select " + attributeNames + " from " + tableName
                + " where " + filter;

        sql += " order by "+attributeNames;

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

            Collection rdnAttributes = entry.getRdnAttributes();
            Set set = new HashSet();

            for (Iterator i = rdnAttributes.iterator(); i.hasNext();) {
                AttributeDefinition attribute = (AttributeDefinition) i.next();
                String name = attribute.getName();

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

            String sql = "insert into " + tableName + " (" + sb + ") values (" + sb2 + ")";

            log.debug("Executing " + sql);
            ps = con.prepareStatement(sql);

            set = new HashSet();
            int index = 0;
            for (Iterator i = rdnAttributes.iterator(); i.hasNext(); ) {
                AttributeDefinition attribute = (AttributeDefinition) i.next();
                String name = attribute.getName();

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

                ps.setString(++index, string);
                log.debug("- " + index + " = " + string);
            }

            ps.setTimestamp(++index, new Timestamp(date.getTime()));

            ps.execute();

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception ex) {}
        }
    }

   public void delete(Row row, Date date) throws Exception {

        Connection con = null;
        PreparedStatement ps = null;

        try {
            con = ds.getConnection();
            StringBuffer sb = new StringBuffer();

            Collection rdnAttributes = entry.getRdnAttributes();
            Set set = new HashSet();

            for (Iterator i = rdnAttributes.iterator(); i.hasNext();) {
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
            int index = 0;

            for (Iterator i = rdnAttributes.iterator(); i.hasNext();) {
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

                ps.setString(++index, string);
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

    public String getTableName(String dn)
            throws Exception {

        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            con = ds.getConnection();
            String sql = "select tableName from "+tableName+" where dn=?";

            log.debug("Executing " + sql);

            ps = con.prepareStatement(sql);
            ps.setObject(1, dn);

            rs = ps.executeQuery();

            if (!rs.next()) return null;

            return rs.getString(1);

        } finally {
            if (rs != null) try { rs.close(); } catch (Exception e) {}
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception ex) {}
        }
    }
}
