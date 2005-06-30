/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.cache.impl;

import org.safehaus.penrose.mapping.Row;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.mapping.*;
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
public class SourceHome {

    protected Logger log = Logger.getLogger(Penrose.CACHE_LOGGER);

    public final static String MODIFY_TIME_FIELD = "__modifyTime";

    private DataSource ds;
    public Source source;
    public String tableName;

    public SourceHome(DataSource ds, Source source, String tableName) throws Exception {

        this.ds = ds;
        this.source = source;
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
            Collection fields = source.getFields();

            for (Iterator i = fields.iterator(); i.hasNext();) {
                Field field = (Field) i.next();

                // TODO need to handle multiple field definitions
                if (set.contains(field.getName())) continue;
                set.add(field.getName());

                if (sb.length() > 0) sb.append(", ");
                sb.append(field.getName());
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

    public void insert(Row row, Date date) throws Exception {

        Connection con = null;
        PreparedStatement ps = null;

        try {
            con = ds.getConnection();

            StringBuffer sb = new StringBuffer();
            StringBuffer sb2 = new StringBuffer();

            Collection fields = source.getFields();
            Set set = new HashSet();

            for (Iterator i = fields.iterator(); i.hasNext();) {
                Field field = (Field) i.next();
                String name = field.getName();

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
            for (Iterator i = fields.iterator(); i.hasNext(); index++) {
                Field field = (Field) i.next();
                String name = field.getName();

                if (set.contains(field.getName()))
                    continue;
                set.add(field.getName());

                Object value = row.get(name);
                //log.debug(" - " + index + ": " + value + " ("
                //        + (value == null ? null : value.getClass().getName())
                //        + ")");

                String string;

                if (value == null) {
                    string = null;

                } else if (value instanceof byte[]) {
                    string = new String((byte[]) value);

                } else if (value instanceof Set) {
                    Set s = (Set) value;
                    string = s.isEmpty() ? null : (String) s.iterator().next();

                } else {
                    string = value.toString();
                }

                ps.setString(index, string);
            }

            ps.setTimestamp(index, new Timestamp(date.getTime()));

            ps.execute();

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception ex) {}
        }
    }

    public void update(Row oldRow, Row newRow, Date date) throws Exception {

        Connection con = null;
        PreparedStatement ps = null;

        try {
            con = ds.getConnection();

            Collection fields = source.getFields();

            StringBuffer sb = new StringBuffer();
            Set set = new HashSet();

            for (Iterator i = fields.iterator(); i.hasNext();) {
                Field field = (Field) i.next();
                String name = field.getName();

                if (set.contains(name)) continue;
                set.add(name);

                if (sb.length() > 0) {
                    sb.append(", ");
                }
                sb.append(name);
                sb.append("=?");
            }

            sb.append(", ");
            sb.append(MODIFY_TIME_FIELD);
            sb.append("=?");

            StringBuffer sb2 = new StringBuffer();
            set = new HashSet();

            for (Iterator i = fields.iterator(); i.hasNext();) {
                Field field = (Field) i.next();
                if (!field.isPrimaryKey()) continue;

                String name = field.getName();
                if (set.contains(name)) continue;
                set.add(name);

                if (sb2.length() > 0) {
                    sb2.append(" and ");
                }
                sb2.append(name);
                sb2.append("=?");
            }

            String sql = "update " + tableName + " set " + sb + " where " + sb2;

            log.debug("Executing " + sql);
            ps = con.prepareStatement(sql);

            set = new HashSet();
            int index = 1;
            for (Iterator i = fields.iterator(); i.hasNext();) {
                Field field = (Field) i.next();
                String name = field.getName();

                if (set.contains(field.getName())) continue;
                set.add(field.getName());

                Object value = newRow.get(name);
                String string;

                if (value instanceof byte[]) {
                    string = new String((byte[]) value);

                } else if (value instanceof Set) {
                    Set s = (Set) value;
                    string = s.isEmpty() ? null : (String) s.iterator().next();

                } else {
                    string = (String) value;
                }

                ps.setString(index++, string);
            }

            ps.setTimestamp(index++, new Timestamp(date.getTime()));

            set = new HashSet();
            for (Iterator i = fields.iterator(); i.hasNext();) {
                Field field = (Field) i.next();
                if (!field.isPrimaryKey()) continue;

                String name = field.getName();
                if (set.contains(name)) continue;
                set.add(name);

                Object value = oldRow.get(name);
                String string;

                if (value instanceof byte[]) {
                    string = new String((byte[]) value);

                } else if (value instanceof Set) {
                    Set s = (Set) value;
                    string = s.isEmpty() ? null : (String) s.iterator().next();

                } else {
                    string = (String) value;
                }

                ps.setString(index++, string);
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

            Collection fields = sourceConfig.getFields();
            Set set = new HashSet();

            for (Iterator i = fields.iterator(); i.hasNext();) {
                FieldDefinition field = (FieldDefinition) i.next();
                if (!field.isPrimaryKey())
                    continue;

                String name = field.getName();
                if (set.contains(name))
                    continue;
                set.add(name);

                if (sb.length() > 0) {
                    sb.append(" and ");
                }
                sb.append(name);
                sb.append("=?");
            }

            String sql = "update " + tableName + " set "+MODIFY_TIME_FIELD+"='"+(validity?1:0)+"' where " + sb;

            log.debug("Executing " + sql);
            ps = con.prepareStatement(sql);

            set = new HashSet();
            int index = 1;
            for (Iterator i = fields.iterator(); i.hasNext();) {
                FieldDefinition field = (FieldDefinition) i.next();
                if (!field.isPrimaryKey())
                    continue;

                String name = field.getName();
                if (set.contains(name))
                    continue;
                set.add(name);

                Object value = row.get(name);
                String string;

                if (value instanceof byte[]) {
                    string = new String((byte[]) value);

                } else if (value instanceof Set) {
                    Set s = (Set) value;
                    string = s.isEmpty() ? null : (String) s.iterator().next();

                } else {
                    string = (String) value;
                }

                ps.setString(index++, string);
            }

            ps.execute();

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

            Collection fields = source.getFields();
            Set set = new HashSet();

            for (Iterator i = fields.iterator(); i.hasNext();) {
                Field field = (Field) i.next();
                if (!field.isPrimaryKey()) continue;

                String name = field.getName();
                if (set.contains(name)) continue;
                set.add(name);

                if (sb.length() > 0) {
                    sb.append(" and ");
                }
                sb.append(name);
                sb.append("=?");
            }

            String sql = "delete from " + tableName + " where " + sb;

            log.debug("Executing " + sql);
            ps = con.prepareStatement(sql);

            set = new HashSet();
            int index = 1;
            for (Iterator i = fields.iterator(); i.hasNext();) {
                Field field = (Field) i.next();
                if (!field.isPrimaryKey()) continue;

                String name = field.getName();
                if (set.contains(name)) continue;
                set.add(name);

                Object value = row.get(name);
                String string;

                if (value instanceof byte[]) {
                    string = new String((byte[]) value);

                } else if (value instanceof Set) {
                    Set s = (Set) value;
                    string = s.isEmpty() ? null : (String) s.iterator().next();

                } else {
                    string = (String) value;
                }

                ps.setString(index++, string);
            }

            ps.execute();

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception ex) {}
        }
    }

    public void delete(
            String filter,
            Date date)
            throws Exception {

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

    public Date getModifyTime(
            String filter)
            throws Exception {

        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            con = ds.getConnection();
            String sql = "select "+MODIFY_TIME_FIELD+" from "+tableName;

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

    public void copy(SourceHome sourceHome, String filter) throws Exception {

        String sql = "insert into " + tableName + " select * from " + sourceHome.tableName+" "+sourceHome.source.getName();

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
/*
    public void findDirtyRows(Row row) throws Exception {

        Connection con = null;
        PreparedStatement ps = null;

        try {
            con = ds.getConnection();
            Collection fields = sourceConfig.getFields();

            String sql = "select * from " + tableName + " where " + MODIFY_TIME_FIELD + "='0'";

            log.debug("Executing " + sql);
            ps = con.prepareStatement(sql);

            Set set = new HashSet();
            int index = 1;
            for (Iterator i = fields.iterator(); i.hasNext();) {
                Field field = (Field) i.next();
                FieldDefinition fieldConfig = sourceConfig.getFieldDefinition(field.getName());
                if (!fieldConfig.isPrimaryKey())
                    continue;

                String name = field.getName();
                if (set.contains(name)) continue;
                set.add(name);

                Object value = row.get(name);
                String string;

                if (value instanceof byte[]) {
                    string = new String((byte[]) value);

                } else if (value instanceof Set) {
                    Set s = (Set) value;
                    string = s.isEmpty() ? null : (String) s.iterator().next();

                } else {
                    string = (String) value;
                }

                ps.setString(index++, string);
            }

            ps.execute();

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception ex) {}
        }
    }
*/

}
