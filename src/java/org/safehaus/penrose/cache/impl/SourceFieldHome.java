/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.cache.impl;

import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.SimpleFilter;
import org.safehaus.penrose.filter.AndFilter;
import org.safehaus.penrose.filter.OrFilter;
import org.safehaus.penrose.mapping.*;
import org.apache.log4j.Logger;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class SourceFieldHome {

    protected Logger log = Logger.getLogger(Penrose.CACHE_LOGGER);

    private DataSource ds;
    private DefaultCache cache;
    public Source source;
    public String tableName;

    public SourceFieldHome(DataSource ds, DefaultCache cache, Source source, String tableName) throws Exception {

        this.ds = ds;
        this.cache = cache;
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

            Collection fields = source.getPrimaryKeyFields();

            for (Iterator i = fields.iterator(); i.hasNext();) {
                Field field = (Field) i.next();
                String name = field.getName();

                if (sb.length() > 0) sb.append(", ");

                sb.append(name);
                sb.append(" varchar(255)");
            }

            sb.append(", name varchar(255)");
            sb.append(", value varchar(255)");

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

        Collection fields = source.getPrimaryKeyFields();

        for (Iterator i = fields.iterator(); i.hasNext();) {
            Field field = (Field) i.next();

            String name = field.getName();

            if (sb.length() > 0) sb.append(", ");
            sb.append(name);
        }

        return sb.toString();
    }

    public String getAttributeNames() {
        StringBuffer sb = new StringBuffer();
        sb.append("name, value");

        return sb.toString();
    }

    public Row getPk(ResultSet rs) throws Exception {
        Row pk = new Row();

        Collection fields = source.getPrimaryKeyFields();

        int c = 0;
        for (Iterator i = fields.iterator(); i.hasNext();) {
            Field field = (Field) i.next();

            String name = field.getName();
            Object value = rs.getObject(++c);

            pk.set(name, value);
        }


        return pk;
    }

    public Row getRow(ResultSet rs) throws Exception {
        Row values = new Row();

        int c = 1;

        String name = rs.getString(c++);
        Object value = rs.getObject(c++);

        values.set(name, value);

        return values;
    }

    public Collection search(Row pk) throws Exception {
        List pks = new ArrayList();
        pks.add(pk);

        return search(pks);
    }

    public String toSQLFilter(Filter filter, boolean includeValues) throws Exception {
        StringBuffer sb = new StringBuffer();
        boolean valid = toSQLFilter(filter, includeValues, sb);

        if (valid && sb.length() > 0) return sb.toString();

        return null;
    }

    public boolean toSQLFilter(
            Filter filter,
            boolean includeValues,
            StringBuffer sb)
            throws Exception {

        if (filter instanceof SimpleFilter) {
            return toSQLFilter((SimpleFilter) filter, includeValues, sb);

        } else if (filter instanceof AndFilter) {
            return toSQLFilter((AndFilter) filter, includeValues, sb);

        } else if (filter instanceof OrFilter) {
            return toSQLFilter((OrFilter) filter, includeValues, sb);
        }

        return true;
    }

    public boolean toSQLFilter(
            SimpleFilter filter,
            boolean includeValues,
            StringBuffer sb)
            throws Exception {

        sb.append("lower(name)=lower(?) and lower(value)=lower(?)");

        return true;
    }

    public boolean toSQLFilter(
            AndFilter filter,
            boolean includeValues,
            StringBuffer sb)
            throws Exception {
        StringBuffer sb2 = new StringBuffer();
        for (Iterator i = filter.getFilterList().iterator(); i.hasNext();) {
            Filter f = (Filter) i.next();

            StringBuffer sb3 = new StringBuffer();
            toSQLFilter(f, includeValues, sb3);

            if (sb2.length() > 0 && sb3.length() > 0) {
                sb2.append(" and ");
            }

            sb2.append(sb3);
        }

        if (sb2.length() == 0)
            return true;

        sb.append("(");
        sb.append(sb2);
        sb.append(")");

        return true;
    }

    public boolean toSQLFilter(
            EntryDefinition entry,
            OrFilter filter,
            boolean includeValues,
            StringBuffer sb)
            throws Exception {
        StringBuffer sb2 = new StringBuffer();
        for (Iterator i = filter.getFilterList().iterator(); i.hasNext();) {
            Filter f = (Filter) i.next();

            StringBuffer sb3 = new StringBuffer();
            toSQLFilter(f, includeValues, sb3);

            if (sb2.length() > 0 && sb3.length() > 0) {
                sb2.append(" or ");
            }

            sb2.append(sb3);
        }

        if (sb2.length() == 0)
            return true;

        sb.append("(");
        sb.append(sb2);
        sb.append(")");

        return true;
    }

    public Collection searchPks(Collection pks) throws Exception {

        if (pks == null || pks.isEmpty()) return new ArrayList();

        String attributeNames = getPkAttributeNames();

        Filter filter = cache.getCacheContext().getFilterTool().createFilter(pks);
        String sqlFilter = toSQLFilter(filter, false);

        String sql = "select " + attributeNames + " from " + tableName
                + " where " + sqlFilter;

        sql += " order by "+attributeNames;

        List results = new ArrayList();

        log.debug("Executing " + sql);

        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            con = ds.getConnection();
            ps = con.prepareStatement(sql);

            int counter = 0;
            for (Iterator i=pks.iterator(); i.hasNext(); ) {
                Row pk = (Row)i.next();

                for (Iterator j=pk.getNames().iterator(); j.hasNext(); ) {
                    String name = (String)j.next();
                    Object value = pk.get(name);

                    ps.setObject(++counter, name);
                    log.debug(" - "+counter+" = "+name);

                    ps.setObject(++counter, value);
                    log.debug(" - "+counter+" = "+value);
                }
            }

            rs = ps.executeQuery();

            while (rs.next()) {
                Row pk = getPk(rs);
                results.add(pk);
            }

        } finally {
            if (rs != null) try { rs.close(); } catch (Exception e) {}
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception ex) {}
        }

        return results;
    }

    public Collection search(Collection pks) throws Exception {

        if (pks == null || pks.isEmpty()) return new ArrayList();

        String attributeNames = getAttributeNames();

        Filter filter = cache.getCacheContext().getFilterTool().createFilter(pks);
        String sqlFilter = ((DefaultCache)cache).getCacheFilterTool().toSQLFilter(filter, false);

        String sql = "select " + attributeNames + " from " + tableName
                + " where " + sqlFilter;

        sql += " order by "+getPkAttributeNames();

        List results = new ArrayList();

        log.debug("Executing " + sql);

        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            con = ds.getConnection();
            ps = con.prepareStatement(sql);

            int counter = 0;
            for (Iterator i=pks.iterator(); i.hasNext(); ) {
                Row pk = (Row)i.next();

                for (Iterator j=pk.getNames().iterator(); j.hasNext(); ) {
                    String name = (String)j.next();
                    Object value = pk.get(name);

                    ps.setObject(++counter, value);
                    log.debug(" - "+counter+" = "+value);
                }
            }

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

    public void insert(Row pk, String name, Object value) throws Exception {

        Connection con = null;
        PreparedStatement ps = null;

        try {
            con = ds.getConnection();
            StringBuffer sb = new StringBuffer();
            StringBuffer sb2 = new StringBuffer();

            Collection fields = source.getPrimaryKeyFields();

            for (Iterator i = fields.iterator(); i.hasNext();) {
                Field field = (Field) i.next();
                String fieldName = field.getName();

                if (sb.length() > 0) {
                    sb.append(", ");
                    sb2.append(", ");
                }

                sb.append(fieldName);
                sb2.append("?");
            }

            sb.append(", name, value");
            sb2.append(", ?, ?");

            String sql = "insert into " + tableName + " (" + sb + ") values (" + sb2 + ")";

            log.debug("Executing " + sql);
            ps = con.prepareStatement(sql);

            int index = 0;

            for (Iterator i = fields.iterator(); i.hasNext(); ) {
                Field field = (Field) i.next();
                String fieldName = field.getName();

                Object attrValue = pk.get(fieldName);
                String string;
                if (attrValue == null) {
                    string = null;
                } else if (attrValue instanceof byte[]) {
                    string = new String((byte[]) attrValue);
                } else if (attrValue instanceof String) {
                    string = (String) attrValue;
                } else {
                    string = attrValue.toString();
                }

                ps.setString(++index, string);
                log.debug("- " + index + " = " + string);
            }

            ps.setString(++index, name);
            log.debug("- " + index + " = " + name);
            ps.setObject(++index, value);
            log.debug("- " + index + " = " + value);

            ps.execute();

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception ex) {}
        }
    }

    public void delete(Row pk) throws Exception {
        Collection pks = new HashSet();
        pks.add(pk);
        delete(pks);
    }

    public void delete(Collection pks) throws Exception {

        if (pks.size() == 0) return;

        Connection con = null;
        PreparedStatement ps = null;

        try {
            con = ds.getConnection();
            StringBuffer sb = new StringBuffer();
            List parameters = new ArrayList();

            Collection fields = source.getPrimaryKeyFields();

            for (Iterator i = pks.iterator(); i.hasNext(); ) {
                Row rdn = (Row)i.next();

                if (sb.length() > 0) sb.append(" or ");

                StringBuffer sb2 = new StringBuffer();
                for (Iterator j = fields.iterator(); j.hasNext();) {
                    Field field = (Field)j.next();
                    String name = field.getName();

                    if (sb2.length() > 0) sb2.append(" and ");
                    sb2.append(name + "=?");

                    Object value = rdn.get(name);

                    if (value instanceof byte[]) {
                        value = new String((byte[]) value);
                    }

                    parameters.add(value);
                }

                sb.append("(");
                sb.append(sb2);
                sb.append(")");
            }

            String sql = "delete from " + tableName + " where " + sb;

            log.debug("Executing " + sql);
            ps = con.prepareStatement(sql);

            int index = 0;

            for (Iterator i = parameters.iterator(); i.hasNext();) {
                Object parameter = i.next();
                ps.setObject(++index, parameter);
                log.debug("- " + index + " = " + parameter);
            }

            ps.execute();

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception ex) {}
        }
    }

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
}
