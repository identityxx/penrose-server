/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.cache;

import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.config.Config;
import org.safehaus.penrose.mapping.Row;
import org.safehaus.penrose.mapping.AttributeDefinition;
import org.safehaus.penrose.mapping.EntryDefinition;
import org.safehaus.penrose.mapping.*;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.commons.dbcp.ConnectionFactory;
import org.apache.commons.dbcp.DriverManagerConnectionFactory;
import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.dbcp.PoolingDataSource;
import org.apache.log4j.Logger;

import javax.sql.DataSource;
import java.util.*;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Connection;

/**
 * @author Endi S. Dewata
 */
public class DefaultEntryCache implements EntryCache {

    public Logger log = Logger.getLogger(Penrose.CACHE_LOGGER);

    private EntryCacheConfig cacheConfig;
    public EntryCacheContext cacheContext;
    public Config config;

    public EntryCacheFilterTool cacheFilterTool;

    public PenroseResultHome resultExpirationHome;
    public Map homes = new HashMap();

    private DataSource ds;

    public EntryCacheConfig getCacheConfig() {
        return cacheConfig;
    }

    public void setCacheConfig(EntryCacheConfig cacheConfig) {
        this.cacheConfig = cacheConfig;
    }

    public Collection getParameterNames() {
        return cacheConfig.getParameterNames();
    }

    public String getParameter(String name) {
        return cacheConfig.getParameter(name);
    }

    public void init(EntryCacheConfig cacheConfig, EntryCacheContext cacheContext) throws Exception {
        this.cacheConfig = cacheConfig;
        this.cacheContext = cacheContext;
        this.config = this.cacheContext.getConfig();

        cacheFilterTool = new EntryCacheFilterTool(cacheContext);

        init();
    }

    public void init() throws Exception {

        String driver    = getParameter(SourceCacheConfig.DRIVER);
        String url       = getParameter(SourceCacheConfig.URL);
        String username  = getParameter(SourceCacheConfig.USER);
        String password  = getParameter(SourceCacheConfig.PASSWORD);

        log.debug("Driver    : " + driver);
        log.debug("Url       : " + url);
        log.debug("Username  : " + username);
        log.debug("Password  : " + password);

        Properties properties = new Properties();
        properties.put("driver", driver);
        properties.put("url", url);
        properties.put("user", username);
        properties.put("password", password);

        Class.forName(driver);

        GenericObjectPool connectionPool = new GenericObjectPool(null);
        //connectionPool.setMaxActive(0);

        ConnectionFactory connectionFactory = new DriverManagerConnectionFactory(
                url, properties);

        PoolableConnectionFactory poolableConnectionFactory = new PoolableConnectionFactory(
                connectionFactory, connectionPool, null, // statement pool
                // factory
                null, // test query
                false, // read only
                true // auto commit
        );

        ds = new PoolingDataSource(connectionPool);

        Collection entries = config.getEntryDefinitions();

        resultExpirationHome = new PenroseResultHome(ds);

        for (Iterator i=entries.iterator(); i.hasNext(); ) {
            EntryDefinition entryDefinition = (EntryDefinition)i.next();

            createTables(entryDefinition);
        }
    }

    public String getEntryTableName(EntryDefinition entry) {
        String dn = entry.getDn();
		dn = dn.replace('=', '_');
		dn = dn.replace(',', '_');
		dn = dn.replace(' ', '_');
		dn = dn.replace('.', '_');
		return dn;
	}

    public String getEntryAttributeTableName(EntryDefinition entry) {
        return getEntryTableName(entry)+"_attributes";
	}

    public void createTables(EntryDefinition entryDefinition) throws Exception {
        resultExpirationHome.insert(entryDefinition);

        String entryTableName = getEntryTableName(entryDefinition);
        EntryHome entryHome = new EntryHome(ds, entryDefinition, entryTableName);
        homes.put(entryTableName, entryHome);

        String entryAttributeTableName = getEntryAttributeTableName(entryDefinition);
        EntryAttributeHome entryAttributeHome = new EntryAttributeHome(ds, entryDefinition, entryAttributeTableName);
        homes.put(entryAttributeTableName, entryAttributeHome);
    }

    public EntryHome getEntryHome(EntryDefinition entryDefinition) throws Exception {
        String tableName = getEntryTableName(entryDefinition);
        return (EntryHome)homes.get(tableName);
    }

    public EntryAttributeHome getEntryAttributeHome(EntryDefinition entryDefinition) throws Exception {
        String tableName = getEntryAttributeTableName(entryDefinition);
        return (EntryAttributeHome)homes.get(tableName);
    }

    public Entry get(EntryDefinition entryDefinition, Row pk) throws Exception {

        EntryAttributeHome entryAttributeHome = getEntryAttributeHome(entryDefinition);
        System.out.println("Searching attributes for pk: "+pk);

        Collection rows = entryAttributeHome.search(pk);

        AttributeValues attributeValues = new AttributeValues();
        for (Iterator i = rows.iterator(); i.hasNext();) {
            Row row = (Row)i.next();

            String name = (String)row.getNames().iterator().next();
            Object value = row.get(name);

            log.debug(" - "+name+": "+value);

            Collection values = (Collection)attributeValues.get(name);
            if (values == null) {
                values = new ArrayList();
                attributeValues.set(name, values);
            }
            values.add(value);
        }

        Entry sr = new Entry(entryDefinition, attributeValues);
        return sr;
    }

    public Map get(EntryDefinition entryDefinition, Collection primaryKeys) throws Exception {

        Map entries = new HashMap();

        for (Iterator i=primaryKeys.iterator(); i.hasNext(); ) {
            Row pk = (Row)i.next();
            Entry entry = get(entryDefinition, pk);
            entries.put(pk, entry);
        }

        /*
        EntryHome entryHome = getEntryHome(entryDefinition);
        Collection rows = entryHome.search(primaryKeys);

        log.debug("Merging " + rows.size() + " rows:");
        Map map = cacheContext.getTransformEngine().merge(entryDefinition, rows);

        for (Iterator i = map.keySet().iterator(); i.hasNext();) {
            Map pk = (Map)i.next();
            AttributeValues values = (AttributeValues)map.get(pk);
            log.debug(" - " + values);

            Entry sr = new Entry(entryDefinition, values);
            entries.put(pk, sr);
        }
        */

        return entries;
    }

    public Row getPk(EntryDefinition entry, AttributeValues attributeValues) throws Exception {
        Row pk = new Row();
        Collection rdnAttributes = entry.getRdnAttributes();

        for (Iterator i = rdnAttributes.iterator(); i.hasNext();) {
            AttributeDefinition attributeDefinition = (AttributeDefinition) i.next();
            String name = attributeDefinition.getName();
            Collection values = attributeValues.get(name);

            // TODO need to handle multiple values
            Object value = values.iterator().next();
            pk.set(name, value);
        }

        return pk;
    }

    public void put(EntryDefinition entryDefinition, AttributeValues values, Date date) throws Exception {
        Collection rows = cacheContext.getTransformEngine().convert(values);

        EntryHome entryHome = getEntryHome(entryDefinition);

        for (Iterator i=rows.iterator(); i.hasNext(); ) {
            Row row = (Row)i.next();
            entryHome.insert(row, date);
        }

        EntryAttributeHome entryAttributeHome = getEntryAttributeHome(entryDefinition);
        Row pk = getPk(entryDefinition, values);
        System.out.println("Inserting pk: "+pk);

        for (Iterator i=values.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Collection c = values.get(name);
            if (c == null) continue;

            for (Iterator j=c.iterator(); j.hasNext(); ) {
                Object value = j.next();
                if (value == null) continue;
                entryAttributeHome.insert(pk, name, value, date);
            }
        }
    }

    public void remove(EntryDefinition entryDefinition, AttributeValues values, Date date) throws Exception {
        Collection rows = cacheContext.getTransformEngine().convert(values);

        EntryHome entryHome = getEntryHome(entryDefinition);

        for (Iterator i=rows.iterator(); i.hasNext(); ) {
            Row row = (Row)i.next();
            entryHome.delete(row, date);
        }

        EntryAttributeHome entryAttributeHome = getEntryAttributeHome(entryDefinition);
        Row pk = getPk(entryDefinition, values);
        System.out.println("Deleting pk: "+pk);

        entryAttributeHome.delete(pk, date);
    }

    public Date getModifyTime(EntryDefinition entryDefinition, Row pk) throws Exception {
        List pks = new ArrayList();
        pks.add(pk);

        return getModifyTime(entryDefinition, pks);

        //return resultExpirationHome.getModifyTime(entry);
    }

    public Date getModifyTime(EntryDefinition entryDefinition, Collection pks) throws Exception {
        EntryHome entryHome = getEntryHome(entryDefinition);
        return entryHome.getModifyTime(pks);
    }

    /**
     * Get the primary key attribute names
     *
     * @param entry
     *            the entry (from config)
     * @return the string "pk1, pk2, ..."
     */
    public String getPkAttributeNames(EntryDefinition entry) {
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

    /**
     * @param entry
     * @param filter
     * @return primary keys
     * @throws Exception
     */
    public Collection findPrimaryKeys(
            EntryDefinition entry,
            Filter filter)
            throws Exception {

        String sqlFilter = cacheFilterTool.toSQLFilter(entry, filter);

        String tableName = getEntryTableName(entry);
        String attributeNames = getPkAttributeNames(entry);

        String sql = "select distinct " + attributeNames + " from " + tableName;

        if (sqlFilter != null) {
            sql += " where " + sqlFilter;
        }

        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        List pks = new ArrayList();

        log.debug("Executing " + sql);
        try {
            con = ds.getConnection();
            ps = con.prepareStatement(sql);
            rs = ps.executeQuery();

            //log.debug("Result:");

            while (rs.next()) {

                Row pk = getPk(entry, rs);
                //log.debug(" - "+row);

                pks.add(pk);
            }

        } finally {
            if (rs != null) try { rs.close(); } catch (Exception e) {}
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception ex) {}
        }

        return pks;
    }

    /**
     * Get the primary key from a given entry and result set.
     *
     * @param entry the entry (from config)
     * @param rs the result set
     * @return a Map containing primary keys and its values
     * @throws Exception
     */
    public Row getPk(EntryDefinition entry, ResultSet rs) throws Exception {
        Row values = new Row();

        ResultSetMetaData rsmd = rs.getMetaData();
        int count = rsmd.getColumnCount();

        int c = 1;
        Collection attributes = entry.getRdnAttributes();

        Set set = new HashSet();
        for (Iterator j = attributes.iterator(); j.hasNext() && c <= count;) {
            AttributeDefinition attribute = (AttributeDefinition) j.next();
            String name = attribute.getName();

            if (set.contains(name)) continue;
            set.add(name);

            Object value = rs.getObject(c);

            values.set(name, value);

            c++;
        }

        return values;
    }

    public EntryCacheFilterTool getCacheFilterTool() {
        return cacheFilterTool;
    }

    public void setCacheFilterTool(EntryCacheFilterTool cacheFilterTool) {
        this.cacheFilterTool = cacheFilterTool;
    }

    /**
     * Join sources
     *
     * @param con
     *            the JDBC connection
     * @param entry
     *            the entry (from config)
     * @param temporary
     *            whether we are using temporary tables
     * @param sourceConfig
     *            the source
     * @return the Collection of rows resulting from the join
     * @throws Exception
     */
/*
    public Collection joinSourcesIncrementally(java.sql.Connection con,
            EntryDefinition entry, boolean temporary, SourceDefinition sourceConfig, Map incRow)
            throws Exception {

        String sqlFieldNames = getFieldNames(entry.getSources());
        // don't use temporary entry tables here
        String sqlTableNames = getTableNames(entry, false);

        String whereClause = "1=1";

        String sql = "select " + sqlFieldNames + " from " + sqlTableNames + " where " + whereClause;

        // Add pk clause
        Collection pkFields = sourceConfig.getPrimaryKeyFields();
        boolean started = false;
        for (Iterator pkIter = pkFields.iterator(); pkIter.hasNext();) {
            FieldDefinition pkField = (FieldDefinition) pkIter.next();
            String pk = pkField.getName();
            sql += (started ? " and " : " where ") + sourceConfig.getName() + "."
                    + pk + " = ?";
            started = true;
        }
        List results = new ArrayList();

        log.debug("Executing " + sql);

        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = con.prepareStatement(sql);
            Iterator pkIter = pkFields.iterator();
            for (int i = 1; pkIter.hasNext(); i++) {
                FieldDefinition pkField = (FieldDefinition) pkIter.next();
                String pk = pkField.getName();
                Object obj = incRow.get(pk);
                if (obj instanceof Collection && ((Collection) obj).size() == 1) {
                    Collection coll = (Collection) obj;
                    Iterator iter = coll.iterator();
                    Object obj1 = iter.next();
                    ps.setObject(i, obj1);
                    log.debug(" - " + i + " = " + obj1.toString() + " (class="
                            + obj1.getClass().getName() + ")");
                    log.debug("   " + i + " = " + obj.toString() + " (class="
                            + obj.getClass().getName() + ")");
                } else {
                    ps.setObject(i, obj);
                    log.debug(" - " + i + " = " + obj.toString() + " (class="
                            + obj.getClass().getName() + ")");
                }
            }
            rs = ps.executeQuery();

            while (rs.next()) {
                Map row = getRow(entry, rs);

                results.add(row);
            }

        } finally {
            if (rs != null) try { rs.close(); } catch (Exception e) {}
            if (ps != null) try { ps.close(); } catch (Exception e) {}
        }

        return results;
    }
*/
}
