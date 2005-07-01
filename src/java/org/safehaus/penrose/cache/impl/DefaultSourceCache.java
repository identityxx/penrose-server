/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.cache.impl;

import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.event.CacheEvent;
import org.safehaus.penrose.SearchResults;
import org.safehaus.penrose.graph.Graph;
import org.safehaus.penrose.cache.SourceCache;
import org.safehaus.penrose.cache.CacheConfig;
import org.safehaus.penrose.mapping.Row;
import org.safehaus.penrose.mapping.SourceDefinition;
import org.safehaus.penrose.mapping.EntryDefinition;
import org.safehaus.penrose.mapping.Source;
import org.safehaus.penrose.mapping.*;

import javax.sql.DataSource;
import java.util.*;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Connection;

/**
 * @author Endi S. Dewata
 */
public class DefaultSourceCache extends SourceCache {

    public DefaultCache cache;

    public PenroseSourceHome sourceExpirationHome;
    public Map homes = new HashMap();

    private DataSource ds;

    public void init() throws Exception {

        cache = (DefaultCache)super.getCache();
        ds = cache.getDs();

        Date date = new Date();

        Collection entries = getConfig().getEntryDefinitions();
        Set set = new HashSet();

        sourceExpirationHome = new PenroseSourceHome(ds);

        for (Iterator i=entries.iterator(); i.hasNext(); ) {
            EntryDefinition entry = (EntryDefinition)i.next();

            Collection sources = entry.getSources();

            for (Iterator j=sources.iterator(); j.hasNext(); ) {
                Source source = (Source)j.next();

                // if source has been initialized => skip
                if (set.contains(source.getSourceName())) continue;
                set.add(source.getSourceName());

                // create source cache tables
                sourceExpirationHome.insert(source);

                createTables(source);

                // check global loading parameter
                String s = getCache().getParameter(CacheConfig.LOAD_ON_STARTUP);
                boolean globalLoadOnStartup = s == null ? false : new Boolean(s).booleanValue();
                log.debug("Global load on startup: "+globalLoadOnStartup);

                // check source loading parameter
                s = source.getParameter(SourceDefinition.LOAD_ON_STARTUP);
                log.debug(source.getSourceName()+"'s load on startup: "+s);

                // if no need to load => skip
                boolean loadOnStartup = s == null ? globalLoadOnStartup : new Boolean(s).booleanValue();
                if (!loadOnStartup) continue;

                // load the source
                load(source, null, date);

                // compute source cache expiration
                s = source.getParameter(SourceDefinition.CACHE_EXPIRATION);
                int cacheExpiration = s == null ? 0 : Integer.parseInt(s);
                if (cacheExpiration < 0) cacheExpiration = Integer.MAX_VALUE;

                Calendar c = Calendar.getInstance();
                c.add(Calendar.MINUTE, cacheExpiration);

                setExpiration(source, cacheExpiration == 0 ? null : c.getTime());
            }
        }
    }

    public String getSourceTableName(Source source) {
        return source.getSourceName();
    }

    public String getSourceFieldTableName(Source source) {
        return getSourceTableName(source)+"_attribute";
    }

    public void createTables(Source source) throws Exception {
        String sourceTableName = getSourceTableName(source);
        SourceHome sourceHome = new SourceHome(ds, source, sourceTableName);
        homes.put(sourceTableName, sourceHome);

        String sourceFieldTableName = getSourceFieldTableName(source);
        SourceFieldHome sourceFieldHome = new SourceFieldHome(ds, cache, source, sourceFieldTableName);
        homes.put(sourceFieldTableName, sourceFieldHome);
    }

    public SourceHome getSourceHome(Source source) throws Exception {
        String tableName = getSourceTableName(source);
        return (SourceHome)homes.get(tableName);
    }

    public SourceFieldHome getSourceFieldHome(Source source) throws Exception {
        String tableName = getSourceFieldTableName(source);
        return (SourceFieldHome)homes.get(tableName);
    }

    /**
     * Reload sources for all entries that has expired.
     *
     * @throws Exception
     */
    public void refresh() throws Exception {

        Date date = new Date();

        Collection entries = getConfig().getEntryDefinitions();
        Set set = new HashSet();

        for (Iterator i=entries.iterator(); i.hasNext(); ) {
            EntryDefinition entry = (EntryDefinition)i.next();

            Collection sources = entry.getSources();

            for (Iterator j=sources.iterator(); j.hasNext(); ) {
                Source source = (Source)j.next();

                if (set.contains(source.getSourceName())) continue;
                set.add(source.getSourceName());

                // Read the source's "cacheExpiration" parameter
                String s = source.getParameter(SourceDefinition.CACHE_EXPIRATION);
                int cacheExpiration = s == null ? 0 : Integer.parseInt(s);
                if (cacheExpiration < 0) cacheExpiration = Integer.MAX_VALUE;

                // Read the source's "loadUponExpiration" parameter
                s = source.getParameter(SourceDefinition.LOAD_UPON_EXPIRATION);
                Boolean loadUponExpiration = s == null ? Boolean.FALSE : new Boolean(s);

                // If the table hasn't been loaded yet and loadUponExpiration == true
                // and expired (current time is past expiration or expiration time == null)
                if (!loadUponExpiration.booleanValue()) continue;

                Date expiration = getExpiration(source);

                // if not yet expired, skip
                if (expiration != null && !expiration.before(date)) continue;

                // reload source
                load(source, null, date);

                Calendar c = Calendar.getInstance();
                c.add(Calendar.MINUTE, cacheExpiration);

                setExpiration(source, cacheExpiration == 0 ? null : c.getTime());
            }
        }
    }

    public Date getExpiration(Source source) throws Exception {
        return sourceExpirationHome.getExpiration(source);
    }

    public void setExpiration(Source source, Date date) throws Exception {
        sourceExpirationHome.setExpiration(source, date);
    }

    public void insert(Source source, Row pk, AttributeValues values, Date date) throws Exception {
        Collection rows = getCacheContext().getTransformEngine().convert(values);

        SourceHome sourceHome = getSourceHome(source);

        for (Iterator i=rows.iterator(); i.hasNext(); ) {
            Row row = (Row)i.next();
            sourceHome.insert(row, date);
        }

        SourceFieldHome sourceFieldHome = getSourceFieldHome(source);

        for (Iterator i=values.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Collection c = values.get(name);
            if (c == null) continue;

            for (Iterator j=c.iterator(); j.hasNext(); ) {
                Object value = j.next();
                if (value == null) continue;
                sourceFieldHome.insert(pk, name, value);
            }
        }
    }

    public void delete(Source source, AttributeValues values, Date date) throws Exception {
        Collection rows = getCacheContext().getTransformEngine().convert(values);

        for (Iterator i=rows.iterator(); i.hasNext(); ) {
            Row row = (Row)i.next();
            String tableName = getSourceTableName(source);
            SourceHome sourceHome = (SourceHome)homes.get(tableName);
            sourceHome.delete(row, date);
        }
    }

    public Date getModifyTime(Source source, String filter) throws Exception {
        String tableName = getSourceTableName(source);
        SourceHome sourceHome = (SourceHome)homes.get(tableName);
        return sourceHome.getModifyTime(filter);
    }

    public Date getModifyTime(Source source) throws Exception {
        return sourceExpirationHome.getModifyTime(source);
    }

    /**
     * Get the row value
     *
     * @param entry the entry (from config)
     * @param rs the result set
     * @return the Map containing the fields/columns and its values
     * @throws Exception
     */
    public Row getRow(EntryDefinition entry, List fieldNames, ResultSet rs) throws Exception {
        Row values = new Row();

        int c = 1;
        for (Iterator i=fieldNames.iterator(); i.hasNext(); c++) {
            String fieldName = (String)i.next();
            Object value = rs.getObject(c);
            values.set(fieldName, value);
        }

        return values;
    }

    /**
     * Join sources
     *
     * @param entryDefinition the entry definition (from config)
     * @return the Collection of rows resulting from the join
     * @throws Exception
     */
    public Collection join(
            EntryDefinition entryDefinition,
            Graph graph,
            Source primarySource,
            String sqlFilter) throws Exception {

        JoinGraphVisitor visitor = new JoinGraphVisitor(entryDefinition);
        graph.traverse(visitor, primarySource);

        List fieldNames = visitor.getFieldNames();
        List tableNames = visitor.getTableNames();
        List joins = visitor.getJoins();

        StringBuffer sb = new StringBuffer();
        sb.append("select ");

        boolean first = true;

        for (Iterator i=fieldNames.iterator(); i.hasNext(); ) {
            String fieldName = (String)i.next();

            if (first) {
                first = false;
            } else {
                sb.append(", ");
            }

            sb.append(fieldName);
        }

        sb.append(" from ");

        Iterator i = tableNames.iterator();
        sb.append(i.next());

        for (Iterator j = joins.iterator(); j.hasNext(); ) {
            String tableName = (String)i.next();
            String join = (String)j.next();

            sb.append(" left join ");
            sb.append(tableName);
            sb.append(" on ");
            sb.append(join);
        }

        if (sqlFilter != null) {
            sb.append(" where ");
            sb.append(sqlFilter);
        }

        Collection fields = primarySource.getPrimaryKeyFields();
        first = true;
        for (Iterator j=fields.iterator(); j.hasNext(); ) {
            Field field = (Field)j.next();

            if (first) {
                sb.append(" order by ");
                first = false;
            } else {
                sb.append(", ");
            }

            sb.append(primarySource.getName());
            sb.append(".");
            sb.append(field.getName());
        }

        String sql = sb.toString();

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
                Row row = getRow(entryDefinition, fieldNames, rs);
                results.add(row);
            }

        } finally {
            if (rs != null) try { rs.close(); } catch (Exception e) {}
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception ex) {}
        }

        return results;
    }

    public Row getPk(Source source, Row row) throws Exception {
        Row pk = new Row();

        Collection fields = source.getPrimaryKeyFields();
        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            Field field = (Field)i.next();
            String name = field.getName();
            Object value = row.get(name);

            pk.set(name, value);
        }

        return pk;
    }

    public SearchResults load(
            Source source,
            Collection pks,
            Date date)
            throws Exception {

        Filter filter = cache.getCacheContext().getFilterTool().createFilter(pks);
        String stringFilter = cache.getCacheFilterTool().toSQLFilter(filter);

        log.info("Loading source "+source.getName()+" "+source.getSourceName()+" with filter "+filter);

        SourceDefinition sourceConfig = source.getSourceDefinition();

        CacheEvent beforeEvent = new CacheEvent(getCacheContext(), sourceConfig, CacheEvent.BEFORE_LOAD_ENTRIES);
        postCacheEvent(sourceConfig, beforeEvent);

        SearchResults results = source.search(filter, 0);

        SourceHome sourceHome = getSourceHome(source);
        sourceHome.delete(stringFilter, date);

        Map records = new HashMap();

        for (Iterator j = results.iterator(); j.hasNext();) {
            Row row = (Row) j.next();
            sourceHome.insert(row, date);

            Row pk = getPk(source, row);

            AttributeValues values = (AttributeValues)records.get(pk);
            if (values == null) {
                values = new AttributeValues();
                records.put(pk, values);
            }

            values.add(row);
        }

        SourceFieldHome sourceFieldHome = getSourceFieldHome(source);

        for (Iterator i=records.keySet().iterator(); i.hasNext(); ) {
            Row pk = (Row)i.next();
            AttributeValues values = (AttributeValues)records.get(pk);

            sourceFieldHome.delete(pk);

            for (Iterator j=values.getNames().iterator(); j.hasNext(); ) {
                String name = (String)j.next();
                Collection c = values.get(name);
                if (c == null) continue;

                for (Iterator k=c.iterator(); k.hasNext(); ) {
                    Object value = k.next();
                    if (value == null) continue;
                    sourceFieldHome.insert(pk, name, value);
                }
            }
        }

        CacheEvent afterEvent = new CacheEvent(getCacheContext(), sourceConfig, CacheEvent.AFTER_LOAD_ENTRIES);
        postCacheEvent(sourceConfig, afterEvent);

        return results;
    }
}
