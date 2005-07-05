/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.cache;

import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.config.Config;
import org.safehaus.penrose.event.CacheEvent;
import org.safehaus.penrose.event.CacheListener;
import org.safehaus.penrose.SearchResults;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.cache.impl.JoinGraphVisitor;
import org.safehaus.penrose.cache.impl.SourceLoaderGraphVisitor;
import org.safehaus.penrose.graph.Graph;
import org.safehaus.penrose.filter.Filter;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class SourceCache {

    public Logger log = Logger.getLogger(Penrose.CACHE_LOGGER);

    private Cache cache;
    private CacheContext cacheContext;
    private Config config;

    private Map records = new HashMap();

    public void init(Cache cache) throws Exception {
        this.cache = cache;
        this.cacheContext = cache.getCacheContext();
        this.config = cacheContext.getConfig();

        init();
    }

    public void init() throws Exception {
    }

    public void refresh() throws Exception {
    }

    public Date getExpiration(Source source) throws Exception {
        return null;
    }

    public void setExpiration(Source source, Date date) throws Exception {
    }

    public Map getMap(Source source) {
        Map map = (Map)records.get(source.getSourceName());
        if (map == null) {
            map = new TreeMap();
            records.put(source.getSourceName(), map);
        }
        return map;
    }

    public AttributeValues get(Source source, Row pk) throws Exception {
        Map map = getMap(source);

        for (Iterator i=map.keySet().iterator(); i.hasNext(); ) {
            Row spk = (Row)i.next();
            if (match(spk, pk)) return (AttributeValues)map.get(spk);
        }

        return null;
    }

    public Map get(Source source, Collection pks) throws Exception {

        Map results = new HashMap();

        for (Iterator i=pks.iterator(); i.hasNext(); ) {
            Row pk = (Row)i.next();

            AttributeValues values = get(source, pk);
            if (values == null) continue;

            results.put(pk, values);
        }

        return results;
    }

    public void put(Source source, Row pk, AttributeValues values, Date date) throws Exception {
        Map map = getMap(source);
        map.put(pk, values);
    }

    public void delete(Source source, Row pk, Date date) throws Exception {
        Map map = getMap(source);

        for (Iterator i=map.keySet().iterator(); i.hasNext(); ) {
            Row spk = (Row)i.next();
            if (match(spk, pk)) map.remove(spk);
        }
    }

    public Collection join(
            EntryDefinition entryDefinition,
            Collection pks) throws Exception {

        Collection results = new ArrayList();

        for (Iterator i=pks.iterator(); i.hasNext(); ) {
            Row pk = (Row)i.next();
            Collection rows = join(entryDefinition, pk);
            results.addAll(rows);
        }

        return results;
    }

    public Collection join(
            EntryDefinition entryDefinition,
            Row pk) throws Exception {

        Graph graph = getConfig().getGraph(entryDefinition);
        Source primarySource = getConfig().getPrimarySource(entryDefinition);

        JoinGraphVisitor visitor = new JoinGraphVisitor(entryDefinition, primarySource, this, pk);
        graph.traverse(visitor, primarySource);

        AttributeValues values = visitor.getAttributeValues();
        log.debug("Rows:");

        Collection rows = getCacheContext().getTransformEngine().convert(values);
        for (Iterator i = rows.iterator(); i.hasNext(); ) {
            Row row = (Row)i.next();
            log.debug(" - "+row);
        }

        return rows;
/*
        Filter filter = getCacheContext().getFilterTool().createFilter(pks);
        String sqlFilter = ((DefaultCache)getCache()).getCacheFilterTool().toSQLFilter(filter);

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
*/
    }

    public void postCacheEvent(SourceDefinition sourceConfig, CacheEvent event)
            throws Exception {
        List listeners = sourceConfig.getListeners();

        for (Iterator i = listeners.iterator(); i.hasNext();) {
            Object listener = i.next();
            if (!(listener instanceof CacheListener))
                continue;

            CacheListener cacheListener = (CacheListener) listener;
            switch (event.getType()) {
            case CacheEvent.BEFORE_LOAD_ENTRIES:
                cacheListener.beforeLoadEntries(event);
                break;

            case CacheEvent.AFTER_LOAD_ENTRIES:
                cacheListener.afterLoadEntries(event);
                break;
            }
        }
    }

    public boolean partialMatch(Row pk1, Row pk2) throws Exception {

        for (Iterator i=pk2.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Object v1 = pk1.get(name);
            Object v2 = pk2.get(name);

            if (v1 == null && v2 == null) {
                continue;

            } else if (v1 == null || v2 == null) {
                return false;

            } else  if (!(v1.toString()).equalsIgnoreCase(v2.toString())) {
                return false;
            }
        }

        return true;
    }

    public boolean match(Row pk1, Row pk2) throws Exception {

        if (!pk1.getNames().equals(pk2.getNames())) return false;

        for (Iterator i=pk2.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Object v1 = pk1.get(name);
            Object v2 = pk2.get(name);

            if (v1 == null && v2 == null) {
                continue;

            } else if (v1 == null || v2 == null) {
                return false;

            } else  if (!(v1.toString()).equalsIgnoreCase(v2.toString())) {
                return false;
            }
        }

        return true;
    }

    public Collection searchPks(
            Source source,
            Collection pks)
            throws Exception {

        Map map = getMap(source);
        log.debug("PKs in cache: "+map.keySet());

        Collection results = new TreeSet();

        for (Iterator i=map.keySet().iterator(); i.hasNext(); ) {
            Row pk = (Row)i.next();

            boolean found = false;

            for (Iterator j=pks.iterator(); !found && j.hasNext(); ) {
                Row spk = (Row)j.next();

                found = partialMatch(pk, spk);
            }

            if (found) results.add(pk);
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

    public void load(
            EntryDefinition entryDefinition,
            Collection pks,
            Date date)
            throws Exception {

        Graph graph = getConfig().getGraph(entryDefinition);
        Source primarySource = getConfig().getPrimarySource(entryDefinition);

        SourceLoaderGraphVisitor visitor = new SourceLoaderGraphVisitor(this, entryDefinition, pks, date);
        graph.traverse(visitor, primarySource);
    }

    public Map load(
            Source source,
            Collection pks,
            Date date)
            throws Exception {

        log.info("Loading source "+source.getName()+" "+source.getSourceName()+" with pks "+pks);

        Filter filter = cache.getCacheContext().getFilterTool().createFilter(pks);

        SourceDefinition sourceConfig = source.getSourceDefinition();

        CacheEvent beforeEvent = new CacheEvent(getCacheContext(), sourceConfig, CacheEvent.BEFORE_LOAD_ENTRIES);
        postCacheEvent(sourceConfig, beforeEvent);

        SearchResults sr = source.search(filter, 0);

        Map results = new HashMap();

        for (Iterator j = sr.iterator(); j.hasNext();) {
            Row row = (Row) j.next();
            Row pk = getPk(source, row);

            AttributeValues values = (AttributeValues)results.get(pk);
            if (values == null) {
                values = new AttributeValues();
                results.put(pk, values);
            }

            values.add(row);
        }

        for (Iterator i=results.keySet().iterator(); i.hasNext(); ) {
            Row pk = (Row)i.next();
            AttributeValues values = (AttributeValues)results.get(pk);

            put(source, pk, values, date);
        }

        CacheEvent afterEvent = new CacheEvent(getCacheContext(), sourceConfig, CacheEvent.AFTER_LOAD_ENTRIES);
        postCacheEvent(sourceConfig, afterEvent);

        return results;
    }

    public Cache getCache() {
        return cache;
    }

    public void setCache(Cache cache) {
        this.cache = cache;
    }

    public CacheContext getCacheContext() {
        return cacheContext;
    }

    public void setCacheContext(CacheContext cacheContext) {
        this.cacheContext = cacheContext;
    }

    public Config getConfig() {
        return config;
    }

    public void setConfig(Config config) {
        this.config = config;
    }
}
