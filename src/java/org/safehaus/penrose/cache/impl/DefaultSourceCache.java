/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.cache.impl;

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
import java.sql.ResultSet;

/**
 * @author Endi S. Dewata
 */
public class DefaultSourceCache extends SourceCache {

    public DefaultCache cache;

    public SourceExpirationHome sourceExpirationHome;
    public Map homes = new HashMap();

    private DataSource ds;

    public void init() throws Exception {

        cache = (DefaultCache)super.getCache();
        ds = cache.getDs();

        Date date = new Date();

        Collection entries = getConfig().getEntryDefinitions();
        Set set = new HashSet();

        sourceExpirationHome = new SourceExpirationHome(ds);

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

    public AttributeValues get(Source source, Row pk) throws Exception {

        //log.debug("Getting source cache for pk: "+pk);

        SourceFieldHome sourceFieldHome = getSourceFieldHome(source);
        Collection rows = sourceFieldHome.search(pk);
        if (rows.size() == 0) return null;

        //log.debug("Fields:");

        AttributeValues values = new AttributeValues();
        for (Iterator i = rows.iterator(); i.hasNext();) {
            Row row = (Row)i.next();
            Row newRow = new Row();

            for (Iterator j = row.getNames().iterator(); j.hasNext(); ) {
                String name = (String)j.next();
                Object value = row.get(name);

                newRow.set(name, value);
            }

            //log.debug(" - "+newRow);
            values.add(newRow);
        }

        return values;
    }

    public void put(Source source, Row pk, AttributeValues values, Date date) throws Exception {

        SourceHome sourceHome = getSourceHome(source);
        sourceHome.delete(pk);
        sourceHome.insert(pk, date);

        SourceFieldHome sourceFieldHome = getSourceFieldHome(source);
        sourceFieldHome.delete(pk);

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

    public void delete(Source source, Row pk, Date date) throws Exception {
        SourceHome sourceHome = getSourceHome(source);
        sourceHome.delete(pk);

        SourceFieldHome sourceFieldHome = getSourceFieldHome(source);
        sourceFieldHome.delete(pk);
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

    public Collection searchPks(
            Source source,
            Collection pks)
            throws Exception {

        SourceFieldHome sourceFieldHome = getSourceFieldHome(source);
        return sourceFieldHome.searchPks(pks);
    }

}
