/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.engine;


import java.util.*;

import org.safehaus.penrose.thread.MRSWLock;
import org.safehaus.penrose.thread.ThreadPool;
import org.safehaus.penrose.thread.Queue;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.SearchResults;
import org.safehaus.penrose.PenroseConnection;
import org.ietf.ldap.LDAPException;

/**
 * @author Endi S. Dewata
 */
public class DefaultEngine extends Engine {

    private Hashtable sourceLocks = new Hashtable();
    private Hashtable resultLocks = new Hashtable();
    private Queue threadWaiterQueue = new Queue();
    private ThreadPool threadPool = null;

	public void init() throws Exception {

        log.debug("-------------------------------------------------");
        log.debug("Initializing Engine");

        initThreadPool();
	}

	public void initThreadPool() throws Exception {
		// Now threadPoolSize is now hardcoded to 20
		// TODO modify threadPoolSize to read from configuration if needed
		int threadPoolSize = 20;
		threadPool = new ThreadPool(threadPoolSize);

		RefreshThread r1 = new RefreshThread(this);
		threadPool.execute(r1);
	}

    public synchronized MRSWLock getLock(Source source) {
		String name = source.getConnectionName() + "." + source.getSourceName();

		MRSWLock lock = (MRSWLock) sourceLocks.get(name);

		if (lock == null) lock = new MRSWLock(threadWaiterQueue);
		sourceLocks.put(name, lock);

		return lock;
	}

	public synchronized MRSWLock getLock(String resultName) {

		MRSWLock lock = (MRSWLock) resultLocks.get(resultName);

		if (lock == null) lock = new MRSWLock(threadWaiterQueue);
		resultLocks.put(resultName, lock);

		return lock;
	}

    public void stop() throws Exception {
        super.stop();

        // wait for all the worker threads to finish
        threadPool.stopRequestAllWorkers();
    }

    public SearchResults search(PenroseConnection connection, String base, int scope,
            int deref, String filter, Collection attributeNames)
            throws Exception {

        SearchResults results = new SearchResults();

        try {
            SearchThread searchRunnable = new SearchThread(getSearchHandler(),
                    connection, base, scope, deref, filter, attributeNames,
                    results);
            threadPool.execute(searchRunnable);

        } catch (Throwable e) {
            log.error(e.getMessage(), e);
            results.setReturnCode(LDAPException.OPERATIONS_ERROR);
            results.close();
        }

        return results;
    }

    /**
     * Load entries into the temporary table in the cache (synchronized)
     *
     * @param con the JDBC connection
     * @param source the source (from config)
     * @param filter the filter (from config)
     * @param attributeNames collection of attribute names
     * @param temporary whether we are using temporary tables
     * @throws Exception
     */
/*
    public void loadEntries(Source source, Filter filter, Collection attributeNames, boolean temporary) throws Exception {
        SourceDefinition sourceConfig = (SourceDefinition)penrose.getConfig().getSources().get(source.getName());
    	String tableName = cache.getTableName(sourceConfig, false);
    	Boolean locked = (Boolean)tableStatus.get(tableName);
    	while (locked == Boolean.TRUE) {
    		// need to wait
    		try {
    			wait();
    		} catch (InterruptedException ex) {}
    		locked = (Boolean)tableStatus.get(tableName);
    	}
    	tableStatus.put(tableName, Boolean.TRUE);
        tableStatus.remove(tableName);
        notifyAll();
    }

    public void loadResults() throws Exception {

        Map entries = config.getEntryDefinitions();

        Iterator iter = entries.keySet().iterator();
        while (iter.hasNext()) {
            String dn = (String) iter.next();
            EntryDefinition entry = config.getDn(dn);

            // See if the result is dirty
            boolean dirty = false;
            List sources = entry.getSources();
            log.debug("dn: " + dn);

            if (entry.isDynamic() && dirty) {
                String searchDn = dn.replace("...", "*");
                log.debug("Loading results table for dn: " + searchDn);
                Entry sr = penrose.getSearchHandler().searchObject(null, searchDn, new ArrayList());
            }
        }
    }


    public Date getModifyTime(EntryDefinition entry, SourceDefinition sourceConfig, String filter) throws Exception {

        String t1 = cache.getTableName(sourceConfig, true);
        SourceHome s1 = (SourceHome)sourceTables.get(t1);
        return s1.getModifyTime(filter);
    }

    public void updateExpiration(SourceDefinition sourceConfig, Calendar calendar) throws Exception {

        int defaultCacheExpiration = config.getCache().getCacheExpiration();
        String s = sourceConfig.getParameter(SourceDefinition.CACHE_EXPIRATION);

        Integer cacheExpiration = s == null ? new Integer(defaultCacheExpiration) : new Integer(s);
        if (cacheExpiration.intValue() < 0) cacheExpiration = new Integer(Integer.MAX_VALUE);

        Calendar c = (Calendar) calendar.clone();
        c.add(Calendar.MINUTE, cacheExpiration.intValue());

        sourceExpirationHome.setExpiration(sourceConfig, cacheExpiration.intValue() == 0 ? null : c.getTime());
        //sourceExpirationHome.setModifyTime(sourceConfig, calendar.getTime());
    }

    public boolean isExpired(SourceDefinition sourceConfig, Calendar calendar) throws Exception {

        Date expiration = sourceExpirationHome.getExpiration(sourceConfig);
        return expiration == null || expiration.before(calendar.getTime());
    }

	public String toString(int[] x) {
		StringBuffer sb = new StringBuffer();
		for (int i = 0; x != null && i < x.length; i++) {
			sb.append(x[i] + " ");
		}
		return sb.toString();
	}

	public void updateDirtyEntries(
            SourceDefinition sourceConfig,
			Filter filter,
            Collection attributeNames,
            boolean temporary)
			throws Exception {
		log.info("--------------------------------------------------------------------------------");
		log.info("LOAD DIRTY ENTRIES");
		log.info(" - source: " + sourceConfig.getName());
		log.info(" - filter: " + filter);
		log.info(" - attributeNames: " + attributeNames);
		log.info(" - temporary: " + temporary);

		CacheEvent beforeEvent = new CacheEvent(penrose, sourceConfig,
				CacheEvent.BEFORE_LOAD_ENTRIES);
		postCacheEvent(sourceConfig, beforeEvent);

		ConnectionConfig connection = (ConnectionConfig)config.connections.get(sourceConfig.getConnectionName());
		Adapter adapter = (Adapter) penrose.getAdapterConfigs().get(connection.getAdapterName());

		SearchResults results = adapter.search(sourceConfig, null);

		log.debug("Rows:");
		for (Iterator j = results.iterator(); j.hasNext();) {
			Map row = (Map) j.next();

			log.debug(" - " + row);
			// TODO update this
			//updateRow(sourceConfig, row, temporary);
		}

		CacheEvent afterEvent = new CacheEvent(penrose, sourceConfig, CacheEvent.AFTER_LOAD_ENTRIES);
		postCacheEvent(sourceConfig, afterEvent);
	}

	public void setValidity(EntryDefinition entry, Map values, boolean validity) throws Exception {

        Collection rows = penrose.getTransformEngine().generateCrossProducts(values);

        for (Iterator i=rows.iterator(); i.hasNext(); ) {
            Map row = (Map)i.next();
            if (validity) {
                log.debug("Validating cache: "+row);
            } else {
                log.debug("Invalidating cache: "+row);
            }

            String tableName = getTableName(entry, false);
            ResultHome resultHome = (ResultHome)resultTables.get(tableName);
            //resultHome.setValidity(row, validity);
        }

        Date now = new Date();
        resultExpirationHome.setModifyTime(entry, now);
	}

	public void updateResult(String operation, EntryDefinition entry, Map row, boolean temporary) throws Exception {

		log.debug("UPDATE RESULT called ========================");
		log.debug("operation = " + operation);
		log.debug("entry = " + entry);
		log.debug("row = " + row);
		log.debug("temporary = " + temporary);

        String tableName = getTableName(entry, temporary);
        ResultHome resultHome = (ResultHome)resultTables.get(tableName);

        log.debug("row = " + row);

        // build keys list
        List keys = new ArrayList();
        keys.addAll(row.keySet());
        log.debug("keys = " + keys);

        // build values (list of list)
        List values = new ArrayList();
        for (Iterator i = row.keySet().iterator(); i.hasNext(); ) {
            String key = (String)i.next();

            Collection c = (Collection) row.get(key);

            List mvalues = new ArrayList();
            if (entry.getAttributeValues().containsKey(key)) {
                mvalues.addAll(c);

            } else if (c.size() >= 1) {
                Object o = c.iterator().next();
                mvalues.add(o);
            }

            values.add(mvalues);
        }
        log.debug("values = " + values);

        // indices
        int[] indices = new int[keys.size()];

        boolean keepgoing = true;
        while (keepgoing) {
            // update
            Map mrow = new HashMap();
            log.debug("indices = " + toString(indices));

            for (int i = 0; i < keys.size() && i < values.size(); i++) {
                try {
                    String key = (String) keys.get(i);
                    mrow.put(key, ((List) values.get(i)).get(indices[i]));
                } catch (IndexOutOfBoundsException ex) {
                    log.debug(ex.toString());
                } catch (Exception ex) {
                    log.error(ex.toString());
                }
            }
            log.debug("operation = " + operation);
            log.debug("mrow = " + mrow);

            if ("insert".equals(operation)) {
                resultHome.insert(mrow);

            } else if ("delete".equals(operation)) {
                resultHome.delete(mrow);

            } else if ("invalidate".equals(operation)) {
                resultHome.setValidity(mrow, false);

            } else if ("validate".equals(operation)) {
                resultHome.setValidity(mrow, true);
            }

            // advance to next
            for (int i = indices.length - 1; i >= 0; i--) {
                if (indices[i] < ((List) values.get(i)).size() - 1) {
                    indices[i]++;
                    for (int j = i + 1; j < indices.length; j++) {
                        indices[j] = 0;
                    }
                    break;
                } else if (i == 0) {
                    keepgoing = false;
                }
            }
        }

        // Update the expiration and modification time
        Date now = new Date();
        resultExpirationHome.setModifyTime(entry, now);
	}
*/
}