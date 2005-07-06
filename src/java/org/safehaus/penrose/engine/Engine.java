/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.engine;

import org.safehaus.penrose.cache.SourceCache;
import org.safehaus.penrose.cache.EntryCache;
import org.safehaus.penrose.cache.Cache;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.SearchResults;
import org.safehaus.penrose.*;
import org.safehaus.penrose.thread.ThreadPool;
import org.safehaus.penrose.thread.Queue;
import org.safehaus.penrose.thread.MRSWLock;
import org.safehaus.penrose.engine.impl.*;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.graph.Graph;
import org.safehaus.penrose.mapping.*;
import org.apache.log4j.Logger;
import org.ietf.ldap.LDAPEntry;
import org.ietf.ldap.LDAPException;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class Engine implements EngineMBean {

    public Logger log = Logger.getLogger(Penrose.ENGINE_LOGGER);

    private AddHandler addHandler;
    private BindHandler bindHandler;
    private CompareHandler compareHandler;
    private DeleteHandler deleteHandler;
    private ModifyHandler modifyHandler;
    private ModRdnHandler modRdnHandler;
    private SearchHandler searchHandler;

    private EngineConfig engineConfig;
    private EngineContext engineContext;

    private Cache cache;
    private SourceCache sourceCache;
    private EntryCache entryCache;

    private Hashtable sourceLocks = new Hashtable();
    private Hashtable resultLocks = new Hashtable();
    private Queue threadWaiterQueue = new Queue();

    private ThreadPool threadPool = null;

    private boolean stopping = false;

    /**
     * Initialize the engine with a Penrose instance
     *
     * @param engineContext
     * @throws Exception
     */
    public void init(EngineConfig engineConfig, EngineContext engineContext) throws Exception {
        this.engineConfig = engineConfig;
        this.engineContext = engineContext;

        this.cache = engineContext.getCache();
        this.sourceCache = cache.getSourceCache();
        this.entryCache = cache.getEntryCache();

        createAddHandler();
        createBindHandler();
        createCompareHandler();
        createDeleteHandler();
        createModifyHandler();
        createModRdnHandler();
        createSearchHandler();

        init();

        addHandler.init(this);
        bindHandler.init(this);
        compareHandler.init(this);
        deleteHandler.init(this);
        modifyHandler.init(this);
        modRdnHandler.init(this);
        searchHandler.init(this);
    }

    public void createAddHandler() throws Exception {
        setAddHandler(new DefaultAddHandler());
    }

    public void createBindHandler() throws Exception {
        setBindHandler(new DefaultBindHandler());
    }

    public void createCompareHandler() throws Exception {
        setCompareHandler(new DefaultCompareHandler());
    }

    public void createDeleteHandler() throws Exception {
        setDeleteHandler(new DefaultDeleteHandler());
    }

    public void createModifyHandler() throws Exception {
        setModifyHandler(new DefaultModifyHandler());
    }

    public void createModRdnHandler() throws Exception {
        setModRdnHandler(new DefaultModRdnHandler());
    }

    public void createSearchHandler() throws Exception {
        setSearchHandler(new DefaultSearchHandler());
    }

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


    public int add(PenroseConnection connection, LDAPEntry entry) throws Exception {
        return getAddHandler().add(connection, entry);
    }

    public int bind(PenroseConnection connection, String dn, String password) throws Exception {
        return getBindHandler().bind(connection, dn, password);
    }

    public int compare(PenroseConnection connection, String dn, String attributeName,
            String attributeValue) throws Exception {

        return getCompareHandler().compare(connection, dn, attributeName, attributeValue);
    }

    public int unbind(PenroseConnection connection) throws Exception {
        return getBindHandler().unbind(connection);
    }

    public int delete(PenroseConnection connection, String dn) throws Exception {
        return getDeleteHandler().delete(connection, dn);
    }

    public int modify(PenroseConnection connection, String dn, List modifications) throws Exception {
        return getModifyHandler().modify(connection, dn, modifications);
    }

    public int modrdn(PenroseConnection connection, String dn, String newRdn) throws Exception {
        return getModRdnHandler().modrdn(connection, dn, newRdn);
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

    public void stop() throws Exception {
        if (stopping) return;
        stopping = true;

        // wait for all the worker threads to finish
        threadPool.stopRequestAllWorkers();
    }

    public boolean isStopping() {
        return stopping;
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

    public SourceCache getSourceCache() {
        return sourceCache;
    }

    public void setSourceCache(SourceCache sourceCache) {
        this.sourceCache = sourceCache;
    }

    public BindHandler getBindHandler() {
        return bindHandler;
    }

    public void setBindHandler(BindHandler bindHandler) {
        this.bindHandler = bindHandler;
    }

    public SearchHandler getSearchHandler() {
        return searchHandler;
    }

    public void setSearchHandler(SearchHandler searchHandler) {
        this.searchHandler = searchHandler;
    }

    public AddHandler getAddHandler() {
        return addHandler;
    }

    public void setAddHandler(AddHandler addHandler) {
        this.addHandler = addHandler;
    }

    public ModifyHandler getModifyHandler() {
        return modifyHandler;
    }

    public void setModifyHandler(ModifyHandler modifyHandler) {
        this.modifyHandler = modifyHandler;
    }

    public DeleteHandler getDeleteHandler() {
        return deleteHandler;
    }

    public void setDeleteHandler(DeleteHandler deleteHandler) {
        this.deleteHandler = deleteHandler;
    }

    public CompareHandler getCompareHandler() {
        return compareHandler;
    }

    public void setCompareHandler(CompareHandler compareHandler) {
        this.compareHandler = compareHandler;
    }

    public ModRdnHandler getModRdnHandler() {
        return modRdnHandler;
    }

    public void setModRdnHandler(ModRdnHandler modRdnHandler) {
        this.modRdnHandler = modRdnHandler;
    }

    public EngineConfig getEngineConfig() {
        return engineConfig;
    }

    public void setEngineConfig(EngineConfig engineConfig) {
        this.engineConfig = engineConfig;
    }

    public EngineContext getEngineContext() {
        return engineContext;
    }

    public void setEngineContext(EngineContext engineContext) {
        this.engineContext = engineContext;
    }

    public EntryCache getEntryCache() {
        return entryCache;
    }

    public void setEntryCache(EntryCache entryCache) {
        this.entryCache = entryCache;
    }

    public Cache getCache() {
        return cache;
    }

    public void setCache(Cache cache) {
        this.cache = cache;
    }

    public Collection load(
            Entry parent,
            EntryDefinition entryDefinition,
            Collection pks)
            throws Exception {

        Graph graph = getEngineContext().getConfig().getGraph(entryDefinition);
        Source primarySource = getEngineContext().getConfig().getPrimarySource(entryDefinition);

        LoaderGraphVisitor loaderVisitor = new LoaderGraphVisitor(this, entryDefinition, pks);
        graph.traverse(loaderVisitor, primarySource);

        Collection results = new ArrayList();

        Map attributeValues = loaderVisitor.getAttributeValues();
        for (Iterator i=attributeValues.keySet().iterator(); i.hasNext(); ) {
            Row pk = (Row)i.next();
            AttributeValues values = (AttributeValues)attributeValues.get(pk);

            Collection c = getEngineContext().getTransformEngine().convert(values);
            results.addAll(c);
        }
/*

        for (Iterator i=pks.iterator(); i.hasNext(); ) {
            Row pk = (Row)i.next();

            JoinGraphVisitor joinerVisitor = new JoinGraphVisitor(entryDefinition, primarySource, sourceCache, pk);
            graph.traverse(joinerVisitor, primarySource);

            AttributeValues values = joinerVisitor.getAttributeValues();
            Collection c = getEngineContext().getTransformEngine().convert(values);

            results.addAll(c);
        }
*/
        log.debug("Rows:");

        for (Iterator j = results.iterator(); j.hasNext(); ) {
            Row row = (Row)j.next();
            log.debug(" - "+row);
        }

        log.debug("Loaded " + results.size() + " rows.");

        return results;
    }

    public Collection merge(Entry parent, EntryDefinition entryDefinition, Collection rows) throws Exception {

        Collection results = new ArrayList();

        //log.debug("Merging:");
        // merge rows into attribute values
        Map entries = new LinkedHashMap();
        for (Iterator i = rows.iterator(); i.hasNext();) {
            Row row = (Row)i.next();
            //log.debug(" - "+row);

            Map rdn = new HashMap();
            Row values = new Row();

            boolean validPK = getEngineContext().getTransformEngine().translate(entryDefinition, row, rdn, values);
            if (!validPK) continue;

            //log.debug(" - "+rdn+": "+values);

            AttributeValues attributeValues = (AttributeValues)entries.get(rdn);
            if (attributeValues == null) {
                attributeValues = new AttributeValues();
                entries.put(rdn, attributeValues);
            }
            attributeValues.add(values);
        }

        log.debug("Merged " + entries.size() + " entries.");

        for (Iterator i=entries.values().iterator(); i.hasNext(); ) {
            AttributeValues values = (AttributeValues)i.next();

            Entry entry = new Entry(entryDefinition, values);
            entry.setParent(parent);
            results.add(entry);
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

    public Map load(
            Source source,
            Collection pks)
            throws Exception {

        log.info("Loading source "+source.getName()+" "+source.getSourceName()+" with pks "+pks);

        SourceDefinition sourceConfig = source.getSourceDefinition();

        //CacheEvent beforeEvent = new CacheEvent(getCacheContext(), sourceConfig, CacheEvent.BEFORE_LOAD_ENTRIES);
        //postCacheEvent(sourceConfig, beforeEvent);

        Collection loadedPks = sourceCache.getPks(source, pks);
        log.debug("Loaded pks: "+loadedPks);

        Collection pksToLoad = new HashSet();
        for (Iterator i=pks.iterator(); i.hasNext(); ) {
            Row pk = (Row)i.next();

            boolean found = false;
            for (Iterator j=loadedPks.iterator(); !found && j.hasNext(); ) {
                Row lpk = (Row)j.next();
                if (match(lpk, pk)) found = true;
            }

            if (!found) pksToLoad.add(pk);
        }
        log.debug("Pks to load: "+pksToLoad);

        Map results = new HashMap();

        if (!pksToLoad.isEmpty()) {
            Filter filter = cache.getCacheContext().getFilterTool().createFilter(pksToLoad);
            SearchResults sr = source.search(filter, 0);

            for (Iterator j = sr.iterator(); j.hasNext();) {
                Row row = (Row) j.next();
                Row pk = getPk(source, row);

                AttributeValues values = (AttributeValues)results.get(pk);
                if (values == null) {
                    values = new AttributeValues();
                    results.put(pk, values);
                }

                values.add(row); // merging row
            }

            for (Iterator i=results.keySet().iterator(); i.hasNext(); ) {
                Row pk = (Row)i.next();
                AttributeValues values = (AttributeValues)results.get(pk);

                sourceCache.put(source, pk, values);
            }
        }

        for (Iterator j=loadedPks.iterator();  j.hasNext(); ) {
            Row pk = (Row)j.next();
            AttributeValues values = sourceCache.get(source, pk);
            results.put(pk, values);
        }
        
        //CacheEvent afterEvent = new CacheEvent(getCacheContext(), sourceConfig, CacheEvent.AFTER_LOAD_ENTRIES);
        //postCacheEvent(sourceConfig, afterEvent);

        return results;
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

}

