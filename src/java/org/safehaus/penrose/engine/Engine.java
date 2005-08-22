/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.engine;

import org.safehaus.penrose.cache.Cache;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.SearchResults;
import org.safehaus.penrose.*;
import org.safehaus.penrose.thread.ThreadPool;
import org.safehaus.penrose.thread.Queue;
import org.safehaus.penrose.thread.MRSWLock;
import org.safehaus.penrose.engine.impl.*;
import org.safehaus.penrose.mapping.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ietf.ldap.LDAPEntry;
import org.ietf.ldap.LDAPException;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class Engine implements EngineMBean {

    Logger log = LoggerFactory.getLogger(getClass());

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
        setAddHandler(new AddHandler());
    }

    public void createBindHandler() throws Exception {
        setBindHandler(new BindHandler());
    }

    public void createCompareHandler() throws Exception {
        setCompareHandler(new CompareHandler());
    }

    public void createDeleteHandler() throws Exception {
        setDeleteHandler(new DeleteHandler());
    }

    public void createModifyHandler() throws Exception {
        setModifyHandler(new ModifyHandler());
    }

    public void createModRdnHandler() throws Exception {
        setModRdnHandler(new ModRdnHandler());
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

    public Cache getCache() {
        return cache;
    }

    public void setCache(Cache cache) {
        this.cache = cache;
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
}

