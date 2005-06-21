/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.engine;

import org.safehaus.penrose.cache.SourceCache;
import org.safehaus.penrose.cache.EntryCache;
import org.safehaus.penrose.cache.DefaultCache;
import org.safehaus.penrose.cache.Cache;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.SearchResults;
import org.safehaus.penrose.*;
import org.apache.log4j.Logger;
import org.ietf.ldap.LDAPEntry;
import org.ietf.ldap.LDAPException;

import java.util.Collection;
import java.util.List;

/**
 * @author Endi S. Dewata
 */
public abstract class Engine implements EngineMBean {

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

        initHandlers();
        init();
    }

    public void initHandlers() throws Exception {
        Class addHandlerClass = Class.forName(engineConfig.getAddHandlerClass());
        addHandler = (AddHandler)addHandlerClass.newInstance();
        addHandler.init(this, engineContext);

        Class bindHandlerClass = Class.forName(engineConfig.getBindHandlerClass());
        bindHandler = (BindHandler)bindHandlerClass.newInstance();
        bindHandler.init(this, engineContext);

        Class compareHandlerClass = Class.forName(engineConfig.getCompareHandlerClass());
        compareHandler = (CompareHandler)compareHandlerClass.newInstance();
        compareHandler.init(this, engineContext);

        Class deleteHandlerClass = Class.forName(engineConfig.getDeleteHandlerClass());
        deleteHandler = (DeleteHandler)deleteHandlerClass.newInstance();
        deleteHandler.init(this, engineContext);

        Class modifyHandlerClass = Class.forName(engineConfig.getModifyHandlerClass());
        modifyHandler = (ModifyHandler)modifyHandlerClass.newInstance();
        modifyHandler.init(this, engineContext);

        Class modRdnHandlerClass = Class.forName(engineConfig.getModRdnHandlerClass());
        modRdnHandler = (ModRdnHandler)modRdnHandlerClass.newInstance();
        modRdnHandler.init(this, engineContext);

        Class searchHandlerClass = Class.forName(engineConfig.getSearchHandlerClass());
        searchHandler = (SearchHandler)searchHandlerClass.newInstance();
        searchHandler.init(this, engineContext);
    }

	public abstract void init() throws Exception;

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
            getSearchHandler().search(connection, base, scope, deref, filter, attributeNames, results);

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
    }

    public boolean isStopping() {
        return stopping;
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
}

