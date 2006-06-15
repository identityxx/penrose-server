/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.safehaus.penrose.handler;

import org.safehaus.penrose.session.PenroseSearchResults;
import org.safehaus.penrose.session.PenroseSession;
import org.safehaus.penrose.session.SessionManager;
import org.safehaus.penrose.session.PenroseSearchControls;
import org.safehaus.penrose.acl.ACLEngine;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.schema.SchemaManager;
import org.safehaus.penrose.engine.Engine;
import org.safehaus.penrose.interpreter.InterpreterManager;
import org.safehaus.penrose.module.Module;
import org.safehaus.penrose.module.ModuleManager;
import org.safehaus.penrose.event.*;
import org.safehaus.penrose.pipeline.PipelineAdapter;
import org.safehaus.penrose.pipeline.PipelineEvent;
import org.safehaus.penrose.mapping.Entry;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.Penrose;
import org.apache.log4j.Logger;

import javax.naming.directory.*;
import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class Handler {

    Logger log = Logger.getLogger(getClass());

    public final static String STOPPED  = "STOPPED";
    public final static String STARTING = "STARTING";
    public final static String STARTED  = "STARTED";
    public final static String STOPPING = "STOPPING";

    private PenroseConfig penroseConfig;

    private Penrose penrose;

    private AddHandler addHandler;
    private BindHandler bindHandler;
    private CompareHandler compareHandler;
    private DeleteHandler deleteHandler;
    private ModifyHandler modifyHandler;
    private ModRdnHandler modRdnHandler;
    private FindHandler findHandler;
    private SearchHandler searchHandler;

    private SchemaManager schemaManager;
    private Engine engine;

    private SessionManager sessionManager;
    private PartitionManager partitionManager;

    private InterpreterManager interpreterManager;
    private ACLEngine aclEngine;
    private FilterTool filterTool;

    private String status = STOPPED;

    public Handler() {
        aclEngine = new ACLEngine();

        addHandler = createAddHandler();
        bindHandler = createBindHandler();
        compareHandler = createCompareHandler();
        deleteHandler = createDeleteHandler();
        modifyHandler = createModifyHandler();
        modRdnHandler = createModRdnHandler();
        findHandler = createFindHandler();
        searchHandler = createSearchHandler();
    }

    public AddHandler createAddHandler() {
        return new AddHandler(this);
    }

    public BindHandler createBindHandler() {
        return new BindHandler(this);
    }

    public CompareHandler createCompareHandler() {
        return new CompareHandler(this);
    }

    public DeleteHandler createDeleteHandler() {
        return new DeleteHandler(this);
    }

    public ModifyHandler createModifyHandler() {
        return new ModifyHandler(this);
    }

    public ModRdnHandler createModRdnHandler() {
        return new ModRdnHandler(this);
    }

    public FindHandler createFindHandler() {
        return new FindHandler(this);
    }

    public SearchHandler createSearchHandler() {
        return new SearchHandler(this);
    }

    public void start() throws Exception {

        if (status != STOPPED) return;

        //log.debug("Starting SessionHandler...");

        try {
            status = STARTING;

            filterTool = new FilterTool();
            filterTool.setSchemaManager(schemaManager);

            status = STARTED;

            //log.debug("SessionHandler started.");

        } catch (Exception e) {
            status = STOPPED;
            log.error(e.getMessage(), e);
            throw e;
        }
    }

    public void stop() throws Exception {

        if (status != STARTED) return;

        try {
            status = STOPPING;

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

        status = STOPPED;
    }

    public int add(PenroseSession session, String dn, Attributes attributes) throws Exception {
        return getAddHandler().add(session, dn, attributes);
    }

    public int bind(PenroseSession session, String dn, String password) throws Exception {
        return getBindHandler().bind(session, dn, password);
    }

    public int unbind(PenroseSession session) throws Exception {
        return getBindHandler().unbind(session);
    }

    public int compare(PenroseSession session, String dn, String attributeName,
            Object attributeValue) throws Exception {
        return getCompareHandler().compare(session, dn, attributeName, attributeValue);
    }

    public int delete(PenroseSession session, String dn) throws Exception {
        return getDeleteHandler().delete(session, dn);
    }

    public int modify(PenroseSession session, String dn, Collection modifications) throws Exception {
        return getModifyHandler().modify(session, dn, modifications);
    }

    public int modrdn(PenroseSession session, String dn, String newRdn) throws Exception {
        return getModRdnHandler().modrdn(session, dn, newRdn);
    }

    /**
     * @param session
     * @param baseDn
     * @param filter
     * @param sc
     * @param results The results will be filled with objects of type SearchResult.
     * @return
     * @throws Exception
     */
    public int search(
            final PenroseSession session,
            final String baseDn,
            final String filter,
            final PenroseSearchControls sc,
            final PenroseSearchResults results)
            throws Exception {

        final PenroseSearchResults tempResults = new PenroseSearchResults();

        tempResults.addListener(new PipelineAdapter() {

            public void objectAdded(PipelineEvent event) {
                try {
                    Entry entry = (Entry)event.getObject();

                    SearchResult searchResult = getSearchHandler().createSearchResult(session, entry);
                    if (searchResult == null) return;

                    results.add(searchResult);

                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }

            public void pipelineClosed(PipelineEvent event) {
                results.setReturnCode(tempResults.getReturnCode());
                results.close();
            }
        });

        return getSearchHandler().search(session, baseDn, filter, sc, tempResults);
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

    public InterpreterManager getInterpreterFactory() {
        return interpreterManager;
    }

    public void setInterpreterFactory(InterpreterManager interpreterManager) {
        this.interpreterManager = interpreterManager;
    }

    public Engine getEngine() {
        return engine;
    }

    public void setEngine(Engine engine) {
        this.engine = engine;
    }

    public PartitionManager getPartitionManager() {
        return partitionManager;
    }

    public void setPartitionManager(PartitionManager partitionManager) {
        this.partitionManager = partitionManager;
        aclEngine.setPartitionManager(partitionManager);
    }

    public FilterTool getFilterTool() {
        return filterTool;
    }

    public void setFilterTool(FilterTool filterTool) {
        this.filterTool = filterTool;
    }

    public ACLEngine getACLEngine() {
        return aclEngine;
    }

    public void setACLEngine(ACLEngine aclEngine) {
        this.aclEngine = aclEngine;
    }

    // ------------------------------------------------
    // Listeners
    // ------------------------------------------------

    public void addConnectionListener(ConnectionListener l) {
    }

    public void removeConnectionListener(ConnectionListener l) {
    }

    public void addBindListener(BindListener l) {
    }

    public void removeBindListener(BindListener l) {
    }

    public void addSearchListener(SearchListener l) {
    }

    public void removeSearchListener(SearchListener l) {
    }

    public void addCompareListener(CompareListener l) {
    }

    public void removeCompareListener(CompareListener l) {
    }

    public void addAddListener(AddListener l) {
    }

    public void removeAddListener(AddListener l) {
    }

    public void addDeleteListener(DeleteListener l) {
    }

    public void removeDeleteListener(DeleteListener l) {
    }

    public void addModifyListener(ModifyListener l) {
    }

    public void removeModifyListener(ModifyListener l) {
    }

    public SchemaManager getSchemaManager() {
        return schemaManager;
    }

    public void setSchemaManager(SchemaManager schemaManager) {
        this.schemaManager = schemaManager;
        aclEngine.setSchemaManager(schemaManager);
    }

    public SessionManager getSessionManager() {
        return sessionManager;
    }

    public void setSessionManager(SessionManager sessionManager) {
        this.sessionManager = sessionManager;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public FindHandler getFindHandler() {
        return findHandler;
    }

    public void setFindHandler(FindHandler findHandler) {
        this.findHandler = findHandler;
    }

    public PenroseConfig getPenroseConfig() {
        return penroseConfig;
    }

    public void setPenroseConfig(PenroseConfig penroseConfig) {
        this.penroseConfig = penroseConfig;
        aclEngine.setPenroseConfig(penroseConfig);
    }

    public Penrose getPenrose() {
        return penrose;
    }

    public void setPenrose(Penrose penrose) {
        this.penrose = penrose;
    }
}

