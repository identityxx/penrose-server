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
import org.safehaus.penrose.user.UserConfig;
import org.safehaus.penrose.session.PenroseSession;
import org.safehaus.penrose.session.SessionManager;
import org.safehaus.penrose.session.SessionConfig;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.acl.ACLEngine;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.schema.SchemaManager;
import org.safehaus.penrose.engine.Engine;
import org.safehaus.penrose.interpreter.InterpreterFactory;
import org.safehaus.penrose.module.Module;
import org.safehaus.penrose.module.ModuleMapping;
import org.safehaus.penrose.module.ModuleConfig;
import org.safehaus.penrose.module.ModuleManager;
import org.safehaus.penrose.event.*;
import org.safehaus.penrose.pipeline.PipelineAdapter;
import org.safehaus.penrose.pipeline.PipelineEvent;
import org.safehaus.penrose.mapping.Entry;
import org.apache.log4j.Logger;
import org.ietf.ldap.LDAPEntry;
import org.ietf.ldap.LDAPException;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class SessionHandler {

    Logger log = Logger.getLogger(getClass());

    public final static String STOPPED  = "STOPPED";
    public final static String STARTING = "STARTING";
    public final static String STARTED  = "STARTED";
    public final static String STOPPING = "STOPPING";

    private AddHandler addHandler;
    private BindHandler bindHandler;
    private CompareHandler compareHandler;
    private DeleteHandler deleteHandler;
    private ModifyHandler modifyHandler;
    private ModRdnHandler modRdnHandler;
    private SearchHandler searchHandler;

    private SchemaManager schemaManager;
    private Engine engine;

    private SessionManager sessionManager;
    private PartitionManager partitionManager;
    private ModuleManager moduleManager;

    private UserConfig rootUserConfig;

    private InterpreterFactory interpreterFactory;
    private ACLEngine aclEngine;
    private FilterTool filterTool;

    private String status = STOPPED;

    public SessionHandler() throws Exception {

        addHandler = new AddHandler(this);
        bindHandler = new BindHandler(this);
        compareHandler = new CompareHandler(this);
        deleteHandler = new DeleteHandler(this);
        modifyHandler = new ModifyHandler(this);
        modRdnHandler = new ModRdnHandler(this);
        searchHandler = new SearchHandler(this);

        aclEngine = new ACLEngine(this);
    }

    public void start() throws Exception {

        if (status != STOPPED) return;

        //log.debug("Starting SessionHandler...");

        try {
            status = STARTING;

            filterTool = new FilterTool();
            filterTool.setSchemaManager(schemaManager);

            initSessionManager();

            status = STARTED;

            //log.debug("SessionHandler started.");

        } catch (Exception e) {
            status = STOPPED;
            log.debug(e.getMessage(), e);
            throw e;
        }
    }

    public void initSessionManager() throws Exception {
        sessionManager.start();
    }

    public void stop() throws Exception {

        if (status != STARTED) return;

        try {
            status = STOPPING;

            sessionManager.stop();

        } catch (Exception e) {
            log.debug(e.getMessage(), e);
        }

        status = STOPPED;
    }

    public int add(PenroseSession session, LDAPEntry entry) throws Exception {
        if (!sessionManager.isValid(session)) throw new Exception("Invalid session.");
        return getAddHandler().add(session, entry);
    }

    public int bind(PenroseSession session, String dn, String password) throws Exception {
        if (!sessionManager.isValid(session)) throw new Exception("Invalid session.");
        return getBindHandler().bind(session, dn, password);
    }

    public int compare(PenroseSession session, String dn, String attributeName,
            String attributeValue) throws Exception {

        if (!sessionManager.isValid(session)) throw new Exception("Invalid session.");
        return getCompareHandler().compare(session, dn, attributeName, attributeValue);
    }

    public int unbind(PenroseSession session) throws Exception {
        if (!sessionManager.isValid(session)) throw new Exception("Invalid session.");
        return getBindHandler().unbind(session);
    }

    public int delete(PenroseSession session, String dn) throws Exception {
        if (!sessionManager.isValid(session)) throw new Exception("Invalid session.");
        return getDeleteHandler().delete(session, dn);
    }

    public int modify(PenroseSession session, String dn, Collection modifications) throws Exception {
        if (!sessionManager.isValid(session)) throw new Exception("Invalid session.");
        return getModifyHandler().modify(session, dn, modifications);
    }

    public int modrdn(PenroseSession session, String dn, String newRdn) throws Exception {
        if (!sessionManager.isValid(session)) throw new Exception("Invalid session.");
        return getModRdnHandler().modrdn(session, dn, newRdn);
    }

    /**
     *
     * @param session
     * @param base
     * @param scope
     * @param deref
     * @param filter
     * @param attributeNames
     * @return LDAPEntry
     * @throws Exception
     */
    public PenroseSearchResults search(
            final PenroseSession session,
            final String base,
            final int scope,
            final int deref,
            final String filter,
            final Collection attributeNames)
            throws Exception {

        if (!sessionManager.isValid(session)) throw new Exception("Invalid session.");
        final PenroseSearchResults results = new PenroseSearchResults();

        final Collection normalizedAttributeNames = attributeNames == null ? null : new HashSet();
        if (attributeNames != null) {
            for (Iterator i=attributeNames.iterator(); i.hasNext(); ) {
                String attributeName = (String)i.next();
                normalizedAttributeNames.add(attributeName.toLowerCase());
            }
        }

        //getSearchHandler().search(connection, base, scope, deref, filter, attributeNames, results);

        engine.execute(new Runnable() {
            public void run() {
                try {
                    final PenroseSearchResults sr = new PenroseSearchResults();

                    sr.addListener(new PipelineAdapter() {
                        public void objectAdded(PipelineEvent event) {
                            try {
                                Entry entry = (Entry)event.getObject();

                                LDAPEntry ldapEntry = entry.toLDAPEntry();
                                Entry.filterAttributes(ldapEntry, normalizedAttributeNames);

                                results.add(ldapEntry);

                            } catch (Exception e) {
                                log.error(e.getMessage(), e);
                            }
                        }

                        public void pipelineClosed(PipelineEvent event) {
                            results.setReturnCode(sr.getReturnCode());
                            results.close();
                        }
                    });

                    getSearchHandler().search(session, base, scope, deref, filter, normalizedAttributeNames, sr);

                } catch (Throwable e) {
                    log.debug(e.getMessage(), e);
                    results.setReturnCode(LDAPException.OPERATIONS_ERROR);
                    results.close();
                }
            }
        });

        return results;
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

    public void postEvent(String dn, Event event) throws Exception {

        Collection c = moduleManager.getModules(dn);

        for (Iterator i=c.iterator(); i.hasNext(); ) {
            Module module = (Module)i.next();

            if (event instanceof AddEvent) {
                switch (event.getType()) {
                    case AddEvent.BEFORE_ADD:
                        module.beforeAdd((AddEvent)event);
                        break;

                    case AddEvent.AFTER_ADD:
                        module.afterAdd((AddEvent)event);
                        break;
                }

            } else if (event instanceof BindEvent) {

                switch (event.getType()) {
                    case BindEvent.BEFORE_BIND:
                        module.beforeBind((BindEvent)event);
                        break;

                    case BindEvent.AFTER_BIND:
                        module.afterBind((BindEvent)event);
                        break;
                }

            } else if (event instanceof DeleteEvent) {

                switch (event.getType()) {
                    case DeleteEvent.BEFORE_DELETE:
                        module.beforeDelete((DeleteEvent)event);
                        break;

                    case DeleteEvent.AFTER_DELETE:
                        module.afterDelete((DeleteEvent)event);
                        break;
                }

            } else if (event instanceof ModifyEvent) {

                switch (event.getType()) {
                case ModifyEvent.BEFORE_MODIFY:
                    module.beforeModify((ModifyEvent)event);
                    break;

                case ModifyEvent.AFTER_MODIFY:
                    module.afterModify((ModifyEvent)event);
                    break;
                }

            } else if (event instanceof SearchEvent) {

                switch (event.getType()) {
                    case SearchEvent.BEFORE_SEARCH:
                        module.beforeSearch((SearchEvent)event);
                        break;

                    case SearchEvent.AFTER_SEARCH:
                        module.afterSearch((SearchEvent)event);
                        break;
                }

            }
        }
    }

    public InterpreterFactory getInterpreterFactory() {
        return interpreterFactory;
    }

    public void setInterpreterFactory(InterpreterFactory interpreterFactory) {
        this.interpreterFactory = interpreterFactory;
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
    }

    public UserConfig getRootUserConfig() {
        return rootUserConfig;
    }

    public void setRootUserConfig(UserConfig rootUserConfig) {
        this.rootUserConfig = rootUserConfig;
    }

    public SessionManager getSessionManager() {
        return sessionManager;
    }

    public void setSessionManager(SessionManager sessionManager) {
        this.sessionManager = sessionManager;
    }

    public PenroseSession newSession() throws Exception {
        if (status != STARTED) return null;
        PenroseSession session = sessionManager.newSession();
        session.setHandler(this);
        return session;
    }

    public void closeSession(PenroseSession session) {
        sessionManager.closeSession(session);
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public ModuleManager getModuleManager() {
        return moduleManager;
    }

    public void setModuleManager(ModuleManager moduleManager) {
        this.moduleManager = moduleManager;
    }
}

