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
import org.safehaus.penrose.schema.AttributeType;
import org.safehaus.penrose.engine.Engine;
import org.safehaus.penrose.interpreter.InterpreterManager;
import org.safehaus.penrose.module.Module;
import org.safehaus.penrose.module.ModuleMapping;
import org.safehaus.penrose.module.ModuleConfig;
import org.safehaus.penrose.module.ModuleManager;
import org.safehaus.penrose.event.*;
import org.safehaus.penrose.pipeline.PipelineAdapter;
import org.safehaus.penrose.pipeline.PipelineEvent;
import org.safehaus.penrose.mapping.Entry;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.util.EntryUtil;
import org.safehaus.penrose.config.PenroseConfig;
import org.apache.log4j.Logger;
import org.ietf.ldap.LDAPEntry;
import org.ietf.ldap.LDAPException;
import org.ietf.ldap.LDAPModification;
import org.ietf.ldap.LDAPAttribute;

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
    private ModuleManager moduleManager;

    private UserConfig rootUserConfig;

    private InterpreterManager interpreterManager;
    private ACLEngine aclEngine = new ACLEngine(this);
    private FilterTool filterTool;

    private String status = STOPPED;

    public Handler() {
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

    public int add(PenroseSession session, LDAPEntry ldapEntry) throws Exception {
        if (!sessionManager.isValid(session)) throw new Exception("Invalid session.");

        AddEvent beforeModifyEvent = new AddEvent(this, AddEvent.BEFORE_ADD, session, ldapEntry);
        postEvent(ldapEntry.getDN(), beforeModifyEvent);

        int rc = getAddHandler().add(session, ldapEntry);

        AddEvent afterModifyEvent = new AddEvent(this, AddEvent.AFTER_ADD, session, ldapEntry);
        afterModifyEvent.setReturnCode(rc);
        postEvent(ldapEntry.getDN(), afterModifyEvent);

        return rc;
    }

    public int bind(PenroseSession session, String dn, String password) throws Exception {
        if (!sessionManager.isValid(session)) throw new Exception("Invalid session.");

        BindEvent beforeBindEvent = new BindEvent(this, BindEvent.BEFORE_BIND, session, dn, password);
        postEvent(dn, beforeBindEvent);

        int rc = getBindHandler().bind(session, dn, password);

        BindEvent afterBindEvent = new BindEvent(this, BindEvent.AFTER_BIND, session, dn, password);
        afterBindEvent.setReturnCode(rc);
        postEvent(dn, afterBindEvent);

        return rc;
    }

    public int compare(PenroseSession session, String dn, String attributeName,
            Object attributeValue) throws Exception {

        if (!sessionManager.isValid(session)) throw new Exception("Invalid session.");
        return getCompareHandler().compare(session, dn, attributeName, attributeValue);
    }

    public int unbind(PenroseSession session) throws Exception {
        if (!sessionManager.isValid(session)) throw new Exception("Invalid session.");
        return getBindHandler().unbind(session);
    }

    public int delete(PenroseSession session, String dn) throws Exception {
        if (!sessionManager.isValid(session)) throw new Exception("Invalid session.");

        DeleteEvent beforeDeleteEvent = new DeleteEvent(this, DeleteEvent.BEFORE_DELETE, session, dn);
        postEvent(dn, beforeDeleteEvent);

        int rc = getDeleteHandler().delete(session, dn);

        DeleteEvent afterDeleteEvent = new DeleteEvent(this, DeleteEvent.AFTER_DELETE, session, dn);
        afterDeleteEvent.setReturnCode(rc);
        postEvent(dn, afterDeleteEvent);

        return rc;
    }

    public int modify(PenroseSession session, String dn, Collection modifications) throws Exception {
        if (!sessionManager.isValid(session)) throw new Exception("Invalid session.");

        Collection normalizedModifications = new ArrayList();

		for (Iterator i = modifications.iterator(); i.hasNext();) {
			LDAPModification modification = (LDAPModification) i.next();

			LDAPAttribute attribute = modification.getAttribute();
			String attributeName = attribute.getName();
            String values[] = attribute.getStringValueArray();

            AttributeType at = schemaManager.getAttributeType(attributeName);
            if (at == null) return LDAPException.UNDEFINED_ATTRIBUTE_TYPE;

            attributeName = at.getName();
            LDAPAttribute normalizedAttribute = new LDAPAttribute(attributeName, values);

            LDAPModification normalizedModification = new LDAPModification(modification.getOp(), normalizedAttribute);

            normalizedModifications.add(normalizedModification);

			switch (modification.getOp()) {
			case LDAPModification.ADD:
				log.debug("add: " + attributeName);
				for (int j = 0; j < values.length; j++)
					log.debug(attributeName + ": " + values[j]);
				break;
			case LDAPModification.DELETE:
				log.debug("delete: " + attributeName);
				for (int j = 0; j < values.length; j++)
					log.debug(attributeName + ": " + values[j]);
				break;
			case LDAPModification.REPLACE:
				log.debug("replace: " + attributeName);
				for (int j = 0; j < values.length; j++)
					log.debug(attributeName + ": " + values[j]);
				break;
			}
			log.debug("-");
		}

        log.info("");

        ModifyEvent beforeModifyEvent = new ModifyEvent(this, ModifyEvent.BEFORE_MODIFY, session, dn, normalizedModifications);
        postEvent(dn, beforeModifyEvent);

        int rc = getModifyHandler().modify(session, dn, normalizedModifications);

        ModifyEvent afterModifyEvent = new ModifyEvent(this, ModifyEvent.AFTER_MODIFY, session, dn, normalizedModifications);
        afterModifyEvent.setReturnCode(rc);
        postEvent(dn, afterModifyEvent);

        return rc;
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

        SearchEvent beforeSearchEvent = new SearchEvent(this, SearchEvent.BEFORE_SEARCH, session, base);
        postEvent(base, beforeSearchEvent);

        //getSearchHandler().search(connection, base, scope, deref, filter, attributeNames, results);

        engine.execute(new Runnable() {
            public void run() {
                try {
                    final PenroseSearchResults sr = new PenroseSearchResults();

                    sr.addListener(new PipelineAdapter() {
                        public void objectAdded(PipelineEvent event) {
                            try {
                                Entry entry = (Entry)event.getObject();
                                // String dn = schemaManager.normalize(entry.getDn());
                                // EntryMapping entryMapping = entry.getEntryMapping();
                                // LDAPEntry ldapEntry = entry.toLDAPEntry();

                                //EntryUtil.filterAttributes(ldapEntry, normalizedAttributeNames);
                                LDAPEntry ldapEntry = aclEngine.filterAttributes(session, entry);

                                results.add(ldapEntry);

                            } catch (Exception e) {
                                log.error(e.getMessage(), e);
                            }
                        }

                        public void pipelineClosed(PipelineEvent event) {
                            results.setReturnCode(sr.getReturnCode());
                            results.close();

                            try {
                                SearchEvent afterSearchEvent = new SearchEvent(this, SearchEvent.AFTER_SEARCH, session, base);
                                afterSearchEvent.setReturnCode(sr.getReturnCode());
                                postEvent(base, afterSearchEvent);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
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
    }
}

