/**
 * Copyright (c) 2000-2006, Identyx Corporation.
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
package org.safehaus.penrose.session;

import org.safehaus.penrose.handler.Handler;
import org.safehaus.penrose.handler.HandlerManager;
import org.safehaus.penrose.event.*;
import org.safehaus.penrose.pipeline.PipelineAdapter;
import org.safehaus.penrose.pipeline.PipelineEvent;
import org.safehaus.penrose.entry.DN;
import org.safehaus.penrose.entry.RDN;
import org.safehaus.penrose.schema.SchemaManager;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.util.ExceptionUtil;
import org.safehaus.penrose.util.PasswordUtil;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.Penrose;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.ietf.ldap.LDAPException;

import javax.naming.directory.Attributes;
import java.util.Date;
import java.util.Collection;
import java.util.Map;
import java.util.HashMap;

/**
 * @author Endi S. Dewata
 */
public class PenroseSession {

    Logger log = LoggerFactory.getLogger(getClass());

    private Penrose penrose;
    private SessionManager sessionManager;
    private EventManager eventManager;
    private SchemaManager schemaManager;
    private PartitionManager partitionManager;
    private HandlerManager handlerManager;

    private String sessionId;

    private DN bindDn;
    private String bindPassword;
    private boolean rootUser;

    private Date createDate;
    private Date lastActivityDate;

    private Map attributes = new HashMap();
    
    boolean enableEventListeners = true;

    public PenroseSession(SessionManager sessionManager) {
        this.sessionManager = sessionManager;

        createDate = new Date();
        lastActivityDate = (Date)createDate.clone();
    }

    public DN getBindDn() {
        return bindDn;
    }

    public void setBindDn(String bindDn) {
        this.bindDn = new DN(bindDn);
    }

    public void setBindDn(DN bindDn) {
        this.bindDn = bindDn;
    }

    public Date getLastActivityDate() {
        return lastActivityDate;
    }

    public void setLastActivityDate(Date lastActivityDate) {
        this.lastActivityDate = lastActivityDate;
    }

    public boolean isValid() {
    	if (sessionManager.isExpired(this))
    	{
    		return sessionManager.isValid(this);
    	}
    	return true;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // ADD
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void add(String dn, Attributes attributes) throws LDAPException {
        add(new DN(dn), attributes);
    }

    public void add(DN dn, Attributes attributes) throws LDAPException {

        Partition partition = null;

        try {
            partition = partitionManager.getPartition(dn);

            if (partition == null) {
                log.debug("Partition for entry "+dn+" not found.");
                throw ExceptionUtil.createLDAPException(LDAPException.NO_SUCH_OBJECT);
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionUtil.createLDAPException(e);
        }

        add(partition, dn, attributes);
    }

    public void add(Partition partition, DN dn, Attributes attributes) throws LDAPException {
        try {
            if (!isValid()) throw new Exception("Invalid session.");

            lastActivityDate.setTime(System.currentTimeMillis());
            
            if (enableEventListeners) {
            	AddEvent beforeModifyEvent = new AddEvent(this, AddEvent.BEFORE_ADD, this, dn, attributes);
            	boolean b = eventManager.postEvent(dn, beforeModifyEvent);

            	if (!b) {
            		throw ExceptionUtil.createLDAPException(LDAPException.UNWILLING_TO_PERFORM);
            	}
            }

            int rc = LDAPException.SUCCESS;
            try {
                Handler handler = handlerManager.getHandler(partition);
                handler.add(this, dn, attributes);
            } catch (LDAPException e) {
                rc = e.getResultCode();
                throw e;
            }
            finally {
                if (enableEventListeners) {
                	AddEvent afterModifyEvent = new AddEvent(this, AddEvent.AFTER_ADD, this, dn, attributes);
                	afterModifyEvent.setReturnCode(rc);
                	eventManager.postEvent(dn, afterModifyEvent);
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionUtil.createLDAPException(e);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // BIND
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void bind(String dn, String password) throws LDAPException {
    	bind(new DN(dn), password);
    }

    public void bind(DN dn, String password) throws LDAPException {
        try {
            if (!isValid()) throw new Exception("Invalid session.");

            lastActivityDate.setTime(System.currentTimeMillis());

            if (enableEventListeners) {
            	BindEvent beforeBindEvent = new BindEvent(this, BindEvent.BEFORE_BIND, this, dn, password);
            	boolean b = eventManager.postEvent(dn, beforeBindEvent);
            
            	if (!b) {
            		throw ExceptionUtil.createLDAPException(LDAPException.UNWILLING_TO_PERFORM);
            	}
        	}
            
            int rc = LDAPException.SUCCESS;
    		if (enableEventListeners) {        		
    			BindEvent afterBindEvent = new BindEvent(this, BindEvent.AFTER_BIND, this, dn, password);
    			afterBindEvent.setReturnCode(rc);
    			eventManager.postEvent(dn, afterBindEvent);
        	}

            PenroseConfig penroseConfig = penrose.getPenroseConfig();
            DN rootDn = penroseConfig.getRootDn();

            try {
                if (dn.matches(rootDn)) {
                    if (!PasswordUtil.comparePassword(password, penroseConfig.getRootPassword())) {
                        log.debug("Password doesn't match => BIND FAILED");
                        throw ExceptionUtil.createLDAPException(LDAPException.INVALID_CREDENTIALS);
                    }

                    log.debug("Bound as root user.");

                    rootUser = true;

                } else {
                    Partition partition = partitionManager.getPartition(dn);

                    if (partition == null) {
                        log.debug("Partition for entry "+dn+" not found.");
                        throw ExceptionUtil.createLDAPException(LDAPException.NO_SUCH_OBJECT);
                    }

                    Handler handler = handlerManager.getHandler(partition);
                    handler.bind(this, dn, password);

                    rootUser = false;
                }

                bindDn = dn;
                bindPassword = password;

            } catch (LDAPException e) {
                rc = e.getResultCode();
                throw e;
            } finally {
                if (enableEventListeners) {
                	BindEvent afterBindEvent = new BindEvent(this, BindEvent.AFTER_BIND, this, dn, password);
                	afterBindEvent.setReturnCode(rc);
                	eventManager.postEvent(dn, afterBindEvent);
                }
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionUtil.createLDAPException(e);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // COMPARE
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public boolean compare(String dn, String attributeName, Object attributeValue) throws LDAPException {
        return compare(new DN(dn), attributeName, attributeValue);
    }

    public boolean compare(DN dn, String attributeName, Object attributeValue) throws LDAPException {
        Partition partition = null;

        try {
            partition = partitionManager.getPartition(dn);

            if (partition == null) {
                log.debug("Partition for entry "+dn+" not found.");
                throw ExceptionUtil.createLDAPException(LDAPException.NO_SUCH_OBJECT);
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionUtil.createLDAPException(e);
        }

        return compare(partition, dn, attributeName, attributeValue);
    }

    public boolean compare(Partition partition, DN dn, String attributeName, Object attributeValue) throws LDAPException {
        try {
            if (!isValid()) throw new Exception("Invalid session.");

            lastActivityDate.setTime(System.currentTimeMillis());

            if (enableEventListeners) {
            	CompareEvent beforeCompareEvent = new CompareEvent(this, CompareEvent.BEFORE_COMPARE, this, dn, attributeName, attributeValue);
            	boolean b = eventManager.postEvent(dn, beforeCompareEvent);

            	if (!b) {
            		throw ExceptionUtil.createLDAPException(LDAPException.UNWILLING_TO_PERFORM);
            	}
            }

            int rc = LDAPException.SUCCESS;
            boolean result = false;
            try {
                Handler handler = handlerManager.getHandler(partition);
                result = handler.compare(this, dn, attributeName, attributeValue);

            } catch (LDAPException e) {
                rc = e.getResultCode();
                throw e;
            }
            finally {
                if (enableEventListeners) {
                	CompareEvent afterCompareEvent = new CompareEvent(this, CompareEvent.AFTER_COMPARE, this, dn, attributeName, attributeValue);
                	afterCompareEvent.setReturnCode(rc);
                	eventManager.postEvent(dn, afterCompareEvent);
                }
            }
            return result;

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionUtil.createLDAPException(e);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // DELETE
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void delete(String dn) throws LDAPException {
        delete(new DN(dn));
    }

    public void delete(DN dn) throws LDAPException {

        Partition partition = null;

        try {
            partition = partitionManager.getPartition(dn);

            if (partition == null) {
                log.debug("Partition for entry "+dn+" not found.");
                throw ExceptionUtil.createLDAPException(LDAPException.NO_SUCH_OBJECT);
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionUtil.createLDAPException(e);
        }

        delete(partition, dn);
    }

    public void delete(Partition partition, DN dn) throws LDAPException {
        try {
            if (!isValid()) throw new Exception("Invalid session.");

            lastActivityDate.setTime(System.currentTimeMillis());

            if (enableEventListeners) {
            	DeleteEvent beforeDeleteEvent = new DeleteEvent(this, DeleteEvent.BEFORE_DELETE, this, dn);
            	boolean b = eventManager.postEvent(dn, beforeDeleteEvent);

            	if (!b) {
            		throw ExceptionUtil.createLDAPException(LDAPException.UNWILLING_TO_PERFORM);
            	}
            }
            
            int rc = LDAPException.SUCCESS;
            try {
                Handler handler = handlerManager.getHandler(partition);
                handler.delete(this, dn);
            } catch (LDAPException e) {
                rc = e.getResultCode();
                throw e;
            }
            finally {
                if (enableEventListeners) {
                	DeleteEvent afterDeleteEvent = new DeleteEvent(this, DeleteEvent.AFTER_DELETE, this, dn);
                	afterDeleteEvent.setReturnCode(rc);
                	eventManager.postEvent(dn, afterDeleteEvent);
                }
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionUtil.createLDAPException(e);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // MODIFY
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modify(String dn, Collection modifications) throws LDAPException {
        modify(new DN(dn), modifications);
    }

    public void modify(DN dn, Collection modifications) throws LDAPException {

        Partition partition = null;

        try {
            partition = partitionManager.getPartition(dn);

            if (partition == null) {
                log.debug("Partition for entry "+dn+" not found.");
                throw ExceptionUtil.createLDAPException(LDAPException.NO_SUCH_OBJECT);
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionUtil.createLDAPException(e);
        }

        modify(partition, dn, modifications);
    }

    public void modify(Partition partition, DN dn, Collection modifications) throws LDAPException {
        try {
            if (!isValid()) throw new Exception("Invalid session.");

            lastActivityDate.setTime(System.currentTimeMillis());
            if (enableEventListeners) {
            	ModifyEvent beforeModifyEvent = new ModifyEvent(this, ModifyEvent.BEFORE_MODIFY, this, dn, modifications);
            	boolean b = eventManager.postEvent(dn, beforeModifyEvent);

            	if (!b) {
            		throw ExceptionUtil.createLDAPException(LDAPException.UNWILLING_TO_PERFORM);
            	}
            }

            int rc = LDAPException.SUCCESS;
            try {
                Handler handler = handlerManager.getHandler(partition);
                handler.modify(this, dn, modifications);
            } catch (LDAPException e) {
                rc = e.getResultCode();
                throw e;
            }
            finally {
                if (enableEventListeners) {
                	ModifyEvent afterModifyEvent = new ModifyEvent(this, ModifyEvent.AFTER_MODIFY, this, dn, modifications);
                	afterModifyEvent.setReturnCode(rc);
                	eventManager.postEvent(dn, afterModifyEvent);
                }
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionUtil.createLDAPException(e);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // MODRDN
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modrdn(String dn, String newRdn, boolean deleteOldRdn) throws LDAPException {
        modrdn(new DN(dn), new RDN(newRdn), deleteOldRdn);
    }

    public void modrdn(DN dn, RDN newRdn, boolean deleteOldRdn) throws LDAPException {

        Partition partition = null;

        try {
            partition = partitionManager.getPartition(dn);

            if (partition == null) {
                log.debug("Partition for entry "+dn+" not found.");
                throw ExceptionUtil.createLDAPException(LDAPException.NO_SUCH_OBJECT);
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionUtil.createLDAPException(e);
        }

        modrdn(partition, dn, newRdn, deleteOldRdn);
    }

    public void modrdn(Partition partition, DN dn, RDN newRdn, boolean deleteOldRdn) throws LDAPException {
        try {
            if (!isValid()) throw new Exception("Invalid session.");

            lastActivityDate.setTime(System.currentTimeMillis());
            if (enableEventListeners) {
            	ModRdnEvent beforeModRdnEvent = new ModRdnEvent(this, ModRdnEvent.BEFORE_MODRDN, this, dn, newRdn, deleteOldRdn);
	            boolean b = eventManager.postEvent(dn, beforeModRdnEvent);
	
	            if (!b) {
	                throw ExceptionUtil.createLDAPException(LDAPException.UNWILLING_TO_PERFORM);
	            }
            }

            int rc = LDAPException.SUCCESS;
            try {
                Handler handler = handlerManager.getHandler(partition);
                handler.modrdn(this, dn, newRdn, deleteOldRdn);
            } catch (LDAPException e) {
                rc = e.getResultCode();
                throw e;
            }
            finally {
                if (enableEventListeners) {
                    ModRdnEvent afterModRdnEvent = new ModRdnEvent(this, ModRdnEvent.AFTER_MODRDN, this, dn, newRdn, deleteOldRdn);
                    afterModRdnEvent.setReturnCode(rc);
                    eventManager.postEvent(dn, afterModRdnEvent);
                }
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionUtil.createLDAPException(e);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // SEARCH
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Asynchronous search.
     * @param baseDn
     * @param filter
     * @param sc
     * @param results This will be filled with objects of type SearchResult.
     * @return return code
     * @throws Exception
     */
    public void search(
            String baseDn,
            String filter,
            PenroseSearchControls sc,
            PenroseSearchResults results
    ) throws LDAPException {
        search(new DN(baseDn), filter, sc, results);
    }

    public void search(
            DN baseDn,
            String filter,
            PenroseSearchControls sc,
            PenroseSearchResults results
    ) throws LDAPException {

        Partition partition = null;

        try {
            partition = partitionManager.getPartition(baseDn);

            if (partition == null && !baseDn.isEmpty()) {
                log.debug("Partition for entry "+baseDn+" not found.");
                throw ExceptionUtil.createLDAPException(LDAPException.NO_SUCH_OBJECT);
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionUtil.createLDAPException(e);
        }

        search(partition, baseDn, filter, sc, results);
    }

    public void search(
            final Partition partition,
            final DN baseDn,
            final String filter,
            final PenroseSearchControls sc,
            final PenroseSearchResults results
    ) throws LDAPException {

        try {
            if (!isValid()) throw new Exception("Invalid session.");

            lastActivityDate.setTime(System.currentTimeMillis());

            //results.setEnableEventListeners(enableEventListeners);

            SearchEvent beforeSearchEvent = null;
            if (enableEventListeners)
            {
	 			beforeSearchEvent = new SearchEvent(this, SearchEvent.BEFORE_SEARCH, this, baseDn, filter, sc, results);
	           	boolean b = eventManager.postEvent(baseDn, beforeSearchEvent);

	            if (!b) {
	                results.close();
	                throw ExceptionUtil.createLDAPException(LDAPException.UNWILLING_TO_PERFORM);
	            }
            }

            final DN newBaseDn = beforeSearchEvent.getBaseDn();
            final String newFilter = beforeSearchEvent.getFilter();
            final PenroseSearchControls newSc = beforeSearchEvent.getSearchControls();

            final PenroseSession session = this;

            results.addListener(new PipelineAdapter() {
                public void pipelineClosed(PipelineEvent event) {
                    try {
                        lastActivityDate.setTime(System.currentTimeMillis());

                        SearchEvent afterSearchEvent = new SearchEvent(session, SearchEvent.AFTER_SEARCH, session, newBaseDn, newFilter, newSc, results);
                        afterSearchEvent.setReturnCode(results.getReturnCode());
                        eventManager.postEvent(newBaseDn, afterSearchEvent);

                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }
                }
            });

            Handler handler = handlerManager.getHandler(partition);
            handler.search(this, newBaseDn, newFilter, newSc, results);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionUtil.createLDAPException(e);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // UNBIND
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void unbind() throws LDAPException {
        try {
            if (!isValid()) throw new Exception("Invalid session.");

            lastActivityDate.setTime(System.currentTimeMillis());
            if (enableEventListeners) {
            	BindEvent beforeUnbindEvent = new BindEvent(this, BindEvent.BEFORE_UNBIND, this, bindDn);
	            boolean b = eventManager.postEvent(bindDn, beforeUnbindEvent);

	            if (!b) {
	                throw ExceptionUtil.createLDAPException(LDAPException.UNWILLING_TO_PERFORM);
	            }
            }

            int rc = LDAPException.SUCCESS;
            try {
                if (!rootUser && bindDn != null) {
                    Partition partition = partitionManager.getPartition(bindDn);

                    if (partition == null) {
                        log.debug("Partition for entry "+bindDn+" not found.");
                        throw ExceptionUtil.createLDAPException(LDAPException.NO_SUCH_OBJECT);
                    }

                    Handler handler = handlerManager.getHandler(partition);
                    handler.unbind(this);
                }

                rootUser = false;
                bindDn = null;
                bindPassword = null;

            } catch (LDAPException e) {
                rc = e.getResultCode();
                throw e;
            }
            finally {
                if (enableEventListeners) {
	                BindEvent afterUnbindEvent = new BindEvent(this, BindEvent.AFTER_UNBIND, this, bindDn);
	                afterUnbindEvent.setReturnCode(rc);
	                eventManager.postEvent(bindDn, afterUnbindEvent);
                }
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionUtil.createLDAPException(e);
        }
    }

    public void close() throws Exception {
        sessionManager.closeSession(this);
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public Date getCreateDate() {
        return createDate;
    }

    public void setCreateDate(Date createDate) {
        this.createDate = createDate;
    }

    public SessionManager getSessionManager() {
        return sessionManager;
    }

    public void setSessionManager(SessionManager sessionManager) {
        this.sessionManager = sessionManager;
    }

    public EventManager getEventManager() {
        return eventManager;
    }

    public void setEventManager(EventManager eventManager) {
        this.eventManager = eventManager;
    }

    public void addAddListener(AddListener listener) {
        eventManager.addAddListener(listener);
    }

    public void removeAddListener(AddListener listener) {
        eventManager.removeAddListener(listener);
    }

    public void addBindListener(BindListener listener) {
        eventManager.addBindListener(listener);
    }

    public void removeBindListener(BindListener listener) {
        eventManager.removeBindListener(listener);
    }

    public void addCompareListener(CompareListener listener) {
        eventManager.addCompareListener(listener);
    }

    public void removeCompareListener(CompareListener listener) {
        eventManager.removeCompareListener(listener);
    }

    public void addDeleteListener(DeleteListener listener) {
        eventManager.addDeleteListener(listener);
    }

    public void removeDeleteListener(DeleteListener listener) {
        eventManager.removeDeleteListener(listener);
    }

    public void addModifyListener(ModifyListener listener) {
        eventManager.addModifyListener(listener);
    }

    public void removeModifyListener(ModifyListener listener) {
        eventManager.removeModifyListener(listener);
    }

    public void addModrdnListener(ModRdnListener listener) {
        eventManager.addModRdnListener(listener);
    }

    public void removeModrdnListener(ModRdnListener listener) {
        eventManager.removeModRdnListener(listener);
    }

    public void addSearchListener(SearchListener listener) {
        eventManager.addSearchListener(listener);
    }

    public void removeSearchListener(SearchListener listener) {
        eventManager.removeSearchListener(listener);
    }

    public Collection getAttributes() {
        return attributes.values();
    }

    public void setAttribute(String name, Object value) {
        attributes.put(name, value);
    }

    public Object getAttribute(String name) {
        return attributes.get(name);
    }

    public Collection getAttributeNames() {
        return attributes.keySet();
    }

    public String getBindPassword() {
        return bindPassword;
    }

    public void setBindPassword(String bindPassword) {
        this.bindPassword = bindPassword;
    }

    public SchemaManager getSchemaManager() {
        return schemaManager;
    }

    public void setSchemaManager(SchemaManager schemaManager) {
        this.schemaManager = schemaManager;
    }

    public PartitionManager getPartitionManager() {
        return partitionManager;
    }

    public void setPartitionManager(PartitionManager partitionManager) {
        this.partitionManager = partitionManager;
    }

    public boolean isRootUser() {
        return rootUser;
    }

    public void setRootUser(boolean rootUser) {
        this.rootUser = rootUser;
    }

    public HandlerManager getHandlerManager() {
        return handlerManager;
    }

    public void setHandlerManager(HandlerManager handlerManager) {
        this.handlerManager = handlerManager;
    }

    public Penrose getPenrose() {
        return penrose;
    }

    public void setPenrose(Penrose penrose) {
        this.penrose = penrose;
        String s = (String)penrose.getPenroseConfig().getProperty("enableEventListeners");
        enableEventListeners = s == null ? true : new Boolean(s).booleanValue();

    }
}
