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

import org.safehaus.penrose.handler.HandlerManager;
import org.safehaus.penrose.event.*;
import org.safehaus.penrose.entry.DN;
import org.safehaus.penrose.entry.RDN;
import org.safehaus.penrose.entry.Attributes;
import org.safehaus.penrose.entry.Attribute;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.util.ExceptionUtil;
import org.safehaus.penrose.util.PasswordUtil;
import org.safehaus.penrose.util.BinaryUtil;
import org.safehaus.penrose.util.LDAPUtil;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.FilterTool;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.ietf.ldap.LDAPException;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class Session {

    Logger log = LoggerFactory.getLogger(getClass());

    private PenroseConfig penroseConfig;
    private PenroseContext penroseContext;

    private SessionManager sessionManager;
    private EventManager eventManager;
    private HandlerManager handlerManager;

    private Object sessionId;

    private DN bindDn;
    private String bindPassword;
    private boolean rootUser;

    private Date createDate;
    private Date lastActivityDate;

    private Map attributes = new HashMap();
    
    boolean enableEventListeners = true;

    public Session(SessionManager sessionManager) {
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
    	if (sessionManager.isExpired(this)) {
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
        try {
            AddRequest request = new AddRequest();
            request.setDn(dn);
            request.setAttributes(attributes);

            AddResponse response = new AddResponse();

            add(request, response);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionUtil.createLDAPException(e);
        }
    }
    
    public void add(AddRequest request, AddResponse response) throws LDAPException {
        try {
            DN dn = request.getDn();

            PartitionManager partitionManager = penroseContext.getPartitionManager();
            Partition partition = partitionManager.getPartition(dn);

            if (partition == null) {
                log.debug("Partition for entry "+dn+" not found.");
                throw ExceptionUtil.createLDAPException(LDAPException.NO_SUCH_OBJECT);
            }

            add(partition, request, response);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionUtil.createLDAPException(e);
        }
    }

    public void add(Partition partition, AddRequest request, AddResponse response) throws LDAPException {
        try {
            if (!isValid()) throw new Exception("Invalid session.");

            lastActivityDate.setTime(System.currentTimeMillis());
            
            DN dn = request.getDn();

            if (log.isWarnEnabled()) {
                log.warn("Add entry \""+dn+"\".");
            }

            boolean debug = log.isDebugEnabled();

            if (debug) {
                log.debug("----------------------------------------------------------------------------------");
                log.debug("ADD:");
                log.debug(" - Bind DN : "+bindDn);
                log.debug(" - Entry   : "+dn);
                log.debug("");

                log.debug("Controls: "+request.getControls());
            }

            if (enableEventListeners) {
            	AddEvent beforeModifyEvent = new AddEvent(this, AddEvent.BEFORE_ADD, this, request, response);
            	boolean b = eventManager.postEvent(beforeModifyEvent);

            	if (!b) {
            		throw ExceptionUtil.createLDAPException(LDAPException.UNWILLING_TO_PERFORM);
            	}
            }

            int rc = LDAPException.SUCCESS;
            try {
                handlerManager.add(this, partition, request, response);
            } catch (LDAPException e) {
                rc = e.getResultCode();
                throw e;
            } finally {
                if (enableEventListeners) {
                	AddEvent afterModifyEvent = new AddEvent(this, AddEvent.AFTER_ADD, this, request, response);
                	afterModifyEvent.setReturnCode(rc);
                	eventManager.postEvent(afterModifyEvent);
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
        BindRequest request = new BindRequest();
        request.setDn(dn);
        request.setPassword(password);

        BindResponse response = new BindResponse();

        bind(request, response);
    }

    public void bind(BindRequest request, BindResponse response) throws LDAPException {
        try {
            if (!isValid()) throw new Exception("Invalid session.");

            lastActivityDate.setTime(System.currentTimeMillis());

            DN dn = request.getDn();
            String password = request.getPassword();

            boolean debug = log.isDebugEnabled();

            if (debug) {
                log.debug("----------------------------------------------------------------------------------");
                log.debug("BIND:");
                log.debug(" - Bind DN       : "+dn);
                log.debug(" - Bind Password : "+password);
                log.debug("");

                log.debug("Controls: "+request.getControls());
            }

            if (enableEventListeners) {
            	BindEvent beforeBindEvent = new BindEvent(this, BindEvent.BEFORE_BIND, this, request, response);
            	boolean b = eventManager.postEvent(beforeBindEvent);
            
            	if (!b) {
            		throw ExceptionUtil.createLDAPException(LDAPException.UNWILLING_TO_PERFORM);
            	}
        	}
            
            int rc = LDAPException.SUCCESS;
            try {
                if (dn.isEmpty()) {
                    log.debug("Anonymous bind.");
                    return;
                }

                DN rootDn = penroseConfig.getRootDn();
                if (dn.matches(rootDn)) {
                    if (!PasswordUtil.comparePassword(password, penroseConfig.getRootPassword())) {
                        log.debug("Password doesn't match => BIND FAILED");
                        throw ExceptionUtil.createLDAPException(LDAPException.INVALID_CREDENTIALS);
                    }

                    log.debug("Bound as root user.");

                    rootUser = true;

                } else {
                    PartitionManager partitionManager = penroseContext.getPartitionManager();
                    Partition partition = partitionManager.getPartition(dn);

                    if (partition == null) {
                        log.debug("Partition for entry "+dn+" not found.");
                        throw ExceptionUtil.createLDAPException(LDAPException.NO_SUCH_OBJECT);
                    }

                    handlerManager.bind(this, partition, request, response);
                }

                bindDn = dn;
                bindPassword = password;

            } catch (LDAPException e) {
                rc = e.getResultCode();
                throw e;
            } finally {
                if (enableEventListeners) {
                	BindEvent afterBindEvent = new BindEvent(this, BindEvent.AFTER_BIND, this, request, response);
                	afterBindEvent.setReturnCode(rc);
                	eventManager.postEvent(afterBindEvent);
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
        CompareRequest request = new CompareRequest();
        request.setDn(dn);
        request.setAttributeName(attributeName);
        request.setAttributeValue(attributeValue);

        CompareResponse response = new CompareResponse();

        return compare(request, response);
    }

    public boolean compare(CompareRequest request, CompareResponse response) throws LDAPException {
        try {
            DN dn = request.getDn();

            PartitionManager partitionManager = penroseContext.getPartitionManager();
            Partition partition = partitionManager.getPartition(dn);

            if (partition == null) {
                log.debug("Partition for entry "+dn+" not found.");
                throw ExceptionUtil.createLDAPException(LDAPException.NO_SUCH_OBJECT);
            }

            return compare(partition, request, response);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionUtil.createLDAPException(e);
        }
    }

    public boolean compare(Partition partition, CompareRequest request, CompareResponse response) throws LDAPException {
        try {
            if (!isValid()) throw new Exception("Invalid session.");

            lastActivityDate.setTime(System.currentTimeMillis());

            DN dn = request.getDn();
            String attributeName = request.getAttributeName();
            Object attributeValue = request.getAttributeValue();

            if (log.isWarnEnabled()) {
                log.warn("Compare attribute "+attributeName+" in \""+dn+"\" with \""+attributeValue+"\".");
            }

            boolean debug = log.isDebugEnabled();

            if (debug) {
                log.debug("----------------------------------------------------------------------------------");
                log.debug("COMPARE:");
                log.debug(" - Bind DN         : "+bindDn);
                log.debug(" - DN              : "+dn);
                log.debug(" - Attribute Name  : "+attributeName);

                Object value = attributeValue;
                if (attributeValue instanceof byte[]) {
                    value = BinaryUtil.encode(BinaryUtil.BIG_INTEGER, (byte[])attributeValue);
                }

                log.debug(" - Attribute Value : "+value);
                log.debug("");

                log.debug("Controls: "+request.getControls());
            }

            if (enableEventListeners) {
            	CompareEvent beforeCompareEvent = new CompareEvent(this, CompareEvent.BEFORE_COMPARE, this, request, response);
            	boolean b = eventManager.postEvent(beforeCompareEvent);

            	if (!b) {
            		throw ExceptionUtil.createLDAPException(LDAPException.UNWILLING_TO_PERFORM);
            	}
            }

            int rc = LDAPException.SUCCESS;
            boolean result = false;
            try {
                result = handlerManager.compare(this, partition, request, response);

            } catch (LDAPException e) {
                rc = e.getResultCode();
                throw e;
            } finally {
                if (enableEventListeners) {
                	CompareEvent afterCompareEvent = new CompareEvent(this, CompareEvent.AFTER_COMPARE, this, request, response);
                	afterCompareEvent.setReturnCode(rc);
                	eventManager.postEvent(afterCompareEvent);
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
        DeleteRequest request = new DeleteRequest();
        request.setDn(dn);

        DeleteResponse response = new DeleteResponse();

        delete(request, response);
    }

    public void delete(DeleteRequest request, DeleteResponse response) throws LDAPException {
        try {
            DN dn = request.getDn();

            PartitionManager partitionManager = penroseContext.getPartitionManager();
            Partition partition = partitionManager.getPartition(dn);

            if (partition == null) {
                log.debug("Partition for entry "+dn+" not found.");
                throw ExceptionUtil.createLDAPException(LDAPException.NO_SUCH_OBJECT);
            }

            delete(partition, request, response);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionUtil.createLDAPException(e);
        }
    }

    public void delete(Partition partition, DeleteRequest request, DeleteResponse response) throws LDAPException {
        try {
            if (!isValid()) throw new Exception("Invalid session.");

            lastActivityDate.setTime(System.currentTimeMillis());

            DN dn = request.getDn();

            if (log.isWarnEnabled()) {
                log.warn("Delete entry \""+dn+"\".");
            }

            boolean debug = log.isDebugEnabled();

            if (debug) {
                log.debug("----------------------------------------------------------------------------------");
                log.debug("DELETE:");
                log.debug(" - Bind DN : "+bindDn);
                log.debug(" - DN      : "+dn);
                log.debug("");

                log.debug("Controls: "+request.getControls());
            }

            if (enableEventListeners) {
            	DeleteEvent beforeDeleteEvent = new DeleteEvent(this, DeleteEvent.BEFORE_DELETE, this, request, response);
            	boolean b = eventManager.postEvent(beforeDeleteEvent);

            	if (!b) {
            		throw ExceptionUtil.createLDAPException(LDAPException.UNWILLING_TO_PERFORM);
            	}
            }
            
            int rc = LDAPException.SUCCESS;
            try {
                handlerManager.delete(this, partition, request, response);
            } catch (LDAPException e) {
                rc = e.getResultCode();
                throw e;
            } finally {
                if (enableEventListeners) {
                	DeleteEvent afterDeleteEvent = new DeleteEvent(this, DeleteEvent.AFTER_DELETE, this, request, response);
                	afterDeleteEvent.setReturnCode(rc);
                	eventManager.postEvent(afterDeleteEvent);
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
        ModifyRequest request = new ModifyRequest();
        request.setDn(dn);
        request.setModifications(modifications);

        ModifyResponse response = new ModifyResponse();

        modify(request, response);
    }

    public void modify(ModifyRequest request, ModifyResponse response) throws LDAPException {
        try {
            DN dn = request.getDn();

            PartitionManager partitionManager = penroseContext.getPartitionManager();
            Partition partition = partitionManager.getPartition(dn);

            if (partition == null) {
                log.debug("Partition for entry "+dn+" not found.");
                throw ExceptionUtil.createLDAPException(LDAPException.NO_SUCH_OBJECT);
            }

            modify(partition, request, response);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionUtil.createLDAPException(e);
        }
    }

    public void modify(Partition partition, ModifyRequest request, ModifyResponse response) throws LDAPException {
        try {
            if (!isValid()) throw new Exception("Invalid session.");

            lastActivityDate.setTime(System.currentTimeMillis());

            DN dn = request.getDn();
            Collection modifications = request.getModifications();

            if (log.isWarnEnabled()) {
                log.warn("Modify entry \""+dn+"\".");
            }

            boolean debug = log.isDebugEnabled();

            if (debug) {
                log.debug("----------------------------------------------------------------------------------");
                log.debug("MODIFY:");
                log.debug(" - Bind DN    : "+bindDn);
                log.debug(" - DN         : "+dn);
                log.debug(" - Attributes : ");

                for (Iterator i=modifications.iterator(); i.hasNext(); ) {
                    Modification modification = (Modification)i.next();
                    Attribute attribute = modification.getAttribute();

                    String op = LDAPUtil.getModificationOperations(modification.getType());
                    log.debug("   - "+op+": "+attribute.getName()+" => "+attribute.getValues());
                }

                log.debug("");

                log.debug("Controls: "+request.getControls());
            }

            if (enableEventListeners) {
            	ModifyEvent beforeModifyEvent = new ModifyEvent(this, ModifyEvent.BEFORE_MODIFY, this, request, response);
            	boolean b = eventManager.postEvent(beforeModifyEvent);

            	if (!b) {
            		throw ExceptionUtil.createLDAPException(LDAPException.UNWILLING_TO_PERFORM);
            	}
            }

            int rc = LDAPException.SUCCESS;
            try {
                handlerManager.modify(this, partition, request, response);
            } catch (LDAPException e) {
                rc = e.getResultCode();
                throw e;
            } finally {
                if (enableEventListeners) {
                	ModifyEvent afterModifyEvent = new ModifyEvent(this, ModifyEvent.AFTER_MODIFY, this, request, response);
                	afterModifyEvent.setReturnCode(rc);
                	eventManager.postEvent(afterModifyEvent);
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
        ModRdnRequest request = new ModRdnRequest();
        request.setDn(dn);
        request.setNewRdn(newRdn);
        request.setDeleteOldRdn(deleteOldRdn);

        ModRdnResponse response = new ModRdnResponse();

        modrdn(request, response);
    }

    public void modrdn(ModRdnRequest request, ModRdnResponse response) throws LDAPException {
        try {
            DN dn = request.getDn();

            PartitionManager partitionManager = penroseContext.getPartitionManager();
            Partition partition = partitionManager.getPartition(dn);

            if (partition == null) {
                log.debug("Partition for entry "+dn+" not found.");
                throw ExceptionUtil.createLDAPException(LDAPException.NO_SUCH_OBJECT);
            }

            modrdn(partition, request, response);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionUtil.createLDAPException(e);
        }
    }

    public void modrdn(Partition partition, ModRdnRequest request, ModRdnResponse response) throws LDAPException {
        try {
            if (!isValid()) throw new Exception("Invalid session.");

            lastActivityDate.setTime(System.currentTimeMillis());

            DN dn = request.getDn();
            RDN newRdn = request.getNewRdn();
            boolean deleteOldRdn = request.getDeleteOldRdn();

            if (log.isWarnEnabled()) {
                log.warn("ModRDN \""+dn+"\" to \""+newRdn+"\".");
            }

            boolean debug = log.isDebugEnabled();

            if (debug) {
                log.debug("----------------------------------------------------------------------------------");
                log.debug("MODRDN:");
                log.debug(" - Bind DN        : "+bindDn);
                log.debug(" - DN             : "+dn);
                log.debug(" - New RDN        : "+newRdn);
                log.debug(" - Delete old RDN : "+deleteOldRdn);
                log.debug("");

                log.debug("Controls: "+request.getControls());
            }

            if (enableEventListeners) {
            	ModRdnEvent beforeModRdnEvent = new ModRdnEvent(this, ModRdnEvent.BEFORE_MODRDN, this, request, response);
	            boolean b = eventManager.postEvent(beforeModRdnEvent);
	
	            if (!b) {
	                throw ExceptionUtil.createLDAPException(LDAPException.UNWILLING_TO_PERFORM);
	            }
            }

            int rc = LDAPException.SUCCESS;
            try {
                handlerManager.modrdn(this, partition, request, response);
            } catch (LDAPException e) {
                rc = e.getResultCode();
                throw e;
            } finally {
                if (enableEventListeners) {
                    ModRdnEvent afterModRdnEvent = new ModRdnEvent(this, ModRdnEvent.AFTER_MODRDN, this, request, response);
                    afterModRdnEvent.setReturnCode(rc);
                    eventManager.postEvent(afterModRdnEvent);
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

    public void search(
            String baseDn,
            String filter,
            SearchResponse response
    ) throws LDAPException {
        search(baseDn, filter, SearchRequest.SCOPE_SUB, response);
    }

    public void search(
            String baseDn,
            String filter,
            int scope,
            SearchResponse response
    ) throws LDAPException {
        try {
            search(new DN(baseDn), FilterTool.parseFilter(filter), scope, response);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionUtil.createLDAPException(e);
        }
    }

    public void search(
            DN baseDn,
            Filter filter,
            int scope,
            SearchResponse response
    ) throws LDAPException {

        SearchRequest request = new SearchRequest();
        request.setDn(baseDn);
        request.setFilter(filter);
        request.setScope(scope);

        search(request, response);
    }

    public void search(
            SearchRequest request,
            SearchResponse response
    ) throws LDAPException {
        try {
            DN baseDn = request.getDn();

            PartitionManager partitionManager = penroseContext.getPartitionManager();
            Partition partition = partitionManager.getPartition(baseDn);

            if (partition == null && !baseDn.isEmpty()) {
                log.debug("Partition for entry "+baseDn+" not found.");
                throw ExceptionUtil.createLDAPException(LDAPException.NO_SUCH_OBJECT);
            }

            search(partition, request, response);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionUtil.createLDAPException(e);
        }
    }

    public void search(
            final Partition partition,
            final SearchRequest request,
            final SearchResponse response
    ) throws LDAPException {

        try {
            if (!isValid()) throw new Exception("Invalid session.");

            lastActivityDate.setTime(System.currentTimeMillis());

            final DN baseDn = request.getDn();
            Filter filter = request.getFilter();
            String scope = LDAPUtil.getScope(request.getScope());

            if (log.isWarnEnabled()) {
                log.warn("Search \""+baseDn +"\" with scope "+scope+" and filter \""+filter+"\"");
            }

            boolean debug = log.isDebugEnabled();

            if (debug) {
                log.debug("----------------------------------------------------------------------------------");
                log.debug("SEARCH:");
                log.debug(" - Bind DN    : "+bindDn);
                log.debug(" - Base DN    : "+baseDn);
                log.debug(" - Scope      : "+scope);
                log.debug(" - Filter     : "+filter);
                log.debug(" - Attributes : "+request.getAttributes());
                log.debug("");

                log.debug("Controls: "+request.getControls());
            }

            response.setEnableEventListeners(enableEventListeners);

            final Session session = this;

            if (enableEventListeners) {
                SearchEvent beforeSearchEvent = new SearchEvent(session, SearchEvent.BEFORE_SEARCH, this, request, response);
	           	boolean b = eventManager.postEvent(beforeSearchEvent);

	            if (!b) {
	                response.close();
	                throw ExceptionUtil.createLDAPException(LDAPException.UNWILLING_TO_PERFORM);
	            }
            }

            response.setSizeLimit(request.getSizeLimit());
            SearchResponse resultsToUse = response;

            if (enableEventListeners) {
            	resultsToUse = new SearchResponse() {
                    public void add(Object value) throws Exception {
                        response.add(value);
                    }
                    public void close() throws Exception {
                        response.close();
    
                        lastActivityDate.setTime(System.currentTimeMillis());
    
                        SearchEvent afterSearchEvent = new SearchEvent(session, SearchEvent.AFTER_SEARCH, session, request, response);
    
                        LDAPException exception = response.getException();
                        if (exception != null) {
                            afterSearchEvent.setReturnCode(exception.getResultCode());
                        }

                        eventManager.postEvent(afterSearchEvent);
                    }
                };
            }

            handlerManager.search(this, partition, request, resultsToUse);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionUtil.createLDAPException(e);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // UNBIND
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void unbind() throws LDAPException {
        UnbindRequest request = new UnbindRequest();
        request.setDn(bindDn);

        UnbindResponse response = new UnbindResponse();

        unbind(request, response);
    }

    public void unbind(UnbindRequest request, UnbindResponse response) throws LDAPException {
        try {
            if (!isValid()) throw new Exception("Invalid session.");

            lastActivityDate.setTime(System.currentTimeMillis());

            if (log.isWarnEnabled()) {
                log.warn("Unbind \""+bindDn+"\".");
            }

            boolean debug = log.isDebugEnabled();

            if (debug) {
                log.debug("----------------------------------------------------------------------------------");
                log.debug("UNBIND:");
                log.debug(" - Bind DN: "+bindDn);
                log.debug("");

                log.debug("Controls: "+request.getControls());
            }

            if (enableEventListeners) {
            	UnbindEvent beforeUnbindEvent = new UnbindEvent(this, UnbindEvent.BEFORE_UNBIND, this, request, response);
	            boolean b = eventManager.postEvent(beforeUnbindEvent);

	            if (!b) {
	                throw ExceptionUtil.createLDAPException(LDAPException.UNWILLING_TO_PERFORM);
	            }
            }

            int rc = LDAPException.SUCCESS;
            try {
                if (!rootUser && bindDn != null) {
                    PartitionManager partitionManager = penroseContext.getPartitionManager();
                    Partition partition = partitionManager.getPartition(bindDn);

                    if (partition == null) {
                        log.debug("Partition for entry "+bindDn+" not found.");
                        throw ExceptionUtil.createLDAPException(LDAPException.NO_SUCH_OBJECT);
                    }

                    handlerManager.unbind(this, partition, request, response);
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
	                UnbindEvent afterUnbindEvent = new UnbindEvent(this, UnbindEvent.AFTER_UNBIND, this, request, response);
	                afterUnbindEvent.setReturnCode(rc);
	                eventManager.postEvent(afterUnbindEvent);
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

    public Object getSessionId() {
        return sessionId;
    }

    public void setSessionId(Object sessionId) {
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

    public PenroseContext getPenroseContext() {
        return penroseContext;
    }

    public void setPenroseContext(PenroseContext penroseContext) {
        this.penroseContext = penroseContext;

        handlerManager = penroseContext.getHandlerManager();
        eventManager = penroseContext.getEventManager();
    }

    public PenroseConfig getPenroseConfig() {
        return penroseConfig;
    }

    public void setPenroseConfig(PenroseConfig penroseConfig) {
        this.penroseConfig = penroseConfig;

        String s = penroseConfig.getProperty("enableEventListeners");
        enableEventListeners = s == null ? true : new Boolean(s).booleanValue();
    }
}
