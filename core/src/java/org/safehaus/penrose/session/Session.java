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

import org.ietf.ldap.LDAPException;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.event.*;
import org.safehaus.penrose.event.SearchListener;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.log.Access;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.util.PasswordUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class Session {

    public Logger log = LoggerFactory.getLogger(getClass());
    public boolean warn = log.isWarnEnabled();
    public boolean debug = log.isDebugEnabled();

    public final static String EVENTS_ENABLED              = "eventsEnabled";
    public final static String SEARCH_RESPONSE_BUFFER_SIZE = "searchResponseBufferSize";

    protected PenroseConfig penroseConfig;
    protected PenroseContext penroseContext;
    protected SessionContext sessionContext;

    protected EventManager eventManager;

    protected String sessionName;

    protected DN bindDn;
    protected boolean rootUser;

    protected Map<String,Object> attributes = new HashMap<String,Object>();
    
    protected boolean eventsEnabled = true;
    protected long bufferSize;

    protected Map<String,Operation> operations = Collections.synchronizedMap(new LinkedHashMap<String,Operation>());

    protected List<SessionListener> listeners = new ArrayList<SessionListener>();

    protected int nextMessageId;
    protected boolean closed;

    public Session() {
    }

    public void init() {

        if (debug) {
            log.debug("----------------------------------------------------------------------------------");
            log.debug("Creating session "+ sessionName +".");
        }

        String s = penroseConfig.getProperty(EVENTS_ENABLED);
        eventsEnabled = s == null || Boolean.valueOf(s);

        s = penroseConfig.getProperty(SEARCH_RESPONSE_BUFFER_SIZE);
        bufferSize = s == null ? 0 : Long.parseLong(s);

        if (debug) log.debug("Session "+ sessionName +" created.");
    }

    public void connect(ConnectRequest request) throws Exception {
        Access.log(this, request);
        if (warn) log.warn("Session "+ sessionName +": Connect from "+request.getClientAddress()+".");
    }

    public void disconnect(DisconnectRequest request) throws Exception {
        Access.log(this, request);
        if (warn) log.warn("Session "+ sessionName +": Disconnect.");
    }

    public void close() throws Exception {

        if (debug) {
            log.debug("----------------------------------------------------------------------------------");
            log.debug("Closing session "+ sessionName +".");
        }

        closed = true;

        for (SessionListener listener : listeners) {
            listener.sessionClosed();
        }

        for (String operationName : operations.keySet()) {
            Operation operation = operations.get(operationName);
            operation.abandon();
        }

        SessionManager sessionManager = sessionContext.getSessionManager();
        sessionManager.removeSession(sessionName);

        if (debug) log.debug("Session "+ sessionName +" closed.");
    }

    public boolean isClosed() {
        return closed;
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

    public synchronized Integer getNextMessageId() {
        return nextMessageId++;
    }

    public void checkMessageId(Request request, Response response) {

        Integer messageId = request.getMessageId();

        if (messageId == null) {
            messageId = getNextMessageId();
            request.setMessageId(messageId);
        }

        response.setMessageId(messageId);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Operations
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public Collection<String> getOperationNames() {
        return operations.keySet();
    }

    public Operation getOperation(String operationName) {
        return operations.get(operationName);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Abandon
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void abandon(String operationName) throws LDAPException {

        AbandonRequest request = new AbandonRequest();
        request.setOperationName(operationName);

        AbandonResponse response = new AbandonResponse();

        abandon(request, response);
    }

    public void abandon(AbandonRequest request, AbandonResponse response) throws LDAPException {
        try {
            checkMessageId(request, response);

            Access.log(this, request);

            Integer messageId = request.getMessageId();
            String operationName = request.getOperationName();

            if (warn) log.warn("Session "+ sessionName +" ("+messageId+"): Abandon "+operationName+".");

            if (debug) {
                log.debug("----------------------------------------------------------------------------------");
                log.debug("ABANDON:");
                log.debug(" - Session        : "+ sessionName);
                log.debug(" - Message        : "+messageId);
                log.debug(" - Bind DN        : "+(bindDn == null ? "" : bindDn));
                log.debug(" - ID to abandon  : "+operationName);
                log.debug("");

                log.debug("Controls: "+request.getControls());
                log.debug("");
            }

            Operation operation = operations.get(operationName);
            if (operation == null) return;

            operation.abandon();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            response.setException(e);
            throw response.getException();

        } finally {
            Access.log(this, response);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Add
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void add(String dn, Attributes attributes) throws LDAPException {
        add(new DN(dn), attributes);
    }

    public void add(DN dn, Attributes attributes) throws LDAPException {

        AddRequest request = new AddRequest();
        request.setDn(dn);
        request.setAttributes(attributes);

        AddResponse response = new AddResponse();

        add(request, response);
    }
    
    public void add(AddRequest request, AddResponse response) throws LDAPException {
        try {
            checkMessageId(request, response);

            Access.log(this, request);

            Integer messageId = request.getMessageId();
            DN dn = request.getDn();

            if (warn) log.warn("Session "+ sessionName +" ("+messageId+"): Add "+dn+".");

            if (debug) {
                log.debug("----------------------------------------------------------------------------------");
                log.debug("ADD:");
                log.debug(" - Session        : "+ sessionName);
                log.debug(" - Message        : "+messageId);
                log.debug(" - Bind DN        : "+(bindDn == null ? "" : bindDn));
                log.debug(" - Entry          : "+dn);
                log.debug("");

                log.debug("Attributes:");
                request.getAttributes().print();
                log.debug("");

                log.debug("Controls: "+request.getControls());
                log.debug("");
            }

            PartitionManager partitionManager = penroseContext.getPartitionManager();
            Partition partition = partitionManager.getPartition(dn);

            if (eventsEnabled) {
            	AddEvent beforeModifyEvent = new AddEvent(this, AddEvent.BEFORE_ADD, this, partition, request, response);
            	eventManager.postEvent(beforeModifyEvent);
            }

            try {
                partition.add(this, request, response);

            } catch (LDAPException e) {
                response.setException(e);
                throw e;

            } finally {
                if (eventsEnabled) {
                    AddEvent addEvent = new AddEvent(this, AddEvent.AFTER_ADD, this, partition, request, response);
                    eventManager.postEvent(addEvent);
                }
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            response.setException(e);
            throw response.getException();

        } finally {
            Access.log(this, response);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Bind
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void bind(String dn, String password) throws LDAPException {
        bind(new DN(dn), password == null ? null : password.getBytes());
    }

    public void bind(String dn, byte[] password) throws LDAPException {
    	bind(new DN(dn), password);
    }

    public void bind(DN dn, String password) throws LDAPException {
        bind(dn, password.getBytes());
    }

    public void bind(DN dn, byte[] password) throws LDAPException {

        BindRequest request = new BindRequest();
        request.setDn(dn);
        request.setPassword(password);

        BindResponse response = new BindResponse();

        bind(request, response);

        LDAPException exception = response.getException();
        if (exception.getResultCode() != LDAP.SUCCESS) {
            throw exception;
        }
    }

    public void bind(BindRequest request, BindResponse response) throws LDAPException {
        try {
            checkMessageId(request, response);

            Access.log(this, request);

            Integer messageId = request.getMessageId();
            DN dn = request.getDn();
            byte[] password = request.getPassword();

            if (warn) log.warn("Session "+ sessionName +" ("+messageId+"): Bind "+(dn.isEmpty() ? "anonymously" : "as "+request.getDn())+".");

            if (debug) {
                log.debug("----------------------------------------------------------------------------------");
                log.debug("BIND:");
                log.debug(" - Session        : "+ sessionName);
                log.debug(" - Message        : "+messageId);
                log.debug(" - Bind DN        : "+dn);
                log.debug(" - Bind Password  : "+(password == null ? "" : new String(password)));
                log.debug("");

                log.debug("Controls: "+request.getControls());
            }

            PartitionManager partitionManager = penroseContext.getPartitionManager();
            Partition partition = partitionManager.getPartition(dn);

            if (eventsEnabled) {
            	BindEvent beforeBindEvent = new BindEvent(this, BindEvent.BEFORE_BIND, this, partition, request, response);
            	eventManager.postEvent(beforeBindEvent);
        	}
            
            if (dn.isEmpty() || password == null || password.length == 0) {
                log.debug("Bound as anonymous user.");
                bindDn = null;
                rootUser = false;
                return;
            }

            DN rootDn = penroseConfig.getRootDn();
            byte[] rootPassword = penroseConfig.getRootPassword();

            if (rootDn.matches(dn)) {
                if (PasswordUtil.comparePassword(password, rootPassword)) {
                    log.debug("Bound as root user.");
                    bindDn = rootDn;
                    rootUser = true;

                } else {
                    log.debug("Root password doesn't match.");
                    response.setException(LDAP.createException(LDAP.INVALID_CREDENTIALS));
                }
                return;
            }

            try {
                partition.bind(this, request, response);

                if (response.getReturnCode() == LDAP.SUCCESS) {
                    if (debug) log.debug("Bound as "+dn);
                    bindDn = dn;
                    rootUser = false;

                } else {
                    log.debug("Bind failed.");
                }

            } catch (LDAPException e) {
                response.setException(e);
                throw e;

            } finally {
                if (eventsEnabled) {
                	BindEvent afterBindEvent = new BindEvent(this, BindEvent.AFTER_BIND, this, partition, request, response);
                	eventManager.postEvent(afterBindEvent);
                }
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            response.setException(e);
            throw response.getException();

        } finally {
            Access.log(this, response);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Compare
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

        compare(request, response);

        return response.getReturnCode() == LDAP.COMPARE_TRUE;
    }

    public void compare(CompareRequest request, CompareResponse response) throws LDAPException {
        try {
            checkMessageId(request, response);

            Access.log(this, request);

            Integer messageId = request.getMessageId();
            DN dn = request.getDn();

            if (warn) log.warn("Session "+ sessionName +" ("+messageId+"): Compare "+dn+".");

            if (debug) {
                log.debug("----------------------------------------------------------------------------------");
                log.debug("COMPARE:");
                log.debug(" - Session        : "+ sessionName);
                log.debug(" - Message        : "+messageId);
                log.debug(" - Bind DN        : "+(bindDn == null ? "" : bindDn));
                log.debug(" - DN             : "+request.getDn());
                log.debug(" - Attribute Name : "+request.getAttributeName());

                Object attributeValue = request.getAttributeValue();

                Object value;
                if (attributeValue instanceof byte[]) {
                    //value = BinaryUtil.encode(BinaryUtil.BIG_INTEGER, (byte[])attributeValue);
                    value = new String((byte[])attributeValue);
                } else {
                    value = attributeValue;
                }

                log.debug(" - Attribute Value : "+value);
                log.debug("");

                log.debug("Controls: "+request.getControls());
            }

            PartitionManager partitionManager = penroseContext.getPartitionManager();
            Partition partition = partitionManager.getPartition(dn);

            if (eventsEnabled) {
            	CompareEvent beforeCompareEvent = new CompareEvent(this, CompareEvent.BEFORE_COMPARE, this, partition, request, response);
            	eventManager.postEvent(beforeCompareEvent);
            }

            try {
                partition.compare(this, request, response);

            } catch (LDAPException e) {
                response.setException(e);
                throw e;

            } finally {
                if (eventsEnabled) {
                	CompareEvent afterCompareEvent = new CompareEvent(this, CompareEvent.AFTER_COMPARE, this, partition, request, response);
                	eventManager.postEvent(afterCompareEvent);
                }
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            response.setException(e);
            throw response.getException();

        } finally {
            Access.log(this, response);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Delete
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
            checkMessageId(request, response);

            Access.log(this, request);

            Integer messageId = request.getMessageId();
            DN dn = request.getDn();

            if (warn) log.warn("Session "+ sessionName +" ("+messageId+"): Delete "+dn+".");

            if (debug) {
                log.debug("----------------------------------------------------------------------------------");
                log.debug("DELETE:");
                log.debug(" - Session        : "+ sessionName);
                log.debug(" - Message        : "+messageId);
                log.debug(" - Bind DN        : "+(bindDn == null ? "" : bindDn));
                log.debug(" - DN             : "+dn);
                log.debug("");

                log.debug("Controls: "+request.getControls());
            }

            PartitionManager partitionManager = penroseContext.getPartitionManager();
            Partition partition = partitionManager.getPartition(dn);

            if (eventsEnabled) {
            	DeleteEvent beforeDeleteEvent = new DeleteEvent(this, DeleteEvent.BEFORE_DELETE, this, partition, request, response);
            	eventManager.postEvent(beforeDeleteEvent);
            }
            
            try {
                partition.delete(this, request, response);

            } catch (LDAPException e) {
                response.setException(e);
                throw e;

            } finally {
                if (eventsEnabled) {
                	DeleteEvent afterDeleteEvent = new DeleteEvent(this, DeleteEvent.AFTER_DELETE, this, partition, request, response);
                	eventManager.postEvent(afterDeleteEvent);
                }
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            response.setException(e);
            throw response.getException();

        } finally {
            Access.log(this, response);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Modify
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modify(String dn, Collection<Modification> modifications) throws LDAPException {
        modify(new DN(dn), modifications);
    }

    public void modify(DN dn, Collection<Modification> modifications) throws LDAPException {

        ModifyRequest request = new ModifyRequest();
        request.setDn(dn);
        request.setModifications(modifications);

        ModifyResponse response = new ModifyResponse();

        modify(request, response);
    }

    public void modify(ModifyRequest request, ModifyResponse response) throws LDAPException {
        try {
            checkMessageId(request, response);
            Access.log(this, request);

            Integer messageId = request.getMessageId();
            DN dn = request.getDn();

            if (warn) log.warn("Session "+ sessionName +" ("+messageId+"): Modify "+dn+".");

            if (debug) {
                log.debug("----------------------------------------------------------------------------------");
                log.debug("MODIFY:");
                log.debug(" - Session        : "+ sessionName);
                log.debug(" - Message        : "+messageId);
                log.debug(" - Bind DN        : "+(bindDn == null ? "" : bindDn));
                log.debug(" - DN             : "+dn);
                log.debug("");

                log.debug("Modifications:");

                Collection<Modification> modifications = request.getModifications();

                for (Modification modification : modifications) {
                    Attribute attribute = modification.getAttribute();

                    String op = LDAP.getModificationOperation(modification.getType());
                    log.debug("   - " + op + ": " + attribute.getName() + " => " + attribute.getValues());
                }

                log.debug("");

                log.debug("Controls: "+request.getControls());
            }

            PartitionManager partitionManager = penroseContext.getPartitionManager();
            Partition partition = partitionManager.getPartition(dn);

            if (eventsEnabled) {
            	ModifyEvent beforeModifyEvent = new ModifyEvent(this, ModifyEvent.BEFORE_MODIFY, this, partition, request, response);
            	eventManager.postEvent(beforeModifyEvent);
            }

            try {
                partition.modify(this, request, response);

            } catch (LDAPException e) {
                response.setException(e);
                throw e;

            } finally {
                if (eventsEnabled) {
                	ModifyEvent afterModifyEvent = new ModifyEvent(this, ModifyEvent.AFTER_MODIFY, this, partition, request, response);
                	eventManager.postEvent(afterModifyEvent);
                }
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            response.setException(e);
            throw response.getException();

        } finally {
            Access.log(this, response);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // ModRdn
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modrdn(String dn, String newRdn, boolean deleteOldRdn) throws LDAPException {
        try {
            modrdn(new DN(dn), new RDN(newRdn), deleteOldRdn);
        } catch (Exception e) {
            throw LDAP.createException(e);
        }
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
            checkMessageId(request, response);
            Access.log(this, request);

            Integer messageId = request.getMessageId();
            DN dn = request.getDn();
            RDN newRdn = request.getNewRdn();

            if (warn) log.warn("Session "+ sessionName +" ("+messageId+"): Rename "+dn+" to "+newRdn+".");

            if (debug) {
                log.debug("----------------------------------------------------------------------------------");
                log.debug("MODRDN:");
                log.debug(" - Session        : "+ sessionName);
                log.debug(" - Message        : "+messageId);
                log.debug(" - Bind DN        : "+(bindDn == null ? "" : bindDn));
                log.debug(" - DN             : "+dn);
                log.debug(" - New RDN        : "+newRdn);
                log.debug(" - Delete old RDN : "+request.getDeleteOldRdn());
                log.debug("");

                log.debug("Controls: "+request.getControls());
            }

            PartitionManager partitionManager = penroseContext.getPartitionManager();
            Partition partition = partitionManager.getPartition(dn);

            if (eventsEnabled) {
            	ModRdnEvent beforeModRdnEvent = new ModRdnEvent(this, ModRdnEvent.BEFORE_MODRDN, this, partition, request, response);
	            eventManager.postEvent(beforeModRdnEvent);
            }

            try {
                partition.modrdn(this, request, response);

            } catch (LDAPException e) {
                response.setException(e);
                throw e;

            } finally {
                if (eventsEnabled) {
                    ModRdnEvent afterModRdnEvent = new ModRdnEvent(this, ModRdnEvent.AFTER_MODRDN, this, partition, request, response);
                    eventManager.postEvent(afterModRdnEvent);
                }
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            response.setException(e);
            throw response.getException();

        } finally {
            Access.log(this, response);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Search
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public SearchOperation createSearchOperation() {

        String operationName = "operation-"+getNextMessageId();

        SearchOperation operation = new SearchOperation();
        operation.setSession(this);
        operation.setOperationName(operationName);

        return operation;
    }

    public SearchOperation createSearchOperation(String operationName) {

        SearchOperation operation = new SearchOperation();
        operation.setSession(this);
        operation.setOperationName(operationName);

        return operation;
    }

    public SearchResponse search(
            String baseDn,
            String filter
    ) throws LDAPException {
        return search(baseDn, filter, SearchRequest.SCOPE_SUB);
    }

    public SearchResponse search(
            String baseDn,
            String filter,
            int scope
    ) throws LDAPException {
        try {
            return search(new DN(baseDn), FilterTool.parseFilter(filter), scope);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw LDAP.createException(e);
        }
    }

    public SearchResponse search(
            DN baseDn,
            Filter filter,
            int scope
    ) throws LDAPException {

        SearchRequest request = new SearchRequest();
        request.setDn(baseDn);
        request.setFilter(filter);
        request.setScope(scope);

        SearchResponse response = new SearchResponse();

        search(request, response);

        return response;
    }

    public void search(final SearchRequest request, final SearchResponse response) throws LDAPException {
        checkMessageId(request, response);

        SearchOperation operation = createSearchOperation(""+request.getMessageId());
        operation.setRequest(request);
        operation.setResponse(response);

        search(operation);
    }

    public void search(SearchOperation operation) throws LDAPException {

        final String operationName = operation.getOperationName();
        operations.put(operationName, operation);

        final SearchResponse response = (SearchResponse)operation.getResponse();

        try {
            Access.log(this, (SearchRequest)operation.getRequest());

            DN dn = operation.getDn();
            Filter filter = operation.getFilter();
            int scope = operation.getScope();

            if (warn) log.warn("Session "+ sessionName +" ("+operationName+"): Search "+dn+" with filter "+filter+".");

            if (debug) {
                log.debug("----------------------------------------------------------------------------------");
                log.debug("SEARCH:");
                log.debug(" - Session        : "+ sessionName);
                log.debug(" - Message        : "+operationName);
                log.debug(" - Bind DN        : "+(bindDn == null ? "" : bindDn));
                log.debug(" - Base DN        : "+dn);
                log.debug(" - Scope          : "+LDAP.getScope(scope));
                log.debug(" - Filter         : "+filter);
                log.debug(" - Attributes     : "+operation.getAttributes());
                log.debug("");

                log.debug("Controls: "+operation.getControls());
            }

            PartitionManager partitionManager = penroseContext.getPartitionManager();
            Partition partition = partitionManager.getPartition(dn);

            operation.setBufferSize(bufferSize);
            response.setSizeLimit(operation.getSizeLimit());

            SearchOperation op = new SearchOperation(operation) {
                public void add(SearchResult result) throws Exception {
                    if (debug) log.debug("Result: \""+result.getDn()+"\".");
                    //if (debug) result.getAttributes().print();
                    super.add(result);
                }
                public void add(SearchReference reference) throws Exception {
                    if (debug) log.debug("Reference: \""+ reference.getDn()+"\".");
                    super.add(reference);
                }
                public void setException(LDAPException exception) {
                    if (debug) log.debug("Error: \""+exception.getMessage()+"\".");
                    super.setException(exception);
                }
                public void close() throws Exception {
                    if (debug) log.debug("Closing search response.");
                    if (super.isClosed()) {
                        if (debug) log.debug("Search response is already closed.");
                    } else {
                        super.close();
                    }

                    operations.remove(operationName);
                    Access.log(Session.this, (SearchResponse)getResponse());
                }
            };

            partition.search(op);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            operation.setException(e);
            try { operation.close(); } catch (Exception ex) { log.error(ex.getMessage(), ex); }

            operations.remove(operationName);

            Access.log(this, (SearchResponse)operation.getResponse());
            throw operation.getException();
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Unbind
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void unbind() throws LDAPException {
        UnbindRequest request = new UnbindRequest();
        request.setDn(bindDn);

        UnbindResponse response = new UnbindResponse();

        unbind(request, response);
    }

    public void unbind(UnbindRequest request, UnbindResponse response) throws LDAPException {

        try {
            checkMessageId(request, response);
            Access.log(this, request);

            Integer messageId = request.getMessageId();
            if (warn) log.warn("Session "+ sessionName +" ("+messageId+"): Unbind.");

            if (debug) {
                log.debug("----------------------------------------------------------------------------------");
                log.debug("UNBIND:");
                log.debug(" - Session        : "+ sessionName);
                log.debug(" - Message        : "+messageId);
                log.debug(" - Bind DN        : "+(bindDn == null ? "" : bindDn));
                log.debug("");

                log.debug("Controls: "+request.getControls());
            }

            PartitionManager partitionManager = penroseContext.getPartitionManager();
            Partition partition;

            if (!rootUser && bindDn != null) {
                partition = partitionManager.getPartition(bindDn);

            } else {
                partition = partitionManager.getPartition("DEFAULT");
            }

            if (eventsEnabled) {
            	UnbindEvent beforeUnbindEvent = new UnbindEvent(this, UnbindEvent.BEFORE_UNBIND, this, partition, request, response);
	            eventManager.postEvent(beforeUnbindEvent);
            }

            try {
                if (bindDn == null) {
                    return;
                }

                if (rootUser) {
                    rootUser = false;
                    return;
                }

                partition.unbind(this, request, response);
                bindDn = null;

            } catch (LDAPException e) {
                response.setException(e);
                throw e;

            } finally {
                if (eventsEnabled) {
	                UnbindEvent afterUnbindEvent = new UnbindEvent(this, UnbindEvent.AFTER_UNBIND, this, partition, request, response);
	                eventManager.postEvent(afterUnbindEvent);
                }
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            response.setException(e);
            throw response.getException();

        } finally {
            Access.log(this, response);
        }
    }

    public String getSessionName() {
        return sessionName;
    }

    public void setSessionName(String sessionName) {
        this.sessionName = sessionName;
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

    public void setAttribute(String name, Object value) {
        attributes.put(name, value);
    }

    public Object getAttribute(String name) {
        return attributes.get(name);
    }

    public Collection getAttributeNames() {
        return attributes.keySet();
    }

    public Object removeAttribute(String name) {
        return attributes.remove(name);
    }
    
    public boolean isRootUser() {
        return rootUser;
    }

    public void setRootUser(boolean rootUser) {
        this.rootUser = rootUser;
    }

    public PenroseContext getPenroseContext() {
        return penroseContext;
    }

    public void setPenroseContext(PenroseContext penroseContext) {
        this.penroseContext = penroseContext;
    }

    public SessionContext getSessionContext() {
        return sessionContext;
    }

    public void setSessionContext(SessionContext sessionContext) {
        this.sessionContext = sessionContext;
        eventManager = sessionContext.getEventManager();
    }

    public PenroseConfig getPenroseConfig() {
        return penroseConfig;
    }

    public void setPenroseConfig(PenroseConfig penroseConfig) {
        this.penroseConfig = penroseConfig;
    }

    public boolean isEventsEnabled() {
        return eventsEnabled;
    }

    public void setEventsEnabled(boolean eventsEnabled) {
        this.eventsEnabled = eventsEnabled;
    }

    public long getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(long bufferSize) {
        this.bufferSize = bufferSize;
    }

    public Collection<SessionListener> getListeners() {
        return listeners;
    }

    public void setListeners(Collection<SessionListener> listeners) {
        if (this.listeners == listeners) return;
        this.listeners.clear();
        if (listeners == null) return;
        this.listeners.addAll(listeners);
    }

    public void addListener(SessionListener listener) {
        listeners.add(listener);
    }

    public void addListener(int i, SessionListener listener) {
        listeners.add(i, listener);
    }

    public void removeListener(SessionListener listener) {
        listeners.remove(listener);
    }
}
