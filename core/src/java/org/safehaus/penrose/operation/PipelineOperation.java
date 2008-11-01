package org.safehaus.penrose.operation;

import org.safehaus.penrose.ldap.Request;
import org.safehaus.penrose.ldap.Response;
import org.safehaus.penrose.control.Control;
import org.safehaus.penrose.session.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ietf.ldap.LDAPException;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class PipelineOperation implements Operation {

    protected Logger log = LoggerFactory.getLogger(getClass());
    protected boolean warn = log.isWarnEnabled();
    protected boolean debug = log.isDebugEnabled();

    protected Operation operation;

    public PipelineOperation(Operation parent) {
        this.operation = parent;
    }

    public Session getSession() {
        return operation.getSession();
    }

    public void setSession(Session session) {
        operation.setSession(session);
    }

    public String getSessionName() {
        return operation.getSessionName();
    }

    public String getOperationName() {
        return operation.getOperationName();
    }

    public void setOperationName(String operationName) {
        operation.setOperationName(operationName);
    }

    public Request getRequest() {
        return operation.getRequest();
    }

    public void setRequest(Request request) {
        operation.setRequest(request);
    }

    public Response getResponse() {
        return operation.getResponse();
    }

    public void setResponse(Response response) {
        operation.setResponse(response);
    }

    public synchronized void abandon() {
        operation.abandon();
    }

    public synchronized boolean isAbandoned() {
        return operation.isAbandoned();
    }

    public int getReturnCode() {
        return operation.getReturnCode();
    }

    public Collection<Control> getRequestControls() {
        return operation.getRequestControls();
    }

    public Collection<Control> getResponseControls() {
        return operation.getResponseControls();
    }

    public LDAPException getException() {
        return operation.getException();
    }

    public void setException(LDAPException exception) {
        operation.setException(exception);
    }

    public void setException(Exception exception) {
        operation.setException(exception);
    }

    public int waitFor() {
        return operation.waitFor();
    }

    public void init() throws Exception {
        operation.init();
    }
}