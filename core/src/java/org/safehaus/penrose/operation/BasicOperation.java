package org.safehaus.penrose.operation;

import org.safehaus.penrose.ldap.Request;
import org.safehaus.penrose.ldap.Response;
import org.safehaus.penrose.control.Control;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.Penrose;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ietf.ldap.LDAPException;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class BasicOperation implements Operation {

    protected Logger log = LoggerFactory.getLogger(getClass());
    protected boolean warn = log.isWarnEnabled();
    protected boolean debug = log.isDebugEnabled();

    protected Session session;
    protected Penrose penrose;

    protected String operationName;

    protected Request request;
    protected Response response;

    protected boolean abandoned;

    public BasicOperation(Session session) {
        this.session = session;
        penrose = session.getPenrose();
    }

    public Session getSession() {
        return session;
    }

    public void setSession(Session session) {
        this.session = session;
    }

    public String getSessionName() {
        return session.getSessionName();
    }

    public String getOperationName() {
        return operationName;
    }

    public void setOperationName(String operationName) {
        this.operationName = operationName;
    }

    public Request getRequest() {
        return request;
    }

    public void setRequest(Request request) {
        this.request = request;
    }

    public Response getResponse() {
        return response;
    }

    public void setResponse(Response response) {
        this.response = response;
    }

    public synchronized void abandon() {
        abandoned = true;
    }

    public synchronized boolean isAbandoned() {
        return abandoned;
    }

    public int getReturnCode() {
        return response.getReturnCode();
    }

    public Collection<Control> getRequestControls() {
        return request.getControls();
    }

    public Collection<Control> getResponseControls() {
        return response.getControls();
    }

    public LDAPException getException() {
        return response.getException();
    }

    public void setException(LDAPException exception) {
        response.setException(exception);
    }

    public void setException(Exception exception) {
        response.setException(exception);
    }

    public int waitFor() throws Exception {
        return response.waitFor();
    }

    public void execute() throws Exception {
    }
}