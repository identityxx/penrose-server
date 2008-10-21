package org.safehaus.penrose.session;

import org.safehaus.penrose.ldap.Request;
import org.safehaus.penrose.ldap.Response;
import org.safehaus.penrose.control.Control;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ietf.ldap.LDAPException;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class Operation {

    public Logger log = LoggerFactory.getLogger(getClass());
    public boolean debug = log.isDebugEnabled();

    private Session session;
    private String operationName;

    private Request request;
    private Response response;

    private boolean abandoned;

    private Operation parent;

    public Operation() {
    }

    public Operation(Operation parent) {
        this.parent = parent;
    }

    public Session getSession() {
        return parent == null ? session : parent.getSession();
    }

    public void setSession(Session session) {
        if (parent == null) {
            this.session = session;
        } else {
            parent.setSession(session);
        }
    }

    public String getOperationName() {
        return parent == null ? operationName : parent.getOperationName();
    }

    public void setOperationName(String operationName) {
        if (parent == null) {
            this.operationName = operationName;
        } else {
            parent.setOperationName(operationName);
        }
    }

    public Request getRequest() {
        return parent == null ? request : parent.getRequest();
    }

    public void setRequest(Request request) {
        if (parent == null) {
            this.request = request;
        } else {
            parent.setRequest(request);
        }
    }

    public Response getResponse() {
        return parent == null ? response : parent.getResponse();
    }

    public void setResponse(Response response) {
        if (parent == null) {
            this.response = response;
        } else {
            parent.setResponse(response);
        }
    }

    public synchronized void abandon() {
        if (parent == null) {
            abandoned = true;
        } else {
            parent.abandon();
        }
    }

    public synchronized boolean isAbandoned() {
        return parent == null ? abandoned : parent.isAbandoned();
    }

    public int getReturnCode() {
        return getResponse().getReturnCode();
    }

    public Collection<Control> getControls() {
        return getRequest().getControls();
    }

    public LDAPException getException() {
        return getResponse().getException();
    }

    public void setException(LDAPException exception) {
        getResponse().setException(exception);
    }

    public void setException(Exception exception) {
        getResponse().setException(exception);
    }

    public int waitFor() throws Exception {
        return getResponse().waitFor();
    }
}
