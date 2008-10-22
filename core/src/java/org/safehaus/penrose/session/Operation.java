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

    protected Session session;
    protected String operationName;

    protected Request request;
    protected Response response;

    protected boolean abandoned;

    protected Operation parent;

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
        if (parent == null) {
            return operationName;
        } else {
            return parent.getOperationName();
        }
    }

    public void setOperationName(String operationName) {
        if (parent == null) {
            this.operationName = operationName;
        } else {
            parent.setOperationName(operationName);
        }
    }

    public Request getRequest() {
        if (parent == null) {
            return request;
        } else {
            return parent.getRequest();
        }
    }

    public void setRequest(Request request) {
        if (parent == null) {
            this.request = request;
        } else {
            parent.setRequest(request);
        }
    }

    public Response getResponse() {
        if (parent == null) {
            return response;
        } else {
            return parent.getResponse();
        }
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
        if (parent == null) {
            return abandoned;
        } else {
            return parent.isAbandoned();
        }
    }

    public int getReturnCode() {
        if (parent == null) {
            return response.getReturnCode();
        } else {
            return parent.getReturnCode();
        }
    }

    public Collection<Control> getControls() {
        if (parent == null) {
            return request.getControls();
        } else {
            return parent.getControls();
        }
    }

    public LDAPException getException() {
        if (parent == null) {
            return response.getException();
        } else {
            return parent.getException();
        }
    }

    public void setException(LDAPException exception) {
        if (parent == null) {
            response.setException(exception);
        } else {
            parent.setException(exception);
        }
    }

    public void setException(Exception exception) {
        if (parent == null) {
            response.setException(exception);
        } else {
            parent.setException(exception);
        }
    }

    public int waitFor() throws Exception {
        if (parent == null) {
            return response.waitFor();
        } else {
            return parent.waitFor();
        }
    }

    public Operation getParent() {
        return parent;
    }
}
