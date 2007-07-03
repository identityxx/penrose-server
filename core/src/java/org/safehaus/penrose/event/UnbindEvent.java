package org.safehaus.penrose.event;

import org.safehaus.penrose.session.*;
import org.safehaus.penrose.ldap.UnbindResponse;
import org.safehaus.penrose.ldap.UnbindRequest;

/**
 * @author Endi S. Dewata
 */
public class UnbindEvent extends Event {

    public final static int BEFORE_UNBIND = 0;
    public final static int AFTER_UNBIND  = 1;

    protected Session session;
    protected int returnCode;

    protected UnbindRequest request;
    protected UnbindResponse response;

    public UnbindEvent(Object source, int type, Session session, UnbindRequest request, UnbindResponse response) {
        super(source, type);
        this.session = session;
        this.request = request;
        this.response = response;
    }

    public int getReturnCode() {
        return returnCode;
    }

    public void setReturnCode(int returnCode) {
        this.returnCode = returnCode;
    }

    public Session getSession() {
        return session;
    }

    public void setSession(Session session) {
        this.session = session;
    }

    public UnbindRequest getRequest() {
        return request;
    }

    public void setRequest(UnbindRequest request) {
        this.request = request;
    }

    public UnbindResponse getResponse() {
        return response;
    }

    public void setResponse(UnbindResponse response) {
        this.response = response;
    }

    public String toString() {
        return (type == BEFORE_UNBIND ? "Before" : "After")+"Unbind";
    }
}
