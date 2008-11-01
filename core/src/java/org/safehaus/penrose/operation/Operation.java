package org.safehaus.penrose.operation;

import org.safehaus.penrose.ldap.Request;
import org.safehaus.penrose.ldap.Response;
import org.safehaus.penrose.control.Control;
import org.safehaus.penrose.session.Session;
import org.ietf.ldap.LDAPException;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public interface Operation {

    public Session getSession();
    public void setSession(Session session);

    public String getSessionName();
    public String getOperationName();

    public void setOperationName(String operationName);

    public Request getRequest();
    public void setRequest(Request request);

    public Response getResponse();
    public void setResponse(Response response);

    public void abandon();
    public boolean isAbandoned();

    public int getReturnCode();

    public Collection<Control> getRequestControls();
    public Collection<Control> getResponseControls();

    public LDAPException getException();
    public void setException(LDAPException exception);
    public void setException(Exception exception);

    public int waitFor();

    public void init() throws Exception;
}
