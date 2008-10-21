package org.safehaus.penrose.management.session;

import org.safehaus.penrose.management.BaseService;
import org.safehaus.penrose.management.PenroseJMXService;
import org.safehaus.penrose.session.SessionManager;
import org.safehaus.penrose.session.SessionServiceMBean;
import org.safehaus.penrose.session.SessionClient;
import org.safehaus.penrose.session.Session;

import java.util.Collection;
import java.util.ArrayList;

/**
 * @author Endi Sukma Dewata
 */
public class SessionService extends BaseService implements SessionServiceMBean {

    public final Collection<String> EMPTY = new ArrayList<String>();

    private SessionManager sessionManager;
    private String sessionName;

    public SessionService(PenroseJMXService jmxService, SessionManager serviceManager, String sessionName) throws Exception {

        this.jmxService = jmxService;
        this.sessionManager = serviceManager;
        this.sessionName = sessionName;
    }

    public Object getObject() {
        return sessionManager.getSession(sessionName);
    }

    public String getObjectName() {
        return SessionClient.getStringObjectName(sessionName);
    }

    public SessionManager getSessionManager() {
        return sessionManager;
    }

    public void setSessionManager(SessionManager serviceManager) {
        this.sessionManager = serviceManager;
    }

    public Object getSessionName() {
        return sessionName;
    }

    public void setSessionName(String sessionName) {
        this.sessionName = sessionName;
    }

    public Collection<String> getOperationNames() throws Exception {

        Session session = sessionManager.getSession(sessionName);
        if (session == null) return EMPTY;

        Collection<String> list = new ArrayList<String>();
        list.addAll(session.getOperationNames());

        return list;
    }

    public void abandon(String operationName) throws Exception {
        Session session = sessionManager.getSession(sessionName);
        if (session == null) return;

        session.abandon(operationName);
    }
}