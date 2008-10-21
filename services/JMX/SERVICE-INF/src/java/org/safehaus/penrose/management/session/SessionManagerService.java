package org.safehaus.penrose.management.session;

import org.safehaus.penrose.management.BaseService;
import org.safehaus.penrose.management.PenroseJMXService;
import org.safehaus.penrose.session.SessionManagerServiceMBean;
import org.safehaus.penrose.session.SessionManager;
import org.safehaus.penrose.session.SessionManagerClient;
import org.safehaus.penrose.session.Session;

import java.util.ArrayList;
import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class SessionManagerService extends BaseService implements SessionManagerServiceMBean {

    public final Collection<String> EMPTY = new ArrayList<String>();

    SessionManager sessionManager;

    public SessionManagerService(PenroseJMXService jmxService, SessionManager sessionManager) {

        this.jmxService = jmxService;
        this.sessionManager = sessionManager;
    }

    public Object getObject() {
        return sessionManager;
    }

    public String getObjectName() {
        return SessionManagerClient.getStringObjectName();
    }


    public Collection<String> getSessionNames() throws Exception {
        Collection<String> list = new ArrayList<String>();
        list.addAll(sessionManager.getSessionNames());
        return list;
    }

    public Collection<String> getOperationNames(String sessionName) throws Exception {
        Session session = sessionManager.getSession(sessionName);
        if (session == null) {
            log.debug("Session "+sessionName+" not found.");
            return EMPTY;
        }

        Collection<String> list = new ArrayList<String>();
        list.addAll(session.getOperationNames());

        return list;
    }

    public SessionService getSessionService(String sessionName) throws Exception {

        SessionService sessionService = new SessionService(jmxService, sessionManager, sessionName);
        sessionService.init();

        return sessionService;
    }

    public void closeSession(String sessionName) throws Exception {
        Session session = sessionManager.getSession(sessionName);
        if (session == null) {
            log.debug("Session "+sessionName+" not found.");
            return;
        }

        session.close();
        log.debug("Session "+sessionName+" closed.");
    }

    public void abandonOperation(String sessionName, String operationName) throws Exception {
        Session session = sessionManager.getSession(sessionName);
        if (session == null) {
            log.debug("Session "+sessionName+" not found.");
            return;
        }

        session.abandon(operationName);
        log.debug("Operation "+operationName+" in session "+sessionName+" abandoned.");
    }
}