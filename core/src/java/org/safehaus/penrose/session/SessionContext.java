package org.safehaus.penrose.session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.event.EventManager;

/**
 * @author Endi S. Dewata
 */
public class SessionContext {

    public Logger log = LoggerFactory.getLogger(getClass());

    private PenroseConfig penroseConfig;
    private PenroseContext penroseContext;

    private EventManager   eventManager;
    private SessionManager sessionManager;

    private boolean closed;

    public SessionContext() {
    }

    public PenroseConfig getPenroseConfig() {
        return penroseConfig;
    }

    public void setPenroseConfig(PenroseConfig penroseConfig) {
        this.penroseConfig = penroseConfig;
    }

    public PenroseContext getPenroseContext() {
        return penroseContext;
    }

    public void setPenroseContext(PenroseContext penroseContext) {
        this.penroseContext = penroseContext;
    }

    public EventManager getEventManager() {
        return eventManager;
    }

    public void setEventManager(EventManager eventManager) {
        this.eventManager = eventManager;
    }

    public SessionManager getSessionManager() {
        return sessionManager;
    }

    public void setSessionManager(SessionManager sessionManager) {
        this.sessionManager = sessionManager;
    }

    public void init() throws Exception {

        eventManager = new EventManager();
        eventManager.setPenroseConfig(penroseConfig);
        eventManager.setPenroseContext(penroseContext);
        eventManager.setSessionContext(this);

        sessionManager = new SessionManager();
        sessionManager.setPenroseConfig(penroseConfig);
        sessionManager.setPenroseContext(penroseContext);
        sessionManager.setSessionContext(this);
    }

    public void load() throws Exception {
    }

    public void start() throws Exception {
        sessionManager.start();
    }

    public void stop() throws Exception {
        closed = true;
        sessionManager.stop();
    }

    public boolean isClosed() {
        return closed;
    }
}
