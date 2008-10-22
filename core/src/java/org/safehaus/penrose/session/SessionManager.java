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

import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.naming.PenroseContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class SessionManager implements SessionManagerMBean {

    public Logger log = LoggerFactory.getLogger(getClass());

    private PenroseConfig penroseConfig;
    private PenroseContext penroseContext;
    private SessionContext sessionContext;

    private SessionConfig sessionConfig;

    public long sessionCounter;

    public Map<String,Session> sessions = Collections.synchronizedMap(new HashMap<String,Session>());

    public SessionManager() {
    }

    public void start() throws Exception {
    }

    public void stop() throws Exception {

        Collection<Session> list = new ArrayList<Session>();
        list.addAll(sessions.values());

        for (Session session : list) {
            session.close();
        }
    }

    public Session createSession() throws Exception {
        String sessionId = createSessionName();
        return createSession(sessionId);
    }

    public Session createAdminSession() throws Exception {
        Session session = createSession();
        session.setBindDn(penroseConfig.getRootDn());
        session.setRootUser(true);
        return session;
    }

    public Session createSession(String sessionName) throws Exception {

        Session session = new Session();
        session.setSessionName(sessionName);
        session.setPenroseConfig(penroseConfig);
        session.setPenroseContext(penroseContext);
        session.setSessionContext(sessionContext);
        session.init();

        sessions.put(sessionName, session);

        return session;
    }

    public Collection<String> getSessionNames() {
        return sessions.keySet();
    }
    
    public Session getSession(String sessionName) {
        return sessions.get(sessionName);
    }

    public Session removeSession(String sessionName) throws Exception {
        return sessions.remove(sessionName);
    }

    public synchronized String createSessionName() {
        String sessionId = "session-"+sessionCounter;
        if (sessionCounter == Long.MAX_VALUE) {
            sessionCounter = 0;
        } else {
            sessionCounter++;
        }
        return sessionId;
    }

    public SessionConfig getSessionConfig() {
        return sessionConfig;
    }

    public void setSessionConfig(SessionConfig sessionConfig) {
        this.sessionConfig = sessionConfig;
    }

    public PenroseConfig getPenroseConfig() {
        return penroseConfig;
    }

    public void setPenroseConfig(PenroseConfig penroseConfig) {
        this.penroseConfig = penroseConfig;

        sessionConfig = penroseConfig.getSessionConfig();
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
    }
}