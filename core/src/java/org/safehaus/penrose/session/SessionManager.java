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

import java.util.*;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.naming.PenroseContext;

public class SessionManager implements SessionManagerMBean {

    public Logger log = LoggerFactory.getLogger(getClass());

    private PenroseConfig penroseConfig;
    private PenroseContext penroseContext;
    private SessionContext sessionContext;

    private SessionConfig sessionConfig;

    //public Map<Object,Session> sessions = new LinkedHashMap<Object,Session>();

    public long sessionCounter;
    private Integer maxSessions;
    private Integer maxIdleTime; // minutes

    public SessionManager() {
    }

    public void start() throws Exception {
        String s = sessionConfig.getParameter(SessionConfig.MAX_SESSIONS);
        if (s != null) maxSessions = new Integer(s);

        s = sessionConfig.getParameter(SessionConfig.MAX_IDLE_TIME);
        if (s != null) maxIdleTime = new Integer(s);
    }

    public void stop() throws Exception {
        //sessions.clear();
    }

    public synchronized Session newSession() throws Exception {

        Object sessionId = createSessionId();
        return createSession(sessionId);
    }

    public synchronized Session newAdminSession() throws Exception {
        Session session = newSession();
        session.setBindDn(penroseConfig.getRootDn());
        session.setRootUser(true);
        return session;
    }

    public synchronized Session createSession(Object sessionId) throws Exception {

/*
        if (maxSessions != null && sessions.size() >= maxSessions) {
            throw new Exception("Maximum number of sessions has been reached.");
        }
*/
        //log.debug("Creating session "+sessionId);
        Session session = new Session(this);
        session.setSessionId(sessionId);
        session.setPenroseConfig(penroseConfig);
        session.setPenroseContext(penroseContext);
        session.setSessionContext(sessionContext);
        session.init();

        //sessions.put(sessionId, session);

        return session;
    }

    public synchronized Session getSession(Object sessionId) {

        //log.debug("Retrieving session "+sessionId);
        //return sessions.get(sessionId);
        return null;
    }

    public synchronized Session removeSession(Object sessionId) {

        //log.debug("Removing session "+sessionId);
        //return sessions.remove(sessionId);
        return null;
    }

    public Object createSessionId() {
        Long sessionId = sessionCounter;
        sessionCounter++;
        return sessionId;
    }

    public synchronized Collection<Session> getSessions() {
        //return sessions.values();
        return null;
    }

    public int getNumberOfSessions() {
        return 0; //sessions.size();
    }

    public void setMaxSessions(Integer maxSessions) {
        this.maxSessions = maxSessions;
    }

    public Integer getMaxSessions() {
        return maxSessions;
    }

    public Integer getMaxIdleTime() {
        return maxIdleTime;
    }

    public void setMaxIdleTime(Integer maxIdleTime) {
        this.maxIdleTime = maxIdleTime;
    }

    public SessionConfig getSessionManagerConfig() {
        return sessionConfig;
    }

    public void setSessionManagerConfig(SessionConfig sessionConfig) {
        this.sessionConfig = sessionConfig;
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