/**
 * Copyright (c) 2000-2005, Identyx Corporation.
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

public class SessionManager implements SessionManagerMBean {

    public Logger log = LoggerFactory.getLogger(getClass());

    public final static String SESSION_ID_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";

    private PenroseConfig penroseConfig;
    private SessionConfig sessionConfig;

    public Map sessions = new LinkedHashMap();

    private int maxSessions;
    private int maxIdleTime; // minutes

    public void start() throws Exception {
        String s = sessionConfig.getParameter(SessionConfig.MAX_SESSIONS);
        maxSessions = s == null ? SessionConfig.DEFAULT_MAX_SESSIONS : Integer.parseInt(s);

        s = sessionConfig.getParameter(SessionConfig.MAX_IDLE_TIME);
        maxIdleTime = s == null ? SessionConfig.DEFAULT_MAX_IDLE_TIME : Integer.parseInt(s);
    }

    public void stop() throws Exception {
        log.debug("Removing all sessions");
        sessions.clear();
    }

    public synchronized PenroseSession newSession() {

        String sessionId = createSessionId();
        while (sessions.get(sessionId) != null) {
            sessionId = createSessionId();
        }

        return createSession(sessionId);
    }

    public synchronized PenroseSession createSession(String sessionId) {

        purge();

        if (sessions.size() >= maxSessions) return null;

        //log.debug("Creating session "+sessionId);
        PenroseSession session = new PenroseSession(this);
        session.setSessionId(sessionId);

        sessions.put(sessionId, session);

        return session;
    }

    public synchronized PenroseSession getSession(String sessionId) {

        purge();

        //log.debug("Retrieving session "+sessionId);
        return (PenroseSession)sessions.get(sessionId);
    }

    public synchronized PenroseSession removeSession(String sessionId) {

        purge();

        //log.debug("Removing session "+sessionId);
        return (PenroseSession)sessions.remove(sessionId);
    }

    public String createSessionId() {
        StringBuffer sb = new StringBuffer();
        for (int i=0; i<64; i++) {
            int index = (int)(SESSION_ID_CHARS.length()*Math.random());
            sb.append(SESSION_ID_CHARS.charAt(index));
        }
        return sb.toString();
    }

    public synchronized void purge() {
        Collection expiredSessions = new ArrayList();

        for (Iterator i=sessions.values().iterator(); i.hasNext(); ) {
            PenroseSession session = (PenroseSession)i.next();
            if (isExpired(session)) expiredSessions.add(session.getSessionId());
        }

        for (Iterator i=expiredSessions.iterator(); i.hasNext(); ) {
            String sessionId = (String)i.next();
            //log.debug("Removing session "+sessionId);
            sessions.remove(sessionId);
        }
    }

    public synchronized boolean isValid(PenroseSession session) {
        purge();
        if (session == null) return true;

        //log.debug("Valid sessions: "+sessions.keySet());
        return sessions.get(session.getSessionId()) != null;
    }

    public boolean isExpired(PenroseSession session) {
        if (session == null) return false;

        long idleTime = System.currentTimeMillis() - session.getLastActivityDate().getTime();
        //log.debug("Session "+session.getSessionId()+" idleTime = "+idleTime);

        return idleTime > maxIdleTime * 60 * 1000;
    }

    public synchronized void closeSession(PenroseSession session) {
        log.debug("Removing session "+session.getSessionId());
        sessions.remove(session.getSessionId());
    }

    public synchronized Collection getSessions() {
        purge();
        return sessions.values();
    }

    public int getNumberOfSessions() {
        return sessions.size();
    }

    public void setMaxSessions(int maxSessions) {
        this.maxSessions = maxSessions;
    }

    public int getMaxSessions() {
        return maxSessions;
    }

    public int getMaxIdleTime() {
        return maxIdleTime;
    }

    public void setMaxIdleTime(int maxIdleTime) {
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
        this.sessionConfig = penroseConfig.getSessionConfig();
    }
}