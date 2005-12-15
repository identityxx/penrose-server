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

import org.apache.log4j.Logger;
import org.safehaus.penrose.handler.SessionHandler;
import org.safehaus.penrose.handler.SessionHandlerConfig;

public class SessionManager {

	public Logger log = Logger.getLogger(getClass());

    public final static String SESSION_ID_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";

    private SessionHandler sessionHandler;

    public Map sessions = new LinkedHashMap();

    private int maxSessions;
    private int maxIdleTime; // minutes

	public SessionManager() {
	}

	public void start() throws Exception {
        SessionHandlerConfig sessionHandlerConfig = sessionHandler.getHandlerConfig();

        String s = sessionHandlerConfig.getParameter(SessionHandlerConfig.MAX_SESSIONS);
        maxSessions = s == null ? SessionHandlerConfig.DEFAULT_MAX_SESSIONS : Integer.parseInt(s);

        s = sessionHandlerConfig.getParameter(SessionHandlerConfig.MAX_IDLE_TIME);
        maxIdleTime = s == null ? SessionHandlerConfig.DEFAULT_MAX_IDLE_TIME : Integer.parseInt(s);
	}

    public void stop() throws Exception {
        log.info("Removing all sessions");
        sessions.clear();
    }

    public synchronized PenroseSession newSession() {

        purge();

        if (sessions.size() >= maxSessions) return null;

        String sessionId = createSessionId();
        while (sessions.get(sessionId) != null) {
            sessionId = createSessionId();
        }

        log.info("Creating session "+sessionId);
        PenroseSession session = new PenroseSession();
        session.setSessionId(sessionId);
        session.setHandler(sessionHandler);

        sessions.put(sessionId, session);

        return session;
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
            log.info("Removing session "+sessionId);
            sessions.remove(sessionId);
        }
    }

    public synchronized boolean isValid(PenroseSession session) {
        purge();
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
        log.info("Removing session "+session.getSessionId());
        sessions.remove(session.getSessionId());
    }

    public synchronized Collection getSessions() {
        purge();
        return sessions.values();
    }

    public SessionHandler getSessionHandler() {
        return sessionHandler;
    }

    public void setSessionHandler(SessionHandler sessionHandler) {
        this.sessionHandler = sessionHandler;
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
}