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
package org.safehaus.penrose.backend;

import java.util.*;
import java.io.File;

import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.server.PenroseServer;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.service.ServiceConfig;
import com.identyx.javabackend.Backend;
import com.identyx.javabackend.Session;
import org.apache.log4j.xml.DOMConfigurator;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import com.novell.ldap.LDAPException;

/**
 * @author Endi S. Dewata
 */
public class PenroseBackend implements Backend {

    Logger log = LoggerFactory.getLogger(getClass());

    public String home;

    public PenroseServer penroseServer;

    public Map sessions = new HashMap();

    public PenroseBackend() {
        home = System.getProperty("penrose.home");

        File f = new File((home == null ? "" : home+File.separator)+"conf"+File.separator+"log4j.xml");
        if (f.exists()) DOMConfigurator.configure(f.getAbsolutePath());
    }

    /**
     * Initialize Penrose engine.
     *
     * @return return code
     * @throws Exception
     */
    public int init() throws Exception {
        try {
            return initImpl();
        } catch (Throwable e) {
            log.error(e.getMessage(), e);
            return LDAPException.OPERATIONS_ERROR;
        }
    }

    public int initImpl() throws Exception {

        penroseServer = new PenroseServer(home);

        PenroseConfig penroseConfig = penroseServer.getPenroseConfig();

        ServiceConfig ldapServiceConfig = penroseConfig.getServiceConfig("LDAP");
        ldapServiceConfig.setEnabled(false);

        penroseServer.start();

        return LDAPException.SUCCESS;
    }

    public boolean contains(String dn) throws Exception {
        Penrose penrose = penroseServer.getPenrose();
        PartitionManager partitionManager = penrose.getPartitionManager();
        return partitionManager.getPartitionByDn(dn) != null;
    }

    /**
     * Get session.
     *
     * @param connectionId
     * @return session
     */
    public Session getSession(long connectionId) throws Exception {
        return (PenroseSession)sessions.get(new Long(connectionId));
    }

    /**
     * Create session.
     *
     * @param connectionId
     */
    public Session createSession(long connectionId) throws Exception {
        log.debug("openConnection("+connectionId+")");
        Penrose penrose = penroseServer.getPenrose();
        PenroseSession session = new PenroseSession(penrose.newSession());
        if (session == null) throw new Exception("Unable to create session.");
        sessions.put(new Long(connectionId), session);
        return session;
    }

    /**
     * Close session.
     *
     * @param connectionId
     */
    public void closeSession(long connectionId) throws Exception {
        log.debug("closeConnection("+connectionId+")");
        PenroseSession session = (PenroseSession)sessions.remove(new Long(connectionId));
        if (session != null) session.close();
    }

}
