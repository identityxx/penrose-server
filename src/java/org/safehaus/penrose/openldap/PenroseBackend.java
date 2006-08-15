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
package org.safehaus.penrose.openldap;

import java.util.*;
import java.io.File;

import org.ietf.ldap.*;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.PenroseServer;
import org.safehaus.penrose.service.ServiceConfig;
import org.safehaus.penrose.session.PenroseSession;
import org.safehaus.penrose.session.PenroseSearchResults;
import org.safehaus.penrose.session.*;
import org.safehaus.penrose.config.PenroseConfig;
import org.openldap.backend.Backend;
import org.openldap.backend.Results;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;

import javax.naming.directory.Attributes;
import javax.naming.directory.SearchControls;

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

    /**
     * Get session.
     * 
     * @param connectionId
     * @return session
     */
    public PenroseSession getSession(int connectionId) throws Exception {
        return (PenroseSession)sessions.get(new Integer(connectionId));
    }

    /**
     * Create connection.
     * 
     * @param connectionId
     */
    public void openConnection(int connectionId) throws Exception {
        log.debug("openConnection("+connectionId+")");
        Penrose penrose = penroseServer.getPenrose();
        PenroseSession session = penrose.newSession();
        if (session == null) throw new Exception("Unable to create session.");
        sessions.put(new Integer(connectionId), session);
    }

    /**
     * Remove connection.
     * 
     * @param connectionId
     */
    public void closeConnection(int connectionId) throws Exception {
        log.debug("closeConnection("+connectionId+")");
        PenroseSession session = (PenroseSession)sessions.remove(new Integer(connectionId));
        if (session != null) session.close();
    }

    /**
     * Performs bind operation.
     *
     * @param connectionId
     * @param dn
     * @param password
     * @return return code
     * @throws Exception
     */
    public int bind(int connectionId, String dn, String password) throws Exception {

        log.debug("bind("+connectionId+", \""+dn+", \""+password+"\")");

        PenroseSession session = getSession(connectionId);
        if (session == null) {
            log.debug("Invalid connection ID: "+connectionId);
            return LDAPException.OPERATIONS_ERROR;
        }

        try {
            return session.bind(dn, password);

        } catch (Throwable e) {
            log.error(e.getMessage(), e);
            return LDAPException.OPERATIONS_ERROR;
        }
    }

    /**
     * Performs unbind operation.
     * 
     * @param connectionId
     * @return return value
     * @throws Exception
     */
    public int unbind(int connectionId) throws Exception {

        log.debug("unbind("+connectionId+")");

        PenroseSession session = getSession(connectionId);
        if (session == null) {
            log.debug("Invalid connection ID: "+connectionId);
            return LDAPException.OPERATIONS_ERROR;
        }

        try {
            return session.unbind();

        } catch (Throwable e) {
            log.error(e.getMessage(), e);
            return LDAPException.OPERATIONS_ERROR;
        }
    }

    /**
     * Performs search operation.
     *
     * @param connectionId
     * @param baseDn
     * @param filter
     * @return search result
     * @throws Exception
     */
    public Results search(
            int connectionId,
            String baseDn,
            String filter,
            SearchControls sc)
    throws Exception {

        log.debug("search("+connectionId+", \""+baseDn+"\", \""+filter+"\", sc)");

        PenroseSession session = getSession(connectionId);
        if (session == null) {
            log.debug("Invalid connection ID: "+connectionId);
            return null;
        }

        PenroseSearchResults results = new PenroseSearchResults();

        try {
            PenroseSearchControls psc = new PenroseSearchControls();
            psc.setScope(sc.getSearchScope());
            psc.setSizeLimit(sc.getCountLimit());
            psc.setTimeLimit(sc.getTimeLimit());
            psc.setDereference(PenroseSearchControls.DEREF_ALWAYS);
            psc.setAttributes(sc.getReturningAttributes());

            int rc = session.search(baseDn, filter, psc, results);
            results.setReturnCode(rc);

        } catch (Throwable e) {
            log.error(e.getMessage(), e);

            results.setReturnCode(LDAPException.OPERATIONS_ERROR);
            results.close();
        }

        return new PenroseResults(results);
    }

    /**
     * Performs add operation.
     * 
     * @param connectionId
     * @param dn
     * @param attributes
     * @return return code
     * @throws Exception
     */
    public int add(
            int connectionId,
            String dn,
            Attributes attributes)
    throws Exception {

        log.debug("add("+connectionId+")");

        PenroseSession session = getSession(connectionId);
        if (session == null) {
            log.debug("Invalid connection ID: "+connectionId);
            return LDAPException.OPERATIONS_ERROR;
        }

        try {
            return session.add(dn, attributes);

        } catch (Throwable e) {
            log.error(e.getMessage(), e);
            return LDAPException.OPERATIONS_ERROR;
        }
    }

    /**
     * Performs delete operation.
     * 
     * @param connectionId
     * @param dn
     * @return return code
     * @throws Exception
     */
    public int delete(
            int connectionId,
            String dn)
    throws Exception {

        log.debug("delete("+connectionId+")");

        PenroseSession session = getSession(connectionId);
        if (session == null) {
            log.debug("Invalid connection ID: "+connectionId);
            return LDAPException.OPERATIONS_ERROR;
        }

        try {
            return session.delete(dn);

        } catch (Throwable e) {
            log.error(e.getMessage(), e);
            return LDAPException.OPERATIONS_ERROR;
        }
    }

    /**
     * Performs modify operation.
     * 
     * @param connectionId
     * @param dn
     * @param modifications
     * @return return code
     * @throws Exception
     */
    public int modify(
            int connectionId,
            String dn,
            Collection modifications)
    throws Exception {

        log.debug("modify("+connectionId+")");

        PenroseSession session = getSession(connectionId);
        if (session == null) {
            log.debug("Invalid connection ID: "+connectionId);
            return LDAPException.OPERATIONS_ERROR;
        }

        try {
            return session.modify(dn, modifications);

        } catch (Throwable e) {
            log.error(e.getMessage(), e);
            return LDAPException.OPERATIONS_ERROR;
        }
    }

    /**
     * Performs modrdn operation.
     *
     * @param connectionId
     * @param dn
     * @param newrdn
     * @return return code
     * @throws Exception
     */
    public int modrdn(
            int connectionId,
            String dn,
            String newrdn,
            boolean deleteOldRdn)
    throws Exception {

        log.debug("modrdn("+connectionId+")");

        PenroseSession session = getSession(connectionId);
        if (session == null) {
            log.debug("Invalid connection ID: "+connectionId);
            return LDAPException.OPERATIONS_ERROR;
        }

        try {
            return session.modrdn(dn, newrdn, deleteOldRdn);

        } catch (Throwable e) {
            log.error(e.getMessage(), e);
            return LDAPException.OPERATIONS_ERROR;
        }
    }

    /**
     * Performs compare operation.
     * 
     * @param connectionId
     * @param dn
     * @param attributeName
     * @param attributeValue
     * @return return code
     * @throws Exception
     */
    public int compare(
            int connectionId,
            String dn,
            String attributeName,
            Object attributeValue)
    throws Exception {

        log.debug("compare("+connectionId+")");

        PenroseSession session = getSession(connectionId);
        if (session == null) {
            log.debug("Invalid connection ID: "+connectionId);
            return LDAPException.OPERATIONS_ERROR;
        }

        try {
            return session.compare(dn, attributeName, attributeValue);

        } catch (Throwable e) {
            log.error(e.getMessage(), e);
            return LDAPException.OPERATIONS_ERROR;
        }
    }
}
