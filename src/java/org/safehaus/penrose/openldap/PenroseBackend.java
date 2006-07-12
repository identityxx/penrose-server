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
import java.io.FileReader;
import java.io.File;

import org.ietf.ldap.*;
import org.safehaus.penrose.openldap.config.ConfigurationItem;
import org.safehaus.penrose.openldap.config.NameValueItem;
import org.safehaus.penrose.openldap.config.SlapdConfig;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.PenroseFactory;
import org.safehaus.penrose.session.PenroseSession;
import org.safehaus.penrose.session.PenroseSearchResults;
import org.safehaus.penrose.session.*;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.config.PenroseConfigReader;
import org.openldap.backend.Backend;
import org.openldap.backend.Result;
import org.apache.log4j.xml.DOMConfigurator;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import javax.naming.directory.Attributes;
import javax.naming.directory.SearchControls;

/**
 * @author Endi S. Dewata
 */
public class PenroseBackend implements Backend {

    Logger log = LoggerFactory.getLogger(getClass());

    public String slapdConfig;

    public String suffixes[];
    public String schemaDn;

    public String rootDn;
    public String rootPassword;

    public String home;

    public Penrose penrose;

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

        log.debug("-------------------------------------------------------------------------------");
        log.debug("PenroseBackend.init();");

        PenroseConfigReader reader = new PenroseConfigReader((home == null ? "" : home+ File.separator)+"conf"+File.separator+"server.xml");
        PenroseConfig penroseConfig = reader.read();
        penroseConfig.setHome(home);

        //UserConfig rootUserConfig = penroseConfig.getRootUserConfig();
        //rootUserConfig.setDn(rootDn);
        //rootUserConfig.setPassword(rootPassword);

        PenroseFactory penroseFactory = PenroseFactory.getInstance();
        penrose = penroseFactory.createPenrose(penroseConfig);
        penrose.start();

        return LDAPException.SUCCESS;
    }

    public Collection getSchemaFiles() throws Exception {
        FileReader in = new FileReader(slapdConfig);
        List items = new SlapdConfig(in).getItems();

        List schemaFiles = new ArrayList();

        for (int i = 0; i < items.size(); i++) {
            ConfigurationItem ci = (ConfigurationItem) items.get(i);
            if (!(ci instanceof NameValueItem))
                continue;

            NameValueItem nvi = (NameValueItem) ci;
            String value = nvi.getValue();

            if (!nvi.getName().equals("include")) continue;

            schemaFiles.add(value);
        }

        return schemaFiles;
    }

    /**
     * Initialize server with schema DN.
     *
     * @param schemaDn
     */
    public void setSchema(String schemaDn) {
        log.debug("-------------------------------------------------------------------------------");
        log.debug("Penrose.setSchema(schemaDn);");
        log.debug(" schemaDN           : " + schemaDn);

        this.schemaDn = schemaDn;
    }

    /**
     * Initialize server with a set of suffixes.
     *
     * @param suffixes
     */
    public void setSuffix(String suffixes[]) {
           log.debug("-------------------------------------------------------------------------------");
           log.debug("PenroseBackend.setSuffix(suffixArray);");

        for (int i=0; i<suffixes.length; i++) {
            log.debug(" suffix            : "+suffixes[i]);
        }

        this.suffixes = suffixes;
    }

    /**
     * Initialize server with root DN and password.
     *
     * @param rootDn
     * @param rootPassword
     */
    public void setRoot(String rootDn, String rootPassword) {
        log.debug("-------------------------------------------------------------------------------");
        log.debug("PenroseBackend.setRoot(rootDn, rootPassword)");
        log.debug(" rootDN           : "+rootDn);
        log.debug(" rootPassword     : "+rootPassword);

        this.rootDn = rootDn;
        this.rootPassword = rootPassword;
    }

    /**
     * Get connection.
     * 
     * @param connectionId
     * @return connection
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
        PenroseSession session = (PenroseSession)sessions.remove(new Integer(connectionId));
        if (session != null) session.close();
    }

    /**
     * Set the location of slapd.conf.
     *
     * @param slapdConfig Location of slapd.conf.
     * @return return value
     * @throws Exception
     */
    public int setSlapdConfig(String slapdConfig) throws Exception {
        try {
            return setSlapdConfigImpl(slapdConfig);
        } catch (Throwable e) {
            log.error(e.getMessage(), e);
            return LDAPException.OPERATIONS_ERROR;
        }
    }

    public int setSlapdConfigImpl(String slapdConfig) throws Throwable {

        log.debug("-------------------------------------------------------------------------------");
        log.debug("PenroseBackend.setSlapdConfig(slapdConfig)");
        log.debug(" slapdConfig: "+slapdConfig);

        this.slapdConfig = slapdConfig;

        return LDAPException.SUCCESS;
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

        PenroseSession session = getSession(connectionId);
        if (session == null) {
            openConnection(connectionId);
            session = getSession(connectionId);
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

        PenroseSession session = getSession(connectionId);
        if (session == null) {
            openConnection(connectionId);
            session = getSession(connectionId);
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
    public Result search(
            int connectionId,
            String baseDn,
            String filter,
            SearchControls sc)
    throws Exception {

        PenroseSession session = getSession(connectionId);
        if (session == null) {
            openConnection(connectionId);
            session = getSession(connectionId);
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

        } catch (Throwable e) {
            log.error(e.getMessage(), e);

            results.setReturnCode(LDAPException.OPERATIONS_ERROR);
            results.close();
        }

        return new PenroseResult(results);
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

        PenroseSession session = getSession(connectionId);
        if (session == null) {
            openConnection(connectionId);
            session = getSession(connectionId);
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

        PenroseSession session = getSession(connectionId);
        if (session == null) {
            openConnection(connectionId);
            session = getSession(connectionId);
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

        PenroseSession session = getSession(connectionId);
        if (session == null) {
            openConnection(connectionId);
            session = getSession(connectionId);
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
            String newrdn)
    throws Exception {

        PenroseSession session = getSession(connectionId);
        if (session == null) {
            openConnection(connectionId);
            session = getSession(connectionId);
        }

        try {
            return session.modrdn(dn, newrdn);

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

        PenroseSession session = getSession(connectionId);
        if (session == null) {
            openConnection(connectionId);
            session = getSession(connectionId);
        }

        try {
            return session.compare(dn, attributeName, attributeValue);

        } catch (Throwable e) {
            log.error(e.getMessage(), e);
            return LDAPException.OPERATIONS_ERROR;
        }
    }
}
