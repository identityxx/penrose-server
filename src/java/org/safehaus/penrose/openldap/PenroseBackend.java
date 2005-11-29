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

import org.ietf.ldap.*;
import org.safehaus.penrose.openldap.config.ConfigurationItem;
import org.safehaus.penrose.openldap.config.NameValueItem;
import org.safehaus.penrose.openldap.config.SlapdConfig;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.session.PenroseSession;
import org.safehaus.penrose.session.PenroseSearchResults;
import org.safehaus.penrose.session.*;
import org.safehaus.penrose.config.PenroseConfig;
import org.apache.log4j.Logger;
import org.openldap.backend.Backend;
import org.openldap.backend.Result;

/**
 * @author Endi S. Dewata
 */
public class PenroseBackend implements Backend {

    public String slapdConfig;
    public Properties properties;

    public String suffixes[];
    public String schemaDn;

    public String rootDn;
    public String rootPassword;

    public String configHomeDirectory;
    public String realHomeDirectory;

    public Penrose penrose;

    public Map connections = new HashMap();

    public PenroseBackend() {
        //File f = new File("conf/log4j.properties");
        //if (f.exists()) PropertyConfigurator.configure(f.getAbsolutePath());
    }

    public int setHomeDirectory(String configHomeDirectory, String realHomeDirectory) {
        this.configHomeDirectory = configHomeDirectory;
        this.realHomeDirectory = realHomeDirectory;

        slapdConfig = this.realHomeDirectory + "/etc/openldap/slapd.conf";

        return LDAPException.SUCCESS;
    }

    /**
     * Initialize Penrose engine.
     *
     * @return return code
     * @throws Exception
     */
    public int init() throws Exception {
        Logger log = Logger.getLogger(getClass());

        try {
            return initImpl();
        } catch (Throwable e) {
            log.error(e.getMessage(), e);
            return LDAPException.OPERATIONS_ERROR;
        }
    }

    public int initImpl() throws Exception {
    	
        Logger log = Logger.getLogger(getClass());
        log.debug("-------------------------------------------------------------------------------");
        log.debug("PenroseBackend.init();");

        penrose = new Penrose(realHomeDirectory);
        penrose.start();

        PenroseConfig penroseConfig = penrose.getPenroseConfig();
        penroseConfig.setRootDn(rootDn);
        penroseConfig.setRootPassword(rootPassword);

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

            if (!nvi.getName().equals("include"))
                continue;

            if (value.startsWith(configHomeDirectory)) {
                value = realHomeDirectory+value.substring(configHomeDirectory.length());
            }

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
        Logger log = Logger.getLogger(getClass());
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
        Logger log = Logger.getLogger(getClass());
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
        Logger log = Logger.getLogger(getClass());
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
    public PenroseSession getConnection(int connectionId) throws Exception {
        return (PenroseSession)connections.remove(new Integer(connectionId));
    }

    /**
     * Create connection.
     * 
     * @param connectionId
     */
    public void createConnection(int connectionId) throws Exception {
        PenroseSession session = penrose.newSession();
        connections.put(new Integer(connectionId), session);
    }

    /**
     * Remove connection.
     * 
     * @param connectionId
     */
    public void removeConnection(int connectionId) throws Exception {
        PenroseSession session = (PenroseSession)connections.remove(new Integer(connectionId));
        session.close();
    }
    
    /**
     * Set the location of slapd.conf.
     *
     * @param slapdConfig Location of slapd.conf.
     * @return return value
     * @throws Exception
     */
    public int setSlapdConfig(String slapdConfig) throws Exception {
        Logger log = Logger.getLogger(getClass());
        try {
            return setSlapdConfigImpl(slapdConfig);
        } catch (Throwable e) {
            log.error(e.getMessage(), e);
            return LDAPException.OPERATIONS_ERROR;
        }
    }

    public int setSlapdConfigImpl(String slapdConfig) throws Throwable {

        Logger log = Logger.getLogger(getClass());
    	log.debug("-------------------------------------------------------------------------------");
    	log.debug("PenroseBackend.setSlapdConfig(slapdConfig)");
    	log.debug(" slapdConfig: "+slapdConfig);

        this.slapdConfig = slapdConfig;

        return LDAPException.SUCCESS;
    }

    /**
     * Set the properties.
     *
     * @param properties
     * @return return value
     * @throws Exception
     */
    public int setProperties(Properties properties) throws Exception {

        Logger log = Logger.getLogger(getClass());
    	log.debug("-------------------------------------------------------------------------------");
    	log.debug("PenroseBackend.setProperties(properties)");

        this.properties = properties;

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

        Logger log = Logger.getLogger(getClass());
        PenroseSession session = getConnection(connectionId);
        if (session == null) {
            createConnection(connectionId);
            session = getConnection(connectionId);
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

        Logger log = Logger.getLogger(getClass());
        PenroseSession session = getConnection(connectionId);
        if (session == null) {
            createConnection(connectionId);
            session = getConnection(connectionId);
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
     * @param base
     * @param scope
     * @param filter
     * @param attributeNames
     * @return search result
     * @throws Exception
     */
    public Result search(
            int connectionId,
            String base,
            int scope,
            String filter,
            String[] attributeNames)
    throws Exception {

        Logger log = Logger.getLogger(getClass());
        PenroseSession session = getConnection(connectionId);
        if (session == null) {
            createConnection(connectionId);
            session = getConnection(connectionId);
        }

        PenroseSearchResults results;

        try {
            PenroseSearchControls sc = new PenroseSearchControls();
            sc.setScope(scope);
            sc.setDereference(PenroseSearchControls.DEREF_ALWAYS);
            sc.setAttributes(attributeNames);

            results = session.search(base, filter, sc);

        } catch (Throwable e) {
            log.error(e.getMessage(), e);

            results = new PenroseSearchResults();
            results.setReturnCode(LDAPException.OPERATIONS_ERROR);
            results.close();
        }

        return new PenroseResult(results);
    }

    /**
     * Performs search operation.
     *
     * @param connectionId
     * @param base
     * @param scope
     * @param deref
     * @param filter
     * @param attributeNames
     * @return search result
     * @throws Exception
     */
    public Result search(
            int connectionId,
            String base,
            int scope,
            int deref,
            String filter,
            String[] attributeNames)
    throws Exception {

        Logger log = Logger.getLogger(getClass());
        PenroseSession session = getConnection(connectionId);
        if (session == null) {
            createConnection(connectionId);
            session = getConnection(connectionId);
        }

        PenroseSearchResults results;

        try {
            PenroseSearchControls sc = new PenroseSearchControls();
            sc.setScope(scope);
            sc.setDereference(PenroseSearchControls.DEREF_ALWAYS);
            sc.setAttributes(attributeNames);

            results = session.search(base, filter, sc);

        } catch (Throwable e) {
            log.error(e.getMessage(), e);

            results = new PenroseSearchResults();
            results.setReturnCode(LDAPException.OPERATIONS_ERROR);
            results.close();
        }

        return new PenroseResult(results);
    }

    /**
     * Performs add operation.
     * 
     * @param connectionId
     * @param entry
     * @return return code
     * @throws Exception
     */
    public int add(
            int connectionId,
            LDAPEntry entry)
    throws Exception {

        Logger log = Logger.getLogger(getClass());
        PenroseSession session = getConnection(connectionId);
        if (session == null) {
            createConnection(connectionId);
            session = getConnection(connectionId);
        }

        try {
            return session.add(entry);

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

        Logger log = Logger.getLogger(getClass());
        PenroseSession session = getConnection(connectionId);
        if (session == null) {
            createConnection(connectionId);
            session = getConnection(connectionId);
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
            List modifications)
    throws Exception {

        Logger log = Logger.getLogger(getClass());
        PenroseSession session = getConnection(connectionId);
        if (session == null) {
            createConnection(connectionId);
            session = getConnection(connectionId);
        }

        try {
            return session.modify(dn, modifications);

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
            String attributeValue)
    throws Exception {

        Logger log = Logger.getLogger(getClass());
        PenroseSession session = getConnection(connectionId);
        if (session == null) {
            createConnection(connectionId);
            session = getConnection(connectionId);
        }

        try {
            return session.compare(dn, attributeName, attributeValue);

        } catch (Throwable e) {
            log.error(e.getMessage(), e);
            return LDAPException.OPERATIONS_ERROR;
        }
    }
}
