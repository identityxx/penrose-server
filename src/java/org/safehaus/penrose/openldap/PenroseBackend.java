/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
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
import org.safehaus.penrose.PenroseConnection;
import org.safehaus.penrose.SearchResults;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.openldap.backend.Backend;
import org.openldap.backend.Result;

/**
 * @author Endi S. Dewata
 */
public class PenroseBackend implements Backend {

    public String slapdConfig;
    public Properties properties;

    public String trustedKeyStore;
    public String serverConfig;
    public String mappingConfigs[];

    public String suffixes[];
    public String schemaDn;

    public String rootDn;
    public String rootPassword;

    public String configHomeDirectory;
    public String realHomeDirectory;

    public Penrose penrose;

    public Map connections = new HashMap();

    public PenroseBackend() {
        File f = new File("conf/log4j.properties");
        if (f.exists()) PropertyConfigurator.configure(f.getAbsolutePath());
    }

    public int setHomeDirectory(String configHomeDirectory, String realHomeDirectory) {
        this.configHomeDirectory = configHomeDirectory;
        this.realHomeDirectory = realHomeDirectory;

        slapdConfig = this.realHomeDirectory + "/etc/openldap/slapd.conf";

        return LDAPException.SUCCESS;
    }

    public int setTrustedKeyStore(String trustedKeyStore) throws Exception {
        this.trustedKeyStore = trustedKeyStore;

        return LDAPException.SUCCESS;
    }

    public int setServerConfig(String serverConfig) throws Exception {
        this.serverConfig = serverConfig;

        return LDAPException.SUCCESS;
    }

    public int setMappingConfigs(String mappingConfigs[]) throws Exception {
        this.mappingConfigs = mappingConfigs;

        return LDAPException.SUCCESS;
    }

    /**
     * Initialize Penrose engine.
     *
     * @return return code
     * @throws Exception
     */
    public int init() throws Exception {
        Logger log = Logger.getLogger(PenroseBackend.class);

        try {
            return initImpl();
        } catch (Throwable e) {
            log.error(e.getMessage(), e);
            return LDAPException.OPERATIONS_ERROR;
        }
    }

    public int initImpl() throws Exception {
    	
        Logger log = Logger.getLogger(PenroseBackend.class);
        log.debug("-------------------------------------------------------------------------------");
        log.debug("PenroseBackend.init();");

        penrose = new Penrose();
        penrose.setRootDn(rootDn);
        penrose.setRootPassword(rootPassword);
        penrose.init();

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
        Logger log = Logger.getLogger(PenroseBackend.class);
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
        Logger log = Logger.getLogger(PenroseBackend.class);
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
        Logger log = Logger.getLogger(PenroseBackend.class);
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
    public PenroseConnection getConnection(int connectionId) throws Exception {
        return (PenroseConnection)connections.remove(new Integer(connectionId));
    }

    /**
     * Create connection.
     * 
     * @param connectionId
     */
    public void createConnection(int connectionId) throws Exception {
        PenroseConnection connection = penrose.openConnection();
        connections.put(new Integer(connectionId), connection);
    }

    /**
     * Remove connection.
     * 
     * @param connectionId
     */
    public void removeConnection(int connectionId) throws Exception {
        PenroseConnection connection = (PenroseConnection)connections.remove(new Integer(connectionId));
        connection.close();
    }
    
    /**
     * Set the location of slapd.conf.
     *
     * @param slapdConfig Location of slapd.conf.
     * @return return value
     * @throws Exception
     */
    public int setSlapdConfig(String slapdConfig) throws Exception {
        Logger log = Logger.getLogger(Penrose.ENGINE_LOGGER);
        try {
            return setSlapdConfigImpl(slapdConfig);
        } catch (Throwable e) {
            log.error(e.getMessage(), e);
            return LDAPException.OPERATIONS_ERROR;
        }
    }

    public int setSlapdConfigImpl(String slapdConfig) throws Throwable {

        Logger log = Logger.getLogger(PenroseBackend.class);
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

        Logger log = Logger.getLogger(PenroseBackend.class);
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

        Logger log = Logger.getLogger(PenroseBackend.class);
        PenroseConnection connection = getConnection(connectionId);
        if (connection == null) {
            createConnection(connectionId);
            connection = getConnection(connectionId);
        }

        try {
            return connection.bind(dn, password);

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

        Logger log = Logger.getLogger(PenroseBackend.class);
        PenroseConnection connection = getConnection(connectionId);
        if (connection == null) {
            createConnection(connectionId);
            connection = getConnection(connectionId);
        }

        try {
            return connection.unbind();

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
            Collection attributeNames)
    throws Exception {

        Logger log = Logger.getLogger(PenroseBackend.class);
        PenroseConnection connection = getConnection(connectionId);
        if (connection == null) {
            createConnection(connectionId);
            connection = getConnection(connectionId);
        }

        SearchResults results;

        try {
            results = connection.search(base, scope, LDAPSearchConstraints.DEREF_ALWAYS, filter, attributeNames);

        } catch (Throwable e) {
            log.error(e.getMessage(), e);

            results = new SearchResults();
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
            Collection attributeNames)
    throws Exception {

        Logger log = Logger.getLogger(PenroseBackend.class);
        PenroseConnection connection = getConnection(connectionId);
        if (connection == null) {
            createConnection(connectionId);
            connection = getConnection(connectionId);
        }

        SearchResults results;

        try {
            results = connection.search(base, scope, deref, filter, attributeNames);

        } catch (Throwable e) {
            log.error(e.getMessage(), e);

            results = new SearchResults();
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

        Logger log = Logger.getLogger(PenroseBackend.class);
        PenroseConnection connection = getConnection(connectionId);
        if (connection == null) {
            createConnection(connectionId);
            connection = getConnection(connectionId);
        }

        try {
            return connection.add(entry);

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

        Logger log = Logger.getLogger(PenroseBackend.class);
        PenroseConnection connection = getConnection(connectionId);
        if (connection == null) {
            createConnection(connectionId);
            connection = getConnection(connectionId);
        }

        try {
            return connection.delete(dn);

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

        Logger log = Logger.getLogger(PenroseBackend.class);
        PenroseConnection connection = getConnection(connectionId);
        if (connection == null) {
            createConnection(connectionId);
            connection = getConnection(connectionId);
        }

        try {
            return connection.modify(dn, modifications);

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

        Logger log = Logger.getLogger(PenroseBackend.class);
        PenroseConnection connection = getConnection(connectionId);
        if (connection == null) {
            createConnection(connectionId);
            connection = getConnection(connectionId);
        }

        try {
            return connection.compare(dn, attributeName, attributeValue);

        } catch (Throwable e) {
            log.error(e.getMessage(), e);
            return LDAPException.OPERATIONS_ERROR;
        }
    }
}
