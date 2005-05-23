/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.openldap.backend;

import java.util.*;

import org.ietf.ldap.LDAPEntry;

/**
 * @author Endi S. Dewata
 */
public interface Backend {

    /**
     * Initialize Penrose engine.
     *
     * @return
     * @throws Exception
     */
    public int init() throws Exception;

    public int setHomeDirectory(String configHomeDirectory, String realHomeDirectory);
    public int setTrustedKeyStore(String trustedKeyStore) throws Exception;
    public int setServerConfig(String serverConfig) throws Exception;
    public int setMappingConfigs(String mappingConfigs[]) throws Exception;


    /**
     * Initialize server with schema DN.
     *
     * @param schemaDn
     */
    public void setSchema(String schemaDn);

    /**
     * Create connection.
     *
     * @param connectionId
     */
    public void createConnection(int connectionId) throws Exception;

    /**
     * Remove connection.
     *
     * @param connectionId
     */
    public void removeConnection(int connectionId) throws Exception;

    /**
     * Initialize server with a set of suffixes.
     *
     * @param suffixes Directory suffix (e.g. dc=penrose, dc=safehaus, dc=org).
     */
    public void setSuffix(String suffixes[]) throws Exception;

    /**
     * Initialize server with root DN and password.
     *
     * @param rootDn Root DN (e.g. cn=Manager, dc=penrose, dc=safehaus, dc=org).
     * @param rootPassword Root password.
     */
    public void setRoot(String rootDn, String rootPassword) throws Exception;

    /**
     * Set the location of slapd.conf.
     *
     * @param slapdConfig Location of slapd.conf.
     * @return
     * @throws Exception
     */
    public int setSlapdConfig(String slapdConfig) throws Exception;

    /**
     * Set the properties.
     *
     * @param properties
     * @return
     * @throws Exception
     */
    public int setProperties(Properties properties) throws Exception;

    /**
     * Performs bind operation.
     *
     * @param connectionId Connection ID.
     * @param dn Bind DN.
     * @param password Bind password.
     * @return
     * @throws Exception
     */
    public int bind(
    		int connectionId,
			String dn,
			String password)
    throws Exception;

    /**
     * Performs unbind operation.
     * 
     * @param connectionId Connection ID.
     * @return
     * @throws Exception
     */
    public int unbind(
    		int connectionId)
    throws Exception;

    /**
     * Performs search operation.
     *
     * @param connectionId Connection ID.
     * @param baseDn Base DN.
     * @param scope Scope (0: base, 1: one level, 2: subtree).
     * @param filter Search filter (e.g. objectClass=*).
     * @param attributeNames Attribute names to be returned.
     * @return
     * @throws Exception
     */
    public Result search(
    		int connectionId,
			String baseDn,
			int scope,
			String filter,
			Collection attributeNames)
    throws Exception;

    /**
     * Performs search operation.
     *
     * @param connectionId Connection ID.
     * @param baseDn Base DN.
     * @param scope Scope (0: base, 1: one level, 2: subtree).
     * @param deref Dereference referrals.
     * @param filter Search filter (e.g. objectClass=*).
     * @param attributeNames Attribute names to be returned.
     * @return
     * @throws Exception
     */
    public Result search(
    		int connectionId,
			String baseDn,
			int scope,
			int deref,
			String filter,
			Collection attributeNames)
    throws Exception;

    /**
     * Performs add operation.
     * 
     * @param connectionId Connection ID.
     * @param entry Entry to be added.
     * @return
     * @throws Exception
     */
    public int add(
    		int connectionId,
			LDAPEntry entry)
    throws Exception;

    /**
     * Performs delete operation.
     * 
     * @param connectionId Connection ID.
     * @param dn DN of the entry to be deleted.
     * @return
     * @throws Exception
     */
    public int delete(
    		int connectionId,
			String dn)
    throws Exception;

    /**
     * Performs modify operation.
     * 
     * @param connectionId Connection ID.
     * @param dn DN of the entry to be modified.
     * @param modifications Modification list.
     * @return
     * @throws Exception
     */
    public int modify(
    		int connectionId,
			String dn,
			List modifications)
    throws Exception;

    /**
     * Performs compare operation.
     * 
     * @param connectionId Connection ID.
     * @param dn DN of the entry to be compared.
     * @param attributeName Attribute name to compare.
     * @param attributeValue Attribute value to compare.
     * @return
     * @throws Exception
     */
    public int compare(
    		int connectionId,
			String dn,
			String attributeName,
			String attributeValue)
    throws Exception;
}
