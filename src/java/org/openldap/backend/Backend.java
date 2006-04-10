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
     * @return return value
     * @throws Exception
     */
    public int init() throws Exception;

    public int setHomeDirectory(String configHomeDirectory, String realHomeDirectory);


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
     * @return return value
     * @throws Exception
     */
    public int setSlapdConfig(String slapdConfig) throws Exception;

    /**
     * Set the properties.
     *
     * @param properties
     * @return return value
     * @throws Exception
     */
    public int setProperties(Properties properties) throws Exception;

    /**
     * Performs bind operation.
     *
     * @param connectionId Connection ID.
     * @param dn Bind DN.
     * @param password Bind password.
     * @return return value
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
     * @return return value
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
     * @return return value
     * @throws Exception
     */
    public Result search(
    		int connectionId,
			String baseDn,
			int scope,
			String filter,
			String[] attributeNames)
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
     * @return return value
     * @throws Exception
     */
    public Result search(
    		int connectionId,
			String baseDn,
			int scope,
			int deref,
			String filter,
			String[] attributeNames)
    throws Exception;

    /**
     * Performs add operation.
     * 
     * @param connectionId Connection ID.
     * @param entry Entry to be added.
     * @return return value
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
     * @return return value
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
     * @return return value
     * @throws Exception
     */
    public int modify(
    		int connectionId,
			String dn,
			Collection modifications)
    throws Exception;

    /**
     * Performs compare operation.
     * 
     * @param connectionId Connection ID.
     * @param dn DN of the entry to be compared.
     * @param attributeName Attribute name to compare.
     * @param attributeValue Attribute value to compare.
     * @return return value
     * @throws Exception
     */
    public int compare(
    		int connectionId,
			String dn,
			String attributeName,
			Object attributeValue)
    throws Exception;
}
