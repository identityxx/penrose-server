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

import org.safehaus.penrose.session.PenroseSession;

import java.util.*;

import javax.naming.directory.Attributes;
import javax.naming.directory.SearchControls;

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

    /**
     * Initialize server with schema DN.
     *
     * @param schemaDn
     */
    public void setSchema(String schemaDn);

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

    public void openConnection(int connectionId) throws Exception;
    public void closeConnection(int connectionId) throws Exception;

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
     * @param filter Search filter (e.g. objectClass=*).
     * @return return value
     * @throws Exception
     */
    public Result search(
            int connectionId,
            String baseDn,
            String filter,
            SearchControls sc)
    throws Exception;

    /**
     * Performs add operation.
     * 
     * @param connectionId Connection ID.
     * @param dn
     * @param attributes
     * @return return value
     * @throws Exception
     */
    public int add(
            int connectionId,
            String dn,
            Attributes attributes)
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
