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
package org.safehaus.penrose.handler;

import org.ietf.ldap.LDAPException;
import org.safehaus.penrose.PenroseConnection;
import org.safehaus.penrose.SearchResults;

import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class SearchThread implements Runnable {

    public SearchHandler searchHandler;
    public PenroseConnection connection;
    public String base;
    public int scope;
    public int deref;
    public String filter;
    public Collection attributeNames;
    public SearchResults results;

    public SearchThread(
            SearchHandler searchHandler,
            PenroseConnection connection,
            String base,
            int scope,
            int deref,
            String filter,
            Collection attributeNames,
            SearchResults results)
    {

        this.searchHandler = searchHandler;
        this.connection = connection;
        this.base = base;
        this.scope = scope;
        this.deref = deref;
        this.filter = filter;
        this.attributeNames = attributeNames;
        this.results = results;
    }

    public void run() {
        try {
            searchHandler.search(connection, base, scope, deref, filter, attributeNames, results);

        } catch (Throwable e) {
            e.printStackTrace(System.out);
            results.setReturnCode(LDAPException.OPERATIONS_ERROR);
            
        } finally {
            results.close();
        }
    }
}
