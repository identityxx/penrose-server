/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.engine;

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
