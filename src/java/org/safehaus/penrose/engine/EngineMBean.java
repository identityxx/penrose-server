/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.engine;

import org.safehaus.penrose.SearchResults;
import org.safehaus.penrose.PenroseConnection;
import org.ietf.ldap.LDAPEntry;

import java.util.Collection;
import java.util.List;

public interface EngineMBean {

    public int bind(PenroseConnection connection, String dn, String password) throws Exception;
    public int unbind(PenroseConnection connection) throws Exception;
    public int add(PenroseConnection connection, LDAPEntry entry) throws Exception;
    public int delete(PenroseConnection connection, String dn) throws Exception;
    public int modify(PenroseConnection connection, String dn, List modifications) throws Exception;
    public int modrdn(PenroseConnection connection, String dn, String newRdn) throws Exception;
    public int compare(PenroseConnection connection, String dn, String attributeName, String attributeValue) throws Exception;

    public SearchResults search(PenroseConnection connection, String base, int scope,
            int deref, String filter, Collection attributeNames)
            throws Exception;

    public void stop() throws Exception;
}
