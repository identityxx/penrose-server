/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.engine;

import org.safehaus.penrose.PenroseConnection;
import org.safehaus.penrose.SearchResults;
import org.safehaus.penrose.Penrose;
import org.ietf.ldap.*;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.ArrayList;
import java.util.Enumeration;

/**
 * @author Endi S. Dewata
 */
public abstract class CompareHandler {

    public Logger log = Logger.getLogger(Penrose.COMPARE_LOGGER);

    private Engine engine;

    public void init(Engine engine) throws Exception {
        this.engine = engine;

        init();
    }

    public void init() throws Exception {
    }
    
    public int compare(PenroseConnection connection, String dn, String attributeName,
            String attributeValue) throws Exception {

        List attributeNames = new ArrayList();
        attributeNames.add(attributeName);

        SearchResults results = new SearchResults();
        try {
            engine.getSearchHandler().search(
                    connection,
                    dn,
                    LDAPConnection.SCOPE_SUB,
                    LDAPSearchConstraints.DEREF_ALWAYS,
                    "(objectClass=*)",
                    attributeNames,
                    results);
        } catch (Exception e) {
            // ignore
        }

        LDAPEntry entry = (LDAPEntry)results.next();
        LDAPAttributeSet attributes = entry.getAttributeSet();
        LDAPAttribute attribute = attributes.getAttribute(attributeName);

        for (Enumeration e = attribute.getStringValues(); e.hasMoreElements(); ) {
            String value = (String)e.nextElement();
            if (value.equals(attributeValue)) return LDAPException.COMPARE_TRUE;
        }

        return LDAPException.COMPARE_FALSE;
    }

    public Engine getEngine() {
        return engine;
    }

    public void setEngine(Engine engine) {
        this.engine = engine;
    }
}
