/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 * All rights reserved.
 */
package org.safehaus.penrose.handler;

import org.safehaus.penrose.PenroseConnection;
import org.safehaus.penrose.SearchResults;
import org.ietf.ldap.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.ArrayList;
import java.util.Enumeration;

/**
 * @author Endi S. Dewata
 */
public class CompareHandler {

    Logger log = LoggerFactory.getLogger(getClass());

    private Handler handler;

    public CompareHandler(Handler handler) throws Exception {
        this.handler = handler;
    }
    
    public int compare(PenroseConnection connection, String dn, String attributeName,
            String attributeValue) throws Exception {

        List attributeNames = new ArrayList();
        attributeNames.add(attributeName);

        SearchResults results = new SearchResults();
        try {
            handler.getSearchHandler().search(
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

    public Handler getEngine() {
        return handler;
    }

    public void setEngine(Handler handler) {
        this.handler = handler;
    }
}
