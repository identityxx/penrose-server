/** * Copyright (c) 2000-2005, Identyx Corporation.
 * 
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA */package org.safehaus.penrose.handler;import org.safehaus.penrose.PenroseConnection;import org.safehaus.penrose.SearchResults;import org.ietf.ldap.*;import org.slf4j.Logger;import org.slf4j.LoggerFactory;import java.util.List;import java.util.ArrayList;import java.util.Enumeration;/** * @author Endi S. Dewata */public class CompareHandler {    Logger log = LoggerFactory.getLogger(getClass());    private Handler handler;    public CompareHandler(Handler handler) throws Exception {        this.handler = handler;    }        public int compare(PenroseConnection connection, String dn, String attributeName,            String attributeValue) throws Exception {        List attributeNames = new ArrayList();        attributeNames.add(attributeName);        SearchResults results = new SearchResults();        try {            handler.getSearchHandler().search(                    connection,                    dn,                    LDAPConnection.SCOPE_SUB,                    LDAPSearchConstraints.DEREF_ALWAYS,                    "(objectClass=*)",                    attributeNames,                    results);        } catch (Exception e) {            // ignore        }        LDAPEntry entry = (LDAPEntry)results.next();        LDAPAttributeSet attributes = entry.getAttributeSet();        LDAPAttribute attribute = attributes.getAttribute(attributeName);        for (Enumeration e = attribute.getStringValues(); e.hasMoreElements(); ) {            String value = (String)e.nextElement();            if (value.equals(attributeValue)) return LDAPException.COMPARE_TRUE;        }        return LDAPException.COMPARE_FALSE;    }    public Handler getEngine() {        return handler;    }    public void setEngine(Handler handler) {        this.handler = handler;    }}