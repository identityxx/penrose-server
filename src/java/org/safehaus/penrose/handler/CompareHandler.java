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

import org.safehaus.penrose.PenroseConnection;
import org.safehaus.penrose.schema.AttributeType;
import org.safehaus.penrose.schema.matchingRule.EqualityMatchingRule;
import org.safehaus.penrose.mapping.Entry;
import org.safehaus.penrose.mapping.AttributeValues;
import org.ietf.ldap.*;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class CompareHandler {

    Logger log = Logger.getLogger(getClass());

    private Handler handler;

    public CompareHandler(Handler handler) throws Exception {
        this.handler = handler;
    }
    
    public int compare(PenroseConnection connection, String dn, String attributeName,
            String attributeValue) throws Exception {

        log.debug("-------------------------------------------------------------------------------");
        log.debug("COMPARE:");
        if (connection.getBindDn() != null) log.info(" - Bind DN: " + connection.getBindDn());
        log.debug(" - DN: " + dn);
        log.debug(" - Attribute Name: " + attributeName);
        log.debug(" - Attribute Value: " + attributeValue);
        log.debug("-------------------------------------------------------------------------------");

        List attributeNames = new ArrayList();
        attributeNames.add(attributeName);

        Entry entry = handler.getSearchHandler().find(connection, dn);

        AttributeValues attributeValues = entry.getAttributeValues();
        Collection values = attributeValues.get(attributeName);

        AttributeType attributeType = handler.getSchema().getAttributeType(attributeName);

        String equality = attributeType == null ? null : attributeType.getEquality();
        EqualityMatchingRule equalityMatchingRule = EqualityMatchingRule.getInstance(equality);

        log.debug("Comparing values:");
        for (Iterator i=values.iterator(); i.hasNext(); ) {
            Object value = i.next();

            boolean b = equalityMatchingRule.compare(value, attributeValue);
            log.debug(" - ["+value+"] => "+b);

            if (b) return LDAPException.COMPARE_TRUE;

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
