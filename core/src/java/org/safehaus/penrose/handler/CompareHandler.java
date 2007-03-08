/**
 * Copyright (c) 2000-2006, Identyx Corporation.
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

import org.safehaus.penrose.session.PenroseSession;
import org.safehaus.penrose.schema.AttributeType;
import org.safehaus.penrose.schema.matchingRule.EqualityMatchingRule;
import org.safehaus.penrose.entry.Entry;
import org.safehaus.penrose.entry.DN;
import org.safehaus.penrose.entry.AttributeValues;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.util.*;
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.partition.Partition;
import org.ietf.ldap.*;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class CompareHandler {

    Logger log = LoggerFactory.getLogger(getClass());

    public Handler handler;

    public CompareHandler(Handler handler) {
        this.handler = handler;
    }
    
    public boolean compare(
            PenroseSession session,
            Partition partition,
            EntryMapping entryMapping,
            DN dn,
            String attributeName,
            Object attributeValue
    ) throws LDAPException {

        boolean debug = log.isDebugEnabled();
        int rc = LDAPException.SUCCESS;
        String message = null;

        try {
            Entry entry = ((DefaultHandler)handler).getFindHandler().find(session, partition, entryMapping, dn);

            if (entry == null) {
                if (debug) log.debug("Entry "+dn+" not found");
                throw ExceptionUtil.createLDAPException(LDAPException.NO_SUCH_OBJECT);
            }

            List attributeNames = new ArrayList();
            attributeNames.add(attributeName);

            AttributeValues attributeValues = entry.getAttributeValues();
            Collection values = attributeValues.get(attributeName);
            if (values == null) {
                if (debug) log.debug("Attribute "+attributeName+" not found.");
                return false;
            }

            AttributeType attributeType = handler.getSchemaManager().getAttributeType(attributeName);

            String equality = attributeType == null ? null : attributeType.getEquality();
            EqualityMatchingRule equalityMatchingRule = EqualityMatchingRule.getInstance(equality);

            if (debug) log.debug("Comparing values:");
            for (Iterator i=values.iterator(); i.hasNext(); ) {
                Object value = i.next();

                boolean b = equalityMatchingRule.compare(value, attributeValue);
                if (debug) log.debug(" - ["+value+"] => "+b);

                if (b) return true;

            }

            return false;

        } catch (LDAPException e) {
            rc = e.getResultCode();
            message = e.getLDAPErrorMessage();
            throw e;

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionUtil.createLDAPException(e);

        } finally {
            if (debug) {
                log.debug(Formatter.displaySeparator(80));
                log.debug(Formatter.displayLine("COMPARE RESPONSE:", 80));
                log.debug(Formatter.displayLine(" - RC      : "+rc, 80));
                log.debug(Formatter.displayLine(" - Message : "+message, 80));
                log.debug(Formatter.displaySeparator(80));
            }
        }
    }

    public Handler getEngine() {
        return handler;
    }

    public void setEngine(Handler handler) {
        this.handler = handler;
    }

    public Handler getHandler() {
        return handler;
    }

    public void setHandler(Handler handler) {
        this.handler = handler;
    }
}
