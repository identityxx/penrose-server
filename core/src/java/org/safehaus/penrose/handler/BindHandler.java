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
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.util.ExceptionUtil;
import org.safehaus.penrose.util.PasswordUtil;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.engine.Engine;
import org.ietf.ldap.LDAPException;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class BindHandler {

    Logger log = LoggerFactory.getLogger(getClass());

    private Handler handler;

    public BindHandler(Handler handler) {
        this.handler = handler;
    }

    public void bind(PenroseSession session, Partition partition, String dn, String password) throws Exception {

        int rc = LDAPException.SUCCESS;
        String message = null;

        try {
            log.warn("Bind as \""+dn+"\".");
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("BIND REQUEST:", 80));
            log.debug(Formatter.displayLine(" - DN       : "+dn, 80));
            log.debug(Formatter.displayLine(" - Password : "+password, 80));
            log.debug(Formatter.displaySeparator(80));

            performBind(session, partition, dn, password);

        } catch (LDAPException e) {
            rc = e.getResultCode();
            message = e.getLDAPErrorMessage();
            throw e;

        } catch (Exception e) {
            rc = ExceptionUtil.getReturnCode(e);
            message = e.getMessage();
            log.error(message, e);
            throw new LDAPException(message, LDAPException.OPERATIONS_ERROR, message);

        } finally {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("BIND RESPONSE:", 80));
            log.debug(Formatter.displayLine(" - RC      : "+rc, 80));
            log.debug(Formatter.displayLine(" - Message : "+message, 80));
            log.debug(Formatter.displaySeparator(80));
        }
    }

    public void performBind(PenroseSession session, Partition partition, String dn, String password) throws Exception {

        PartitionManager partitionManager = handler.getPartitionManager();
        Collection entryMappings = partitionManager.findEntryMappings(partition, dn);

        for (Iterator i=entryMappings.iterator(); i.hasNext(); ) {
            EntryMapping entryMapping = (EntryMapping)i.next();

            String engineName = "DEFAULT";
            if (partition.isProxy(entryMapping)) engineName = "PROXY";

            Engine engine = handler.getEngine(engineName);

            if (engine == null) {
                log.debug("Engine "+engineName+" not found");
                continue;
            }

            // attempt direct bind to the source
            try {
                engine.bind(session, partition, entryMapping, dn, password);
                return;

            } catch (LDAPException e) {
                // continue
            }

            // attempt to compare the userPassword attribute
/*
            List path = new ArrayList();
            AttributeValues sourceValues = new AttributeValues();

            handler.getFindHandler().find(partition, dn, path, sourceValues);

            if (path.isEmpty()) {
                log.debug("Entry "+dn+" not found");
                continue;
            }

            Entry entry = (Entry)path.iterator().next();
*/
            Entry entry = handler.getFindHandler().find(partition, dn);
            
            if (entry == null) {
                log.debug("Entry "+dn+" not found");
                continue;
            }

            AttributeValues attributeValues = entry.getAttributeValues();

            Collection userPasswords = attributeValues.get("userPassword");

            if (userPasswords == null) {
                log.debug("Attribute userPassword not found");
                continue;
            }

            for (Iterator j = userPasswords.iterator(); j.hasNext(); ) {
                Object userPassword = j.next();
                log.debug("userPassword: "+userPassword);
                if (PasswordUtil.comparePassword(password, userPassword)) return;
            }
        }

        int rc = LDAPException.INVALID_CREDENTIALS;
        throw new LDAPException(LDAPException.resultCodeToString(rc), rc, null);
    }

    public Handler getHandler() {
        return handler;
    }

    public void setHandler(Handler handler) {
        this.handler = handler;
    }
}
