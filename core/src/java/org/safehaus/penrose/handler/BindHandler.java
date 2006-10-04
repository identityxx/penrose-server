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

import org.safehaus.penrose.session.PenroseSession;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.util.ExceptionUtil;
import org.safehaus.penrose.util.PasswordUtil;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.engine.Engine;
import org.ietf.ldap.LDAPException;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;

/**
 * @author Endi S. Dewata
 */
public class BindHandler {

    Logger log = LoggerFactory.getLogger(getClass());

    private Handler handler;

    public BindHandler(Handler handler) {
        this.handler = handler;
    }

    public int bind(PenroseSession session, Partition partition, String dn, String password) throws Exception {

        int rc;
        try {
            log.warn("Bind as \""+dn+"\".");
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("BIND:", 80));
            log.debug(Formatter.displayLine(" - DN       : "+dn, 80));
            log.debug(Formatter.displayLine(" - Password : "+password, 80));
            log.debug(Formatter.displaySeparator(80));

            rc = performBind(session, partition, dn, password);

        } catch (LDAPException e) {
            rc = e.getResultCode();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            rc = ExceptionUtil.getReturnCode(e);
        }

        return rc;
    }

    public int performBind(PenroseSession session, Partition partition, String dn, String password) throws Exception {

        EntryMapping entryMapping = partition.findEntryMapping(dn);

        String engineName = "DEFAULT";
        if (partition.isProxy(entryMapping)) engineName = "PROXY";

        Engine engine = handler.getEngine(engineName);

        if (engine == null) {
            log.debug("Engine "+engineName+" not found");
            return LDAPException.OPERATIONS_ERROR;
        }

        // attempt direct bind to the source
        int rc = engine.bind(session, partition, entryMapping, dn, password);
        if (rc == LDAPException.SUCCESS) return rc;

        // attempt to compare the userPassword attribute
        List path = new ArrayList();
        AttributeValues sourceValues = new AttributeValues();

        handler.getFindHandler().find(partition, dn, path, sourceValues);

        if (path.isEmpty()) {
            log.debug("Entry "+dn+" not found");
            return LDAPException.INVALID_CREDENTIALS;
        }

        Entry entry = (Entry)path.iterator().next();

        if (entry == null) {
            log.debug("Entry "+dn+" not found");
            return LDAPException.INVALID_CREDENTIALS;
        }

        AttributeValues attributeValues = entry.getAttributeValues();

        Collection userPasswords = attributeValues.get("userPassword");

        if (userPasswords == null) {
            log.debug("Attribute userPassword not found");
            return LDAPException.INVALID_CREDENTIALS;
        }

        for (Iterator i = userPasswords.iterator(); i.hasNext(); ) {
            Object userPassword = i.next();
            log.debug("userPassword: "+userPassword);
            if (PasswordUtil.comparePassword(password, userPassword)) return LDAPException.SUCCESS;
        }

        return LDAPException.INVALID_CREDENTIALS;
    }

    public Handler getHandler() {
        return handler;
    }

    public void setHandler(Handler handler) {
        this.handler = handler;
    }
}
