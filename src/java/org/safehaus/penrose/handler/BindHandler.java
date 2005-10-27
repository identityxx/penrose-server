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
import org.safehaus.penrose.config.Config;
import org.safehaus.penrose.event.BindEvent;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.util.PasswordUtil;
import org.ietf.ldap.LDAPDN;
import org.ietf.ldap.LDAPException;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class BindHandler {

    Logger log = Logger.getLogger(getClass());

    private Handler handler;
    private HandlerContext handlerContext;

    public BindHandler(Handler handler) throws Exception {
        this.handler = handler;
        this.handlerContext = handler.getHandlerContext();
    }

    public int bind(PenroseConnection connection, String dn, String password) throws Exception {

        log.info("-------------------------------------------------");
        log.info("BIND:");
        log.info(" - DN      : "+dn);

        BindEvent beforeBindEvent = new BindEvent(this, BindEvent.BEFORE_BIND, connection, dn, password);
        handler.postEvent(dn, beforeBindEvent);

        int rc = performBind(connection, dn, password);

        BindEvent afterBindEvent = new BindEvent(this, BindEvent.AFTER_BIND, connection, dn, password);
        afterBindEvent.setReturnCode(rc);
        handler.postEvent(dn, afterBindEvent);

        return rc;
    }

    public int performBind(PenroseConnection connection, String dn, String password) throws Exception {

        String ndn = LDAPDN.normalize(dn);

        if (handlerContext.getRootDn() != null && ndn.equals(LDAPDN.normalize(handlerContext.getRootDn()))) { // bind as root

            int rc = bindAsRoot(password);
            if (rc != LDAPException.SUCCESS) return rc;

        } else {

            int rc = bindAsUser(connection, ndn, password);
            if (rc != LDAPException.SUCCESS) return rc;
        }

        connection.setBindDn(dn);
        return LDAPException.SUCCESS; // LDAP_SUCCESS
    }

    public int unbind(PenroseConnection connection) throws Exception {

        log.debug("-------------------------------------------------------------------------------");
        log.debug("UNBIND:");

        if (connection == null) return 0;

        connection.setBindDn(null);

        log.debug("  dn: " + connection.getBindDn());

        return 0;
    }

    public int bindAsRoot(String password) throws Exception {
        log.debug("Comparing root's password");

        if (!PasswordUtil.comparePassword(password, handlerContext.getRootPassword())) {
            log.debug("Password doesn't match => BIND FAILED");
            return LDAPException.INVALID_CREDENTIALS;
        }

        return LDAPException.SUCCESS;
    }

    public int bindAsUser(PenroseConnection connection, String dn, String password) throws Exception {
        log.debug("Searching for "+dn);

        List path = handler.getSearchHandler().find(connection, dn);
        if (path == null) {
            log.debug("Entry "+dn+" not found => BIND FAILED");
            return LDAPException.INVALID_CREDENTIALS;
        }

        Entry entry = (Entry)path.iterator().next();
        log.debug("Found "+entry.getDn());

        return bindAsUser(connection, entry, password);
    }

    public int bindAsUser(PenroseConnection connection, Entry sr, String password) throws Exception {

        log.debug("Bind as user "+sr.getDn());

        EntryDefinition entry = sr.getEntryDefinition();
        AttributeValues attributeValues = sr.getAttributeValues();

        return bind(entry, attributeValues, password);
    }

    public int bind(EntryDefinition entry, AttributeValues attributeValues, String password) throws Exception {

        Collection set = attributeValues.get("userPassword");

        if (set != null) {
            for (Iterator i = set.iterator(); i.hasNext(); ) {
                String userPassword = (String)i.next();
                log.debug("userPassword: "+userPassword);
                if (PasswordUtil.comparePassword(password, userPassword)) return LDAPException.SUCCESS;
            }
        }

        Collection sources = entry.getSources();
        Config config = handlerContext.getConfig(entry.getDn());

        for (Iterator i=sources.iterator(); i.hasNext(); ) {
            Source source = (Source)i.next();

            ConnectionConfig connectionConfig = config.getConnectionConfig(source.getConnectionName());
            SourceDefinition sourceDefinition = connectionConfig.getSourceDefinition(source.getSourceName());

            Map entries = handlerContext.getTransformEngine().split(source, attributeValues);

            for (Iterator j=entries.keySet().iterator(); j.hasNext(); ) {
                Row pk = (Row)j.next();
                AttributeValues sourceValues = (AttributeValues)entries.get(pk);

                log.debug("Bind to "+source.getName()+" as "+pk+": "+sourceValues);

                int rc = getEngineContext().getSyncService().bind(sourceDefinition, entry, sourceValues, password);
                if (rc == LDAPException.SUCCESS) return rc;
            }
        }

        return LDAPException.INVALID_CREDENTIALS;
    }

    public Handler getEngine() {
        return handler;
    }

    public void setEngine(Handler handler) {
        this.handler = handler;
    }

    public HandlerContext getEngineContext() {
        return handlerContext;
    }

    public void setEngineContext(HandlerContext handlerContext) {
        this.handlerContext = handlerContext;
    }
}
