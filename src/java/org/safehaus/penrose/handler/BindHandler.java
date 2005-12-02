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
import org.safehaus.penrose.session.PenroseSession;
import org.safehaus.penrose.event.BindEvent;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.util.PasswordUtil;
import org.ietf.ldap.LDAPDN;
import org.ietf.ldap.LDAPException;
import org.apache.log4j.Logger;

/**
 * @author Endi S. Dewata
 */
public class BindHandler {

    Logger log = Logger.getLogger(getClass());

    private Handler handler;

    public BindHandler(Handler handler) throws Exception {
        this.handler = handler;
    }

    public int bind(PenroseSession session, String dn, String password) throws Exception {

        log.info("BIND:");
        log.info(" - DN      : "+dn);

        BindEvent beforeBindEvent = new BindEvent(this, BindEvent.BEFORE_BIND, session, dn, password);
        handler.postEvent(dn, beforeBindEvent);

        int rc = performBind(session, dn, password);

        BindEvent afterBindEvent = new BindEvent(this, BindEvent.AFTER_BIND, session, dn, password);
        afterBindEvent.setReturnCode(rc);
        handler.postEvent(dn, afterBindEvent);

        return rc;
    }

    public int performBind(PenroseSession session, String dn, String password) throws Exception {

        String ndn = LDAPDN.normalize(dn);

        if (handler.getRootDn() != null && ndn.equals(LDAPDN.normalize(handler.getRootDn()))) { // bind as root

            int rc = bindAsRoot(password);
            if (rc != LDAPException.SUCCESS) return rc;

        } else {

            int rc = bindAsUser(session, ndn, password);
            if (rc != LDAPException.SUCCESS) return rc;
        }

        session.setBindDn(dn);
        return LDAPException.SUCCESS; // LDAP_SUCCESS
    }

    public int unbind(PenroseSession session) throws Exception {

        log.debug("-------------------------------------------------------------------------------");
        log.debug("UNBIND:");

        if (session == null) return 0;

        session.setBindDn(null);

        log.debug("  dn: " + session.getBindDn());

        return 0;
    }

    public int bindAsRoot(String password) throws Exception {
        log.debug("Comparing root's password");

        if (!PasswordUtil.comparePassword(password, handler.getRootPassword())) {
            log.debug("Password doesn't match => BIND FAILED");
            return LDAPException.INVALID_CREDENTIALS;
        }

        return LDAPException.SUCCESS;
    }

    public int bindAsUser(PenroseSession session, String dn, String password) throws Exception {
        log.debug("Searching for "+dn);

        Entry entry = handler.getSearchHandler().find(session, dn);
        if (entry == null) {
            log.debug("Entry "+dn+" not found => BIND FAILED");
            return LDAPException.INVALID_CREDENTIALS;
        }

        log.debug("Found "+entry.getDn());

        return handler.getEngine().bind(entry, password);
    }

    public Handler getHandler() {
        return handler;
    }

    public void setHandler(Handler handler) {
        this.handler = handler;
    }
}
