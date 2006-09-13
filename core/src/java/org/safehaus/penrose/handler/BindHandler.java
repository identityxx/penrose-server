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
import org.safehaus.penrose.util.PasswordUtil;
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.util.ExceptionUtil;
import org.safehaus.penrose.util.EntryUtil;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.engine.Engine;
import org.ietf.ldap.LDAPDN;
import org.ietf.ldap.LDAPException;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import javax.naming.AuthenticationException;

/**
 * @author Endi S. Dewata
 */
public class BindHandler {

    Logger log = LoggerFactory.getLogger(getClass());

    private Handler handler;

    public BindHandler(Handler handler) {
        this.handler = handler;
    }

    public int bind(PenroseSession session, String dn, String password) throws Exception {

        int rc;
        try {
            log.warn("Bind as \""+dn+"\".");
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("BIND:", 80));
            log.debug(Formatter.displayLine(" - DN       : "+dn, 80));
            log.debug(Formatter.displayLine(" - Password : "+password, 80));
            log.debug(Formatter.displaySeparator(80));

            String rootDn = handler.getPenroseConfig().getRootDn();

            if (rootDn != null && dn != null && EntryUtil.match(dn, rootDn)) { // bind as root

                rc = bindAsRoot(password);
                if (rc != LDAPException.SUCCESS) return rc;

            } else {

                rc = bindAsUser(session, dn, password);
                if (rc != LDAPException.SUCCESS) return rc;
            }

            session.setBindDn(dn);
            session.setBindPassword(password);

            return LDAPException.SUCCESS; // LDAP_SUCCESS

        } catch (LDAPException e) {
            rc = e.getResultCode();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            rc = ExceptionUtil.getReturnCode(e);
        }

        return rc;
    }

    public int unbind(PenroseSession session) throws Exception {

        log.debug("-------------------------------------------------------------------------------");
        log.debug("UNBIND:");

        if (session == null) return 0;

        session.setBindDn(null);
        session.setBindPassword(null);

        log.debug("  dn: " + session.getBindDn());

        return 0;
    }

    public int bindAsRoot(String password) throws Exception {

        if (!PasswordUtil.comparePassword(password, handler.getPenroseConfig().getRootPassword())) {
            log.debug("Password doesn't match => BIND FAILED");
            return LDAPException.INVALID_CREDENTIALS;
        }

        return LDAPException.SUCCESS;
    }

    public int bindAsUser(PenroseSession session, String dn, String password) throws Exception {

        PartitionManager partitionManager = handler.getPartitionManager();
        Partition partition = partitionManager.findPartition(dn);
        EntryMapping entryMapping = partition.findEntryMapping(dn);

        Engine engine = handler.getEngine();

        if (partition.isProxy(entryMapping)) {
            engine = handler.getEngine("PROXY");
        }
        
        return engine.bind(session, partition, dn, password);
    }

    public Handler getHandler() {
        return handler;
    }

    public void setHandler(Handler handler) {
        this.handler = handler;
    }
}
