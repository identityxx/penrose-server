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

    public int bind(PenroseSession session, Partition partition, String dn, String password) throws Exception {

        int rc;
        try {
            log.warn("Bind as \""+dn+"\".");
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("BIND:", 80));
            log.debug(Formatter.displayLine(" - DN       : "+dn, 80));
            log.debug(Formatter.displayLine(" - Password : "+password, 80));
            log.debug(Formatter.displaySeparator(80));

            rc = bindAsUser(session, partition, dn, password);

        } catch (LDAPException e) {
            rc = e.getResultCode();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            rc = ExceptionUtil.getReturnCode(e);
        }

        return rc;
    }

    public int bindAsUser(PenroseSession session, Partition partition, String dn, String password) throws Exception {

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
