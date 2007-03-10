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
import org.safehaus.penrose.engine.Engine;
import org.safehaus.penrose.entry.DN;
import org.safehaus.penrose.entry.Entry;
import org.safehaus.penrose.entry.AttributeValues;
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

    public Handler handler;

    public BindHandler(Handler handler) {
        this.handler = handler;
    }

    public void bind(PenroseSession session, Partition partition, EntryMapping entryMapping, DN dn, String password) throws Exception {

        log.warn("Bind as \""+dn+"\".");
        log.debug(Formatter.displaySeparator(80));
        log.debug(Formatter.displayLine("BIND:", 80));
        log.debug(Formatter.displayLine(" - DN       : "+dn, 80));
        log.debug(Formatter.displayLine(" - Password : "+password, 80));
        log.debug(Formatter.displaySeparator(80));

        String engineName = entryMapping.getEngineName();
        Engine engine = handler.getEngine(engineName);

        if (engine == null) {
            log.debug("Engine "+engineName+" not found");
            throw ExceptionUtil.createLDAPException(LDAPException.OPERATIONS_ERROR);
        }

        engine.bind(session, partition, entryMapping, dn, password);
    }

    public Handler getHandler() {
        return handler;
    }

    public void setHandler(Handler handler) {
        this.handler = handler;
    }
}
