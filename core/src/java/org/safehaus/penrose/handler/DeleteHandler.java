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
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.entry.Entry;
import org.safehaus.penrose.util.ExceptionUtil;
import org.safehaus.penrose.engine.Engine;
import org.safehaus.penrose.entry.DN;
import org.ietf.ldap.LDAPException;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class DeleteHandler {

    Logger log = LoggerFactory.getLogger(getClass());

    public Handler handler;

    public DeleteHandler(Handler handler) {
        this.handler = handler;
    }

    public void delete(
            PenroseSession session,
            Partition partition,
            EntryMapping entryMapping,
            DN dn
    ) throws Exception {

        String engineName = entryMapping.getEngineName();
        Engine engine = handler.getEngine(engineName);

        if (engine == null) {
            log.debug("Engine "+engineName+" not found");
            throw ExceptionUtil.createLDAPException(LDAPException.OPERATIONS_ERROR);
        }

        engine.delete(session, partition, null, entryMapping, dn);
    }

    public int deleteStaticEntry(Partition partition, EntryMapping entryMapping) throws Exception {

        log.debug("Deleting static entry "+entryMapping.getDn());

        // can't delete no leaf
        Collection children = partition.getChildren(entryMapping);
        if (!children.isEmpty()) return LDAPException.NOT_ALLOWED_ON_NONLEAF;

        partition.removeEntryMapping(entryMapping);

        return LDAPException.SUCCESS;
    }

    public Handler getHandler() {
        return handler;
    }

    public void getHandler(Handler handler) {
        this.handler = handler;
    }

    public void setHandler(Handler handler) {
        this.handler = handler;
    }
}
