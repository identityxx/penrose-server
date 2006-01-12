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
package org.safehaus.penrose.engine;

import org.apache.log4j.Logger;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.graph.Graph;
import org.ietf.ldap.LDAPException;

/**
 * @author Endi S. Dewata
 */
public class DeleteEngine {

    Logger log = Logger.getLogger(getClass());

    Engine engine;

    public DeleteEngine(Engine engine) {
        this.engine = engine;
    }

    public int delete(Entry entry) throws Exception {

        if (log.isDebugEnabled()) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("DELETE", 80));
            log.debug(Formatter.displayLine("DN: "+entry.getDn(), 80));

            log.debug(Formatter.displaySeparator(80));
        }

        EntryMapping entryMapping = entry.getEntryMapping();

        AttributeValues sourceValues = entry.getSourceValues();
        //getFieldValues(entry.getDn(), sourceValues);

        Graph graph = engine.getGraph(entryMapping);
        SourceMapping primarySourceMapping = engine.getPrimarySource(entryMapping);

        log.debug("Deleting entry "+entry.getDn()+" ["+sourceValues+"]");

        DeleteGraphVisitor visitor = new DeleteGraphVisitor(engine, entryMapping, sourceValues);
        graph.traverse(visitor, primarySourceMapping);

        if (visitor.getReturnCode() != LDAPException.SUCCESS) return visitor.getReturnCode();

        return LDAPException.SUCCESS;
    }
}
