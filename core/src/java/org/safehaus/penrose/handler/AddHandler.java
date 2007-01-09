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
import org.safehaus.penrose.session.PenroseSearchResults;
import org.safehaus.penrose.session.PenroseSearchControls;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.util.EntryUtil;
import org.safehaus.penrose.util.ExceptionUtil;
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.engine.Engine;
import org.ietf.ldap.*;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import javax.naming.directory.Attributes;
import javax.naming.directory.Attribute;
import javax.naming.NamingEnumeration;
import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class AddHandler {

    Logger log = LoggerFactory.getLogger(getClass());

    private Handler handler;

    public AddHandler(Handler handler) {
        this.handler = handler;
    }

    public void add(
            PenroseSession session,
            Partition partition,
            Entry parent,
            String dn,
            Attributes attributes)
    throws Exception {

        int rc = LDAPException.SUCCESS;
        String message = null;

        try {
            log.warn("Adding entry \""+dn+"\".");
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("ADD REQUEST:", 80));
            log.debug(Formatter.displayLine(" - DN       : "+dn, 80));
            log.debug(Formatter.displaySeparator(80));

            performAdd(session, partition, parent, dn, attributes);

            // refreshing entry cache

            PenroseSession adminSession = handler.getPenrose().newSession();
            adminSession.setBindDn(handler.getPenrose().getPenroseConfig().getRootDn());

            PenroseSearchResults results = new PenroseSearchResults();

            PenroseSearchControls sc = new PenroseSearchControls();
            sc.setScope(PenroseSearchControls.SCOPE_SUB);

            adminSession.search(
                    dn,
                    "(objectClass=*)",
                    sc,
                    results
            );

            while (results.hasNext()) results.next();

        } catch (LDAPException e) {
            rc = e.getResultCode();
            message = e.getLDAPErrorMessage();
            throw e;

        } catch (Exception e) {
            rc = ExceptionUtil.getReturnCode(e);
            message = e.getMessage();
            log.error(message, e);
            throw new LDAPException(LDAPException.resultCodeToString(rc), rc, message);

        } finally {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("ADD RESPONSE:", 80));
            log.debug(Formatter.displayLine(" - RC      : "+rc, 80));
            log.debug(Formatter.displayLine(" - Message : "+message, 80));
            log.debug(Formatter.displaySeparator(80));
        }
    }

    public void performAdd(
            PenroseSession session,
            Partition partition,
            Entry parent,
            String dn,
            Attributes attributes)
    throws Exception {

        try {
            log.debug("Adding entry under "+parent.getDn());

            EntryMapping parentMapping = parent.getEntryMapping();

            Collection children = partition.getChildren(parentMapping);

            for (Iterator iterator = children.iterator(); iterator.hasNext(); ) {
                EntryMapping entryMapping = (EntryMapping)iterator.next();
                if (!partition.isDynamic(entryMapping)) continue;

                String engineName = "DEFAULT";
                if (partition.isProxy(entryMapping)) engineName = "PROXY";

                Engine engine = handler.getEngine(engineName);

                if (engine == null) {
                    int rc = LDAPException.OPERATIONS_ERROR;;
                    String message = "Engine "+engineName+" not found";
                    log.error(message);
                    throw new LDAPException(LDAPException.resultCodeToString(rc), rc, message);
                }

                engine.add(session, partition, parent, entryMapping, dn, attributes);
                return;
            }

            String engineName = "DEFAULT";
            if (partition.isProxy(parentMapping)) engineName = "PROXY";

            Engine engine = handler.getEngine(engineName);

            if (engine == null) {
                int rc = LDAPException.OPERATIONS_ERROR;;
                String message = "Engine "+engineName+" not found";
                log.error(message);
                throw new LDAPException(LDAPException.resultCodeToString(rc), rc, message);
            }

            engine.add(session, partition, parent, parentMapping, dn, attributes);

        } catch (LDAPException e) {
            throw e;

        } catch (Exception e) {
            int rc = ExceptionUtil.getReturnCode(e);
            String message = e.getMessage();
            log.error(message, e);
            throw new LDAPException(LDAPException.resultCodeToString(rc), rc, message);
        }
    }

    public Handler getHandler() {
        return handler;
    }

    public void setHandler(Handler handler) {
        this.handler = handler;
    }

    public int addStaticEntry(EntryMapping parent, String dn, Attributes attributes) throws Exception {
        log.debug("Adding static entry "+dn);

        AttributeValues values = new AttributeValues();

        for (NamingEnumeration i=attributes.getAll(); i.hasMore(); ) {
            Attribute attribute = (Attribute)i.next();
            String attributeName = attribute.getID();

            for (NamingEnumeration j=attribute.getAll(); j.hasMore(); ) {
                Object value = j.next();
                values.set(attributeName, value);
            }
        }

        EntryMapping newEntry;

        Row rdn = EntryUtil.getRdn(dn);

        if (parent == null) {
            newEntry = new EntryMapping(dn);

        } else {
            newEntry = new EntryMapping(rdn.toString(), parent);
        }

        Partition partition = handler.getPartitionManager().getPartitionByDn(dn);
        if (partition == null) return LDAPException.NO_SUCH_OBJECT;

        partition.addEntryMapping(newEntry);

        Collection objectClasses = newEntry.getObjectClasses();
        //Collection attributes = newEntry.getAttributeMappings();

        for (Iterator iterator=values.getNames().iterator(); iterator.hasNext(); ) {
            String name = (String)iterator.next();
            Set set = (Set)values.get(name);

            if ("objectclass".equals(name.toLowerCase())) {
                for (Iterator j=set.iterator(); j.hasNext(); ) {
                    String value = (String)j.next();
                    if (!objectClasses.contains(name)) {
                        objectClasses.add(value);
                        log.debug("Add objectClass: "+value);
                    }
                }

                continue;
            }

            for (Iterator j=set.iterator(); j.hasNext(); ) {
                String value = (String)j.next();

                AttributeMapping newAttribute = new AttributeMapping();
                newAttribute.setName(name);
                newAttribute.setConstant(value);
                newAttribute.setRdn(rdn.contains(name) ? AttributeMapping.RDN_TRUE : AttributeMapping.RDN_FALSE);

                log.debug("Add attribute "+name+": "+value);
                newEntry.addAttributeMapping(newAttribute);
            }
        }

        log.debug("New entry "+dn+" has been added.");

        return LDAPException.SUCCESS;
    }
}
