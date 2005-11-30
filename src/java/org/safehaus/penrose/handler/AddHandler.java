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
import org.safehaus.penrose.session.PenroseSearchResults;
import org.safehaus.penrose.session.PenroseSession;
import org.safehaus.penrose.session.PenroseSearchResults;
import org.safehaus.penrose.event.AddEvent;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.mapping.*;
import org.ietf.ldap.*;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class AddHandler {

    Logger log = Logger.getLogger(getClass());

    private Handler handler;

    public AddHandler(Handler handler) throws Exception {
        this.handler = handler;
    }

    /**
     * The interface function called to add an LDAP entry
     *
     * @param session the connection
     * @param entry the entry to be added
     * @return return code (see LDAPException)
     * @throws Exception
     */
    public int add(
            PenroseSession session,
            LDAPEntry entry)
    throws Exception {

        log.info("-------------------------------------------------");
        log.info("ADD:");
        if (session.getBindDn() != null) log.info(" - Bind DN: "+session.getBindDn());
        log.info(" - Entry:\n"+Entry.toString(entry));
        log.info("");

        AddEvent beforeModifyEvent = new AddEvent(this, AddEvent.BEFORE_ADD, session, entry);
        handler.postEvent(entry.getDN(), beforeModifyEvent);

        int rc = performAdd(session, entry);

        handler.getSearchHandler().search(
                session,
                entry.getDN(),
                LDAPConnection.SCOPE_SUB,
                LDAPSearchConstraints.DEREF_NEVER,
                "(objectClass=*)",
                new ArrayList(),
                new PenroseSearchResults()
        );

        AddEvent afterModifyEvent = new AddEvent(this, AddEvent.AFTER_ADD, session, entry);
        afterModifyEvent.setReturnCode(rc);
        handler.postEvent(entry.getDN(), afterModifyEvent);

        return rc;
    }

    public int performAdd(
            PenroseSession session,
            LDAPEntry ldapEntry)
    throws Exception {

        String dn = LDAPDN.normalize(ldapEntry.getDN());

        // find existing entry
        Entry entry = getHandler().getSearchHandler().find(session, dn);
        if (entry != null) return LDAPException.ENTRY_ALREADY_EXISTS;

        // find parent entry
        String parentDn = Entry.getParentDn(dn);
        Entry parent = getHandler().getSearchHandler().find(session, parentDn);
        if (parent == null) return LDAPException.NO_SUCH_OBJECT;

        int rc = handler.getACLEngine().checkAdd(session, parent);
        if (rc != LDAPException.SUCCESS) return rc;

        log.debug("Adding entry under "+parent.getDn());

        EntryMapping parentMapping = parent.getEntryMapping();
        Partition partition = handler.getPartitionManager().getPartition(parentMapping);
        Collection children = partition.getChildren(parentMapping);

        AttributeValues values = new AttributeValues();

        for (Iterator iterator=ldapEntry.getAttributeSet().iterator(); iterator.hasNext(); ) {
            LDAPAttribute attribute = (LDAPAttribute)iterator.next();
            String attributeName = attribute.getName();

            String v[] = attribute.getStringValueArray();
            Set set = (Set)values.get(attributeName);
            if (set == null) set = new HashSet();
            set.addAll(Arrays.asList(v));
            values.set(attributeName, set);
        }

        // add into the first matching child
        if (children != null) {
            for (Iterator iterator = children.iterator(); iterator.hasNext(); ) {
                EntryMapping entryMapping = (EntryMapping)iterator.next();
                if (!partition.isDynamic(entryMapping)) continue;

                return handler.getEngine().add(parent, entryMapping, values);
            }
        }

        return addStaticEntry(parentMapping, values, dn);
    }

    public Handler getHandler() {
        return handler;
    }

    public void setHandler(Handler handler) {
        this.handler = handler;
    }

    public int addStaticEntry(EntryMapping parent, AttributeValues values, String dn) throws Exception {
        log.debug("Adding regular entry "+dn);

        EntryMapping newEntry;

        Row rdn = Entry.getRdn(dn);

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
                newAttribute.setRdn(rdn.contains(name));

                log.debug("Add attribute "+name+": "+value);
                newEntry.addAttributeMapping(newAttribute);
            }
        }

        log.debug("New entry "+dn+" has been added.");

        return LDAPException.SUCCESS;
    }
}
