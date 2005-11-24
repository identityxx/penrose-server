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
import org.safehaus.penrose.SearchResults;
import org.safehaus.penrose.event.AddEvent;
import org.safehaus.penrose.config.Config;
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
     * @param connection the connection
     * @param entry the entry to be added
     * @return return code (see LDAPException)
     * @throws Exception
     */
    public int add(
            PenroseConnection connection,
            LDAPEntry entry)
    throws Exception {

        log.info("-------------------------------------------------");
        log.info("ADD:");
        if (connection.getBindDn() != null) log.info(" - Bind DN: "+connection.getBindDn());
        log.info(" - Entry:\n"+Entry.toString(entry));
        log.info("");

        AddEvent beforeModifyEvent = new AddEvent(this, AddEvent.BEFORE_ADD, connection, entry);
        handler.postEvent(entry.getDN(), beforeModifyEvent);

        int rc = performAdd(connection, entry);

        handler.getSearchHandler().search(
                connection,
                entry.getDN(),
                LDAPConnection.SCOPE_SUB,
                LDAPSearchConstraints.DEREF_NEVER,
                "(objectClass=*)",
                new ArrayList(),
                new SearchResults()
        );

        AddEvent afterModifyEvent = new AddEvent(this, AddEvent.AFTER_ADD, connection, entry);
        afterModifyEvent.setReturnCode(rc);
        handler.postEvent(entry.getDN(), afterModifyEvent);

        return rc;
    }

    public int performAdd(
            PenroseConnection connection,
            LDAPEntry ldapEntry)
    throws Exception {

        String dn = LDAPDN.normalize(ldapEntry.getDN());

        // find existing entry
        Entry entry = getHandler().getSearchHandler().find(connection, dn);
        if (entry != null) return LDAPException.ENTRY_ALREADY_EXISTS;

        // find parent entry
        String parentDn = Entry.getParentDn(dn);
        Entry parent = getHandler().getSearchHandler().find(connection, parentDn);
        if (parent == null) return LDAPException.NO_SUCH_OBJECT;

        int rc = handler.getACLEngine().checkAdd(connection, parent);
        if (rc != LDAPException.SUCCESS) return rc;

        log.debug("Adding entry under "+parent.getDn());

        EntryDefinition parentDefinition = parent.getEntryDefinition();
        Config config = handler.getConfigManager().getConfig(parentDefinition);
        Collection children = config.getChildren(parentDefinition);

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
                EntryDefinition entryDefinition = (EntryDefinition)iterator.next();
                if (!config.isDynamic(entryDefinition)) continue;

                return handler.getEngine().add(parent, entryDefinition, values);
            }
        }

        return addStaticEntry(parentDefinition, values, dn);
    }

    public Handler getHandler() {
        return handler;
    }

    public void setHandler(Handler handler) {
        this.handler = handler;
    }

    public int addStaticEntry(EntryDefinition parent, AttributeValues values, String dn) throws Exception {
        log.debug("Adding regular entry "+dn);

        EntryDefinition newEntry;

        Row rdn = Entry.getRdn(dn);

        if (parent == null) {
            newEntry = new EntryDefinition(dn);

        } else {
            newEntry = new EntryDefinition(rdn.toString(), parent);
        }

        Config config = handler.getConfigManager().getConfig(dn);
        if (config == null) return LDAPException.NO_SUCH_OBJECT;

        config.addEntryDefinition(newEntry);

        Collection objectClasses = newEntry.getObjectClasses();
        Collection attributes = newEntry.getAttributeDefinitions();

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

                AttributeDefinition newAttribute = new AttributeDefinition();
                newAttribute.setName(name);
                newAttribute.setConstant(value);
                newAttribute.setRdn(rdn.contains(name));

                log.debug("Add attribute "+name+": "+value);
                newEntry.addAttributeDefinition(newAttribute);
            }
        }

        log.debug("New entry "+dn+" has been added.");

        return LDAPException.SUCCESS;
    }
}
