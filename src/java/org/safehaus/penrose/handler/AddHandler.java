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
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.util.EntryUtil;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.service.ServiceConfig;
import org.ietf.ldap.*;
import org.apache.log4j.Logger;

import javax.naming.directory.Attributes;
import javax.naming.directory.Attribute;
import javax.naming.NamingEnumeration;
import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class AddHandler {

    Logger log = Logger.getLogger(getClass());

    private Handler handler;

    public AddHandler(Handler handler) {
        this.handler = handler;
    }

    public int add(
            PenroseSession session,
            String dn,
            Attributes attributes)
    throws Exception {

        int rc;
        try {
            log.info("Adding "+dn);
            log.debug("-------------------------------------------------");
            log.debug("ADD:");
            if (session != null && session.getBindDn() != null) log.debug(" - Bind DN: "+session.getBindDn());
            log.debug(" - Entry:\n"+dn);
            log.debug("");

            if (session != null && session.getBindDn() == null) {
                PenroseConfig penroseConfig = handler.getPenroseConfig();
                ServiceConfig serviceConfig = penroseConfig.getServiceConfig("LDAP");
                String s = serviceConfig == null ? null : serviceConfig.getParameter("allowAnonymousAccess");
                boolean allowAnonymousAccess = s == null ? true : new Boolean(s).booleanValue();
                if (!allowAnonymousAccess) {
                    return LDAPException.INSUFFICIENT_ACCESS_RIGHTS;
                }
            }

            rc = performAdd(session, dn, attributes);
            if (rc != LDAPException.SUCCESS) return rc;

            PenroseSearchResults results = new PenroseSearchResults();

            handler.getSearchHandler().search(
                    null,
                    dn,
                    LDAPConnection.SCOPE_SUB,
                    LDAPSearchConstraints.DEREF_NEVER,
                    "(objectClass=*)",
                    new ArrayList(),
                    results
            );

        } catch (LDAPException e) {
            rc = e.getResultCode();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            rc = LDAPException.OPERATIONS_ERROR;
        }

        return rc;
    }

    public int performAdd(
            PenroseSession session,
            String dn,
            Attributes attributes)
    throws Exception {

        dn = LDAPDN.normalize(dn);

        // find parent entry
        String parentDn = EntryUtil.getParentDn(dn);
        Entry parent = getHandler().getFindHandler().find(session, parentDn);
        if (parent == null) {
            log.debug("Parent entry "+parentDn+" not found");
            return LDAPException.NO_SUCH_OBJECT;
        }

        int rc = handler.getACLEngine().checkAdd(session, parentDn, parent.getEntryMapping());
        if (rc != LDAPException.SUCCESS) {
            log.debug("Not allowed to add children under "+parentDn);
            return rc;
        }

        log.debug("Adding entry under "+parentDn);

        EntryMapping parentMapping = parent.getEntryMapping();
        PartitionManager partitionManager = handler.getPartitionManager();
        Partition partition = partitionManager.getPartition(parentMapping);

        if (partition.isProxy(parentMapping)) {
            log.debug("Adding "+dn+" via proxy");
            handler.getEngine().addProxy(partition, parentMapping, dn, attributes);
            return LDAPException.SUCCESS;
        }

        Collection children = partition.getChildren(parentMapping);

        AttributeValues values = new AttributeValues();

        for (NamingEnumeration i=attributes.getAll(); i.hasMore(); ) {
            Attribute attribute = (Attribute)i.next();
            String attributeName = attribute.getID();

            Set set = new HashSet();
            for (NamingEnumeration j=attribute.getAll(); j.hasMore(); ) {
                Object value = j.next();
                set.add(value);
            }
            values.set(attributeName, set);
        }

        // add into the first matching child
        for (Iterator iterator = children.iterator(); iterator.hasNext(); ) {
            EntryMapping entryMapping = (EntryMapping)iterator.next();
            boolean dynamic = partition.isDynamic(entryMapping);

            //log.debug("Checking mapping "+entryMapping.getDn()+": "+dynamic);
            if (!dynamic) continue;

            return handler.getEngine().add(parent, entryMapping, values);
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
        log.debug("Adding static entry "+dn);

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
                newAttribute.setRdn(rdn.contains(name));

                log.debug("Add attribute "+name+": "+value);
                newEntry.addAttributeMapping(newAttribute);
            }
        }

        log.debug("New entry "+dn+" has been added.");

        return LDAPException.SUCCESS;
    }
}
