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
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.util.ExceptionUtil;
import org.safehaus.penrose.engine.Engine;
import org.safehaus.penrose.schema.AttributeType;
import org.safehaus.penrose.entry.DN;
import org.safehaus.penrose.entry.RDN;
import org.safehaus.penrose.entry.AttributeValues;
import org.ietf.ldap.*;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import javax.naming.directory.Attributes;
import javax.naming.directory.Attribute;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;
import javax.naming.NamingEnumeration;
import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class AddHandler {

    Logger log = LoggerFactory.getLogger(getClass());

    public Handler handler;

    public AddHandler(Handler handler) {
        this.handler = handler;
    }

    public void add(
            PenroseSession session,
            Partition partition,
            EntryMapping entryMapping,
            DN dn,
            Attributes attributes)
    throws Exception {

        boolean debug = log.isDebugEnabled();
        if (debug) log.debug("Adding entry "+dn);

        Attributes normalizedAttributes = new BasicAttributes();

        for (NamingEnumeration ne = attributes.getAll(); ne.hasMore(); ) {
            Attribute attribute = (Attribute)ne.next();

            String attributeName = attribute.getID();
            String normalizedAttributeName = attributeName;

            AttributeType at = handler.getSchemaManager().getAttributeType(attributeName);
            if (debug) log.debug("Attribute "+attributeName+": "+at);
            if (at != null) {
                normalizedAttributeName = at.getName();
            }

            Attribute normalizedAttribute = new BasicAttribute(normalizedAttributeName);
            for (NamingEnumeration j=attribute.getAll(); j.hasMore(); ) {
                Object value = j.next();
                normalizedAttribute.add(value);
            }

            normalizedAttributes.put(normalizedAttribute);
        }

        if (debug) {
            log.debug("Original attributes:");
            for (NamingEnumeration ne = attributes.getAll(); ne.hasMore(); ) {
                Attribute attribute = (Attribute)ne.next();
                String attributeName = attribute.getID();

                for (NamingEnumeration j=attribute.getAll(); j.hasMore(); ) {
                    Object value = j.next();
                    log.debug(" - "+attributeName+": "+value);
                }
            }

            log.debug("Normalized attributes:");
            for (NamingEnumeration ne = normalizedAttributes.getAll(); ne.hasMore(); ) {
                Attribute attribute = (Attribute)ne.next();
                String attributeName = attribute.getID();

                for (NamingEnumeration j=attribute.getAll(); j.hasMore(); ) {
                    Object value = j.next();
                    log.debug(" - "+attributeName+": "+value);
                }
            }
        }

        attributes = normalizedAttributes;

        // -dlc- if the objectClass of the add Attributes does
        // not match any of the objectClasses of the entryMapping, there
        // is no sense trying to perform an add on this entryMapping
        Attribute at = attributes.get("objectClass");

        boolean childHasObjectClass = false;
        for (Iterator it2 = entryMapping.getObjectClasses().iterator();
            (!childHasObjectClass) && it2.hasNext();)
        {
            String cObjClass = (String) it2.next();
            for (int i = 0; i < at.size(); i++)
            {
                String objectClass = (String) at.get(i);
                if (childHasObjectClass = cObjClass.equalsIgnoreCase(objectClass))
                {
                    break;
                }
            }
        }
        if (!childHasObjectClass)
        {
            throw ExceptionUtil.createLDAPException(LDAPException.OBJECT_CLASS_VIOLATION);
        }

        String engineName = entryMapping.getEngineName();
        Engine engine = handler.getEngine(engineName);

        if (engine == null) {
            log.debug("Engine "+engineName+" not found");
            throw ExceptionUtil.createLDAPException(LDAPException.OPERATIONS_ERROR);
        }

        engine.add(session, partition, null, entryMapping, dn, attributes);
    }

    public Handler getHandler() {
        return handler;
    }

    public void setHandler(Handler handler) {
        this.handler = handler;
    }

    public int addStaticEntry(Partition partition, EntryMapping parent, DN dn, Attributes attributes) throws Exception {
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

        RDN rdn = dn.getRdn();

        if (parent == null) {
            newEntry = new EntryMapping(dn);

        } else {
            newEntry = new EntryMapping(rdn, parent);
        }

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
