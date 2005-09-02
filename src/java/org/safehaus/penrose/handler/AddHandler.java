/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 * All rights reserved.
 */
package org.safehaus.penrose.handler;

import org.safehaus.penrose.PenroseConnection;
import org.safehaus.penrose.config.Config;
import org.safehaus.penrose.mapping.*;
import org.ietf.ldap.LDAPEntry;
import org.ietf.ldap.LDAPDN;
import org.ietf.ldap.LDAPException;
import org.ietf.ldap.LDAPAttribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class AddHandler {

    Logger log = LoggerFactory.getLogger(getClass());

    private Handler handler;
    private HandlerContext handlerContext;

    public AddHandler(Handler handler) throws Exception {
        this.handler = handler;
        this.handlerContext = handler.getHandlerContext();
    }

    /**
     * The interface function called to add an LDAP entry
     *
     * @param connection the connection
     * @param ldapEntry the entry to be added
     * @return return code (see LDAPException)
     * @throws Exception
     */
    public int add(
            PenroseConnection connection,
            LDAPEntry ldapEntry)
    throws Exception {

        String dn = LDAPDN.normalize(ldapEntry.getDN());

        // find existing entry
        Entry entry = null;
        try {
            entry = getHandler().getSearchHandler().find(connection, dn);
        } catch (Exception e) {
            // ignore
        }

        if (entry != null) return LDAPException.ENTRY_ALREADY_EXISTS;

        // find parent entry
        int i = dn.indexOf(",");
        String rdn = dn.substring(0, i);
        String parentDn = dn.substring(i+1);

        Entry parent = getHandler().getSearchHandler().find(connection, parentDn);
        if (parent == null) return LDAPException.NO_SUCH_OBJECT;

        int rc = handlerContext.getACLEngine().checkAdd(connection, parent);
        if (rc != LDAPException.SUCCESS) return rc;

        log.debug("Adding entry under "+parent.getDn());

        EntryDefinition parentDefinition = parent.getEntryDefinition();
        Collection children = parentDefinition.getChildren();

        AttributeValues values = new AttributeValues();

        for (Iterator iterator=ldapEntry.getAttributeSet().iterator(); iterator.hasNext(); ) {
            LDAPAttribute attribute = (LDAPAttribute)iterator.next();
            String attributeName = attribute.getName();

            String v[] = attribute.getStringValueArray();
            Set set = (Set)values.get(attributeName);
            if (set == null) {
                set = new HashSet();
                values.set(attributeName, set);
            }
            set.addAll(Arrays.asList(v));
        }

        // add into the first matching child
        for (Iterator iterator = children.iterator(); iterator.hasNext(); ) {
            EntryDefinition entryDefinition = (EntryDefinition)iterator.next();
            if (!entryDefinition.isDynamic()) continue;

            return handlerContext.getEngine().add(parent, entryDefinition, values);
        }

        return addStaticEntry(parentDefinition, values, dn);
    }

    public Handler getHandler() {
        return handler;
    }

    public void setHandler(Handler handler) {
        this.handler = handler;
    }

    public HandlerContext getHandlerContext() {
        return handlerContext;
    }

    public void setHandlerContext(HandlerContext handlerContext) {
        this.handlerContext = handlerContext;
    }

    public int addStaticEntry(EntryDefinition parent, AttributeValues values, String dn) throws Exception {
        log.debug("Adding regular entry "+dn);

        int i = dn.indexOf(",");
        EntryDefinition newEntry;

        String rdn;

        if (i < 0) { // no commas
            rdn = dn;
            newEntry = new EntryDefinition(dn);

        } else if (parent == null) { // no parent
            rdn = dn.substring(0, i);
            newEntry = new EntryDefinition(dn);

        } else {
            rdn = dn.substring(0, i);
            newEntry = new EntryDefinition(rdn, parent);
        }

        int k = rdn.indexOf("=");
        String rdnAttribute = rdn.substring(0, k);
        String rdnValue = rdn.substring(k+1);

        Config config = getHandlerContext().getConfig(dn);
        if (config == null) return LDAPException.NO_SUCH_OBJECT;

        config.addEntryDefinition(newEntry);

        Collection objectClasses = newEntry.getObjectClasses();
        Map attributes = newEntry.getAttributes();

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
                attributes.put(name, newAttribute);

                String newExpressions = "\""+value+"\"";
                log.debug("Add attribute "+name+": "+newExpressions);

                newAttribute.setName(name);

                newAttribute.setExpression(newExpressions);

                newAttribute.setRdn(rdnAttribute.equals(name));
            }
        }

        log.debug("New entry "+dn+" has been added.");

        return LDAPException.SUCCESS;
    }
}
