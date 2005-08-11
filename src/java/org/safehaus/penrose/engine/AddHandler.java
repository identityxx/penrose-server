/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.engine;

import org.safehaus.penrose.PenroseConnection;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.mapping.Entry;
import org.safehaus.penrose.mapping.AttributeValues;
import org.safehaus.penrose.mapping.EntryDefinition;
import org.safehaus.penrose.mapping.AttributeDefinition;
import org.safehaus.penrose.config.Config;
import org.ietf.ldap.LDAPEntry;
import org.ietf.ldap.LDAPDN;
import org.ietf.ldap.LDAPException;
import org.ietf.ldap.LDAPAttribute;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public abstract class AddHandler {

    public Logger log = Logger.getLogger(Penrose.ADD_LOGGER);

    private Engine engine;
    private EngineContext engineContext;

    public void init (Engine engine) throws Exception {
        this.engine = engine;
        this.engineContext = engine.getEngineContext();

        init();
    }

    public void init() throws Exception {        
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

        String dn = LDAPDN.normalize(entry.getDN());

        // find existing entry
        try {
            Entry en = getEngine().getSearchHandler().find(connection, dn);
            if (en != null) return LDAPException.ENTRY_ALREADY_EXISTS;
        } catch (Exception e) {
            // ignore
        }

        AttributeValues values = new AttributeValues();

        for (Iterator iterator=entry.getAttributeSet().iterator(); iterator.hasNext(); ) {
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

        // find parent entry
        int i = dn.indexOf(",");
        String rdn = dn.substring(0, i);
        String parentDn = dn.substring(i+1);

        Entry parent = getEngine().getSearchHandler().find(connection, parentDn);
        if (parent == null) return LDAPException.NO_SUCH_OBJECT;

        EntryDefinition parentDefinition = parent.getEntryDefinition();

        log.debug("Adding entry under "+parent.getDn());

        Collection children = parentDefinition.getChildren();

        // add into the first matching child
        for (Iterator iterator = children.iterator(); iterator.hasNext(); ) {
            EntryDefinition childDefinition = (EntryDefinition)iterator.next();
            if (!childDefinition.isDynamic()) continue;

            return add(connection, childDefinition, values);
        }

        return addStaticEntry(parentDefinition, values, dn);
    }

    public Engine getEngine() {
        return engine;
    }

    public void setEngine(Engine engine) {
        this.engine = engine;
    }

    public EngineContext getEngineContext() {
        return engineContext;
    }

    public void setEngineContext(EngineContext engineContext) {
        this.engineContext = engineContext;
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

        getEngine().getConfig().addEntryDefinition(newEntry);

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

    public abstract int add(
            PenroseConnection connection,
            EntryDefinition entryDefinition,
            AttributeValues values)
            throws Exception;
}
