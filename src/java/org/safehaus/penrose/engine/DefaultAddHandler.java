/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.engine;

import org.ietf.ldap.*;
import org.safehaus.penrose.config.*;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.PenroseConnection;
import org.safehaus.penrose.thread.MRSWLock;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class DefaultAddHandler implements AddHandler {

    public Logger log = Logger.getLogger(Penrose.ADD_LOGGER);

    public DefaultEngine engine;
    public EngineContext engineContext;
    public Config config;

    public void init (Engine engine, EngineContext engineContext) throws Exception {
        this.engine = (DefaultEngine)engine;
        this.engineContext = engineContext;
        config = engineContext.getConfig();
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
        if (config.getEntryDefinition(dn) != null) return LDAPException.ENTRY_ALREADY_EXISTS;

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

        EntryDefinition parentEntry = config.getEntryDefinition(parentDn);
        if (parentEntry == null) return LDAPException.NO_SUCH_OBJECT;

        return add(connection, parentEntry, dn, values);
    }

    public int add(PenroseConnection connection, EntryDefinition parentEntry, String dn, AttributeValues values) throws Exception {

        log.debug("Adding entry under "+parentEntry.getDn());

        Collection children = parentEntry.getChildren();

        // add into the first matching child
        for (Iterator iterator = children.iterator(); iterator.hasNext(); ) {
            EntryDefinition entry = (EntryDefinition)iterator.next();
            if (!entry.isDynamic()) continue;

            return add(entry, values);
        }

        return addStaticEntry(dn, values, parentEntry);
    }

    public int addStaticEntry(String dn, AttributeValues values, EntryDefinition parent) throws Exception {
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

    public int add(EntryDefinition entry, AttributeValues values) throws Exception {

        Date date = new Date();

        Collection sources = entry.getSources();

        for (Iterator i2 = sources.iterator(); i2.hasNext(); ) {
            Source source = (Source)i2.next();

            int rc = add(source, entry, values, date);
            if (rc != LDAPException.SUCCESS) return rc;
        }

        engine.getEntryCache().put(entry, values, date);

        return LDAPException.SUCCESS;
    }

    public int add(Source source, EntryDefinition entry, AttributeValues values, Date date) throws Exception {

        log.debug("----------------------------------------------------------------");
        log.debug("Adding entry into "+source.getName());
        log.debug("Values: "+values);

        MRSWLock lock = engine.getLock(source);
        lock.getWriteLock(Penrose.WAIT_TIMEOUT);

        try {

	        Map rows = engineContext.getTransformEngine().transform(source, values);

	        log.debug("New entries: "+rows);

	        for (Iterator i=rows.keySet().iterator(); i.hasNext(); ) {
	            Row pk = (Row)i.next();
	            AttributeValues attributes = (AttributeValues)rows.get(pk);

                //AttributeValues attributes = engineContext.getTransformEngine().convert(row);

	            // Add row to the source table in the source database/directory
	            int rc = source.add(attributes);
	            if (rc != LDAPException.SUCCESS) return rc;

	            // Add row to the source table in the cache
	            engine.getSourceCache().insert(source, attributes, date);
	        }

        } finally {
        	lock.releaseWriteLock(Penrose.WAIT_TIMEOUT);
        }

        return LDAPException.SUCCESS;
    }

}
