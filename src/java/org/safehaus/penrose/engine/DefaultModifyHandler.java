/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.engine;

import org.ietf.ldap.*;
import org.safehaus.penrose.config.*;
import org.safehaus.penrose.util.PasswordUtil;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.PenroseConnection;
import org.safehaus.penrose.thread.MRSWLock;
import org.safehaus.penrose.schema.Schema;
import org.safehaus.penrose.schema.ObjectClass;
import org.apache.log4j.Logger;

import java.util.*;


/**
 * @author Endi S. Dewata
 */
public class DefaultModifyHandler implements ModifyHandler {

    public Logger log = Logger.getLogger(Penrose.MODIFY_LOGGER);

    public DefaultEngine engine;
	public EngineContext engineContext;
    public Config config;

	public void init(Engine engine, EngineContext engineContext) throws Exception {
        this.engine = ((DefaultEngine)engine);
		this.engineContext = engineContext;
        this.config = engineContext.getConfig();
	}

	public int modify(PenroseConnection connection, String dn, List modifications)
			throws Exception {

		String ndn = LDAPDN.normalize(dn);

		EntryDefinition entry = config.getEntryDefinition(ndn);
		if (entry != null) {
			return modifyStaticEntry(entry, modifications);
		}

		Entry sr;
		try {
			sr = ((DefaultSearchHandler)engine.getSearchHandler()).getVirtualEntry(connection, ndn, new ArrayList());
		} catch (LDAPException e) {
			return e.getResultCode();
		}

		if (sr == null) return LDAPException.NO_SUCH_OBJECT;

		return modifyVirtualEntry(connection, sr, modifications);
	}

    /**
     * Apply encryption/encoding at attribute level.
     * @param entry
     * @param modifications
     * @throws Exception
     */
	public void convertValues(EntryDefinition entry, Collection modifications)
			throws Exception {
		Map attributes = entry.getAttributes();

		for (Iterator i = modifications.iterator(); i.hasNext();) {
			LDAPModification modification = (LDAPModification) i.next();

			LDAPAttribute attribute = modification.getAttribute();
			String attributeName = attribute.getName();
			String values[] = attribute.getStringValueArray();

			AttributeDefinition attr = (AttributeDefinition) attributes.get(attributeName);
			if (attr == null) continue;

			String encryption = attr.getEncryption();
			String encoding = attr.getEncoding();

			for (int j = 0; j < values.length; j++) {
				log.debug("old " + attributeName + ": " + values[j]);
				attribute.removeValue(values[j]);
				values[j] = PasswordUtil.encrypt(encryption, encoding,
						values[j]);
				log.debug("new " + attributeName + ": " + values[j]);
				attribute.addValue(values[j]);
			}
		}
	}

	public int modifyVirtualEntry(
            PenroseConnection connection,
            Entry entry,
			Collection modifications)
            throws Exception {

		EntryDefinition entryDefinition = entry.getEntryDefinition();

		convertValues(entryDefinition, modifications);

		AttributeValues oldValues = entry.getAttributeValues();

		log.debug("--- old values:");
		log.debug(entry);

		log.debug("--- perform modification:");
		AttributeValues newValues = new AttributeValues(oldValues);

        Schema schema = engineContext.getSchema();
		Collection objectClasses = schema.getObjectClasses(entryDefinition);
		//log.debug("Object Classes: " + objectClasses);

		for (Iterator i = modifications.iterator(); i.hasNext();) {
			LDAPModification modification = (LDAPModification) i.next();

			LDAPAttribute attribute = modification.getAttribute();
			String attributeName = attribute.getName();

			if (attributeName.equals("entryCSN"))
				continue; // ignore
			if (attributeName.equals("modifiersName"))
				continue; // ignore
			if (attributeName.equals("modifyTimestamp"))
				continue; // ignore

			if (attributeName.equals("objectClass"))
				return LDAPException.OBJECT_CLASS_MODS_PROHIBITED;

			// check if the attribute is defined in the object class

			boolean found = false;
			for (Iterator j = objectClasses.iterator(); j.hasNext();) {
				ObjectClass oc = (ObjectClass) j.next();
				log.debug("Object Class: " + oc.getName());
				log.debug(" - required: " + oc.getRequiredAttributes());
				log.debug(" - optional: " + oc.getOptionalAttributes());

				if (oc.getRequiredAttributes().contains(attributeName)
						|| oc.getOptionalAttributes().contains(attributeName)) {
					found = true;
					break;
				}
			}

			if (!found) {
				log.debug("Can't find attribute " + attributeName
						+ " in object classes");
				return LDAPException.OBJECT_CLASS_VIOLATION;
			}

			String attributeValues[] = attribute.getStringValueArray();
			Set newAttrValues = new HashSet();
			for (int j = 0; j < attributeValues.length; j++) {
				newAttrValues.add(attributeValues[j]);
			}

			Collection value = newValues.get(attributeName);
			log.debug("old value " + attributeName + ": "
					+ newValues.get(attributeName));

			Set newValue = new HashSet();
            if (value != null) newValue.addAll(value);

			switch (modification.getOp()) {
			case LDAPModification.ADD:
				newValue.addAll(newAttrValues);
				break;
			case LDAPModification.DELETE:
				if (attributeValues.length == 0) {
					newValue.clear();
				} else {
					newValue.removeAll(newAttrValues);
				}
				break;
			case LDAPModification.REPLACE:
				newValue = newAttrValues;
				break;
			}

			newValues.set(attributeName, newValue);

			log.debug("new value " + attributeName + ": "
					+ newValues.get(attributeName));
		}

		log.debug("--- new values:");
		log.debug(entryDefinition.toString(newValues));

        return modify(entryDefinition, oldValues, newValues);
	}

	public int modifyStaticEntry(EntryDefinition entry, Collection modifications)
			throws Exception {

		convertValues(entry, modifications);

		Map attributes = entry.getAttributes();

		for (Iterator i = modifications.iterator(); i.hasNext();) {
			LDAPModification modification = (LDAPModification) i.next();

			LDAPAttribute attribute = modification.getAttribute();
			String attributeName = attribute.getName();

			String attributeValues[] = attribute.getStringValueArray();

			switch (modification.getOp()) {
			case LDAPModification.ADD:
				for (int j = 0; j < attributeValues.length; j++) {
					String v = "\"" + attributeValues[j] + "\"";
					addAttribute(attributes, attributeName, v);
				}
				break;

			case LDAPModification.DELETE:
				if (attributeValues.length == 0) {
					deleteAttribute(attributes, attributeName);

				} else {
					for (int j = 0; j < attributeValues.length; j++) {
						String v = "\"" + attributeValues[j] + "\"";
						deleteAttribute(attributes, attributeName, v);
					}
				}
				break;
			case LDAPModification.REPLACE:
				deleteAttribute(attributes, attributeName);

				for (int j = 0; j < attributeValues.length; j++) {
					String v = "\"" + attributeValues[j] + "\"";
					addAttribute(attributes, attributeName, v);
				}
				break;
			}
		}

		/*
		 * for (Iterator i = attributes.iterator(); i.hasNext(); ) { AttributeDefinition
		 * attribute = (AttributeDefinition)i.next(); log.debug(attribute.getName()+":
		 * "+attribute.getExpression()); }
		 */
		return LDAPException.SUCCESS;
	}

	public void addAttribute(Map attributes, String name, String value)
			throws Exception {

		AttributeDefinition attribute = (AttributeDefinition) attributes.get(name);

		if (attribute == null) {
			attribute = new AttributeDefinition(name, value);
			attributes.put(name, attribute);

		} else {
			if (attribute.getExpression().equals(value))
				return; // if already exists, don't add
			attribute.setExpression(value);
		}
	}

	public void deleteAttribute(Map attributes, String name) throws Exception {
		attributes.remove(name);
	}

	public void deleteAttribute(Map attributes, String name, String value)
			throws Exception {

		AttributeDefinition attribute = (AttributeDefinition) attributes.get(name);
		if (attribute == null) return;

		Interpreter interpreter = engineContext.newInterpreter();

		String attrValue = (String)interpreter.eval(attribute.getExpression());
		if (attrValue.equals(value)) attributes.remove(name);
	}

    public int modify(EntryDefinition entry, AttributeValues oldValues, AttributeValues newValues) throws Exception {

        Date date = new Date();

        Collection sources = entry.getSources();

        for (Iterator iterator = sources.iterator(); iterator.hasNext();) {
            Source source = (Source) iterator.next();

            int result = modify(source, entry, oldValues, newValues, date);

            if (result != LDAPException.SUCCESS) return result;
        }

        engine.getCache().delete(entry, oldValues, date);
        engine.getCache().insert(entry, newValues, date);

        return LDAPException.SUCCESS;
    }

    /**
     * Modify virtual entry in the LDAP tree
     *
     * @param entry
     * @param source
     * @param oldValues
     * @param newValues
     * @return return code
     * @throws Exception
     */
    public int modify(Source source, EntryDefinition entry, AttributeValues oldValues, AttributeValues newValues, Date date) throws Exception {

        log.debug("----------------------------------------------------------------");
        log.debug("Updating entry in " + source.getName());
        //log.debug("Old values: " + oldValues);
        //log.debug("New values: " + newValues);

        MRSWLock lock = engine.getLock(source);
        lock.getWriteLock(Penrose.WAIT_TIMEOUT);

        try {

            Map oldEntries = engineContext.getTransformEngine().transform(source, oldValues);
            Map newEntries = engineContext.getTransformEngine().transform(source, newValues);

            //log.debug("Old entries: " + oldEntries);
            //log.debug("New entries: " + newEntries);

            Collection oldPKs = oldEntries.keySet();
            Collection newPKs = newEntries.keySet();

            log.debug("Old PKs: " + oldPKs);
            log.debug("New PKs: " + newPKs);

            Set addRows = new HashSet(newPKs);
            addRows.removeAll(oldPKs);
            log.debug("PKs to add: " + addRows);

            Set removeRows = new HashSet(oldPKs);
            removeRows.removeAll(newPKs);
            log.debug("PKs to remove: " + removeRows);

            Set replaceRows = new HashSet(oldPKs);
            replaceRows.retainAll(newPKs);
            log.debug("PKs to replace: " + replaceRows);

            // Add rows
            for (Iterator i = addRows.iterator(); i.hasNext();) {
                Row pk = (Row) i.next();
                AttributeValues newEntry = (AttributeValues) newEntries.get(pk);
                log.debug("ADDING ROW: " + newEntry);

                //AttributeValues attributes = engineContext.getTransformEngine().convert(newEntry);

                // Add row to source table in the source database/directory
                int rc = source.add(newEntry);
                if (rc != LDAPException.SUCCESS) return rc;

                // Add row to source table in the cache
                engine.getCache().insert(source, newEntry, date);
            }

            // Remove rows
            for (Iterator i = removeRows.iterator(); i.hasNext();) {
                Row pk = (Row) i.next();
                AttributeValues oldEntry = (AttributeValues) oldEntries.get(pk);
                log.debug("DELETE ROW: " + oldEntry);

                //AttributeValues attributes = engineContext.getTransformEngine().convert(oldEntry);

                // Delete row from source table in the source database/directory
                int rc = source.delete(oldEntry);
                if (rc != LDAPException.SUCCESS)
                    return rc;

                // Delete row from source table in the cache
                engine.getCache().delete(source, oldEntry, date);
            }

            // Replace rows
            for (Iterator i = replaceRows.iterator(); i.hasNext();) {
                Row pk = (Row) i.next();
                AttributeValues oldEntry = (AttributeValues) oldEntries.get(pk);
                AttributeValues newEntry = (AttributeValues) newEntries.get(pk);
                log.debug("REPLACE ROW: " + newEntry.toString());

                //AttributeValues oldAttributes = engineContext.getTransformEngine().convert(oldEntry);
                //AttributeValues newAttributes = engineContext.getTransformEngine().convert(newEntry);

                // Modify row from source table in the source database/directory
                int rc = source.modify(oldEntry, newEntry);
                if (rc != LDAPException.SUCCESS) return rc;

                // Modify row from source table in the cache
                engine.getCache().delete(source, oldEntry, date);
                engine.getCache().insert(source, newEntry, date);
            }

        } finally {
            lock.releaseWriteLock(Penrose.WAIT_TIMEOUT);
        }

        return LDAPException.SUCCESS;
    }
}