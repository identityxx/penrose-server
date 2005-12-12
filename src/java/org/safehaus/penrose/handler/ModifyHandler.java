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
import org.safehaus.penrose.event.ModifyEvent;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.schema.ObjectClass;
import org.safehaus.penrose.util.PasswordUtil;
import org.safehaus.penrose.mapping.*;
import org.ietf.ldap.*;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class ModifyHandler {

    Logger log = Logger.getLogger(getClass());

    private Handler handler;

    public ModifyHandler(Handler handler) throws Exception {
        this.handler = handler;
    }

    public int modify(PenroseSession session, String dn, Collection modifications)
			throws Exception {

        log.info("-------------------------------------------------");
		log.info("MODIFY:");
		if (session != null && session.getBindDn() != null) log.info(" - Bind DN: " + session.getBindDn());
        log.info(" - DN: " + dn);
        log.debug("-------------------------------------------------");
		log.debug("changetype: modify");

		for (Iterator i = modifications.iterator(); i.hasNext();) {
			LDAPModification modification = (LDAPModification) i.next();

			LDAPAttribute attribute = modification.getAttribute();
			String attributeName = attribute.getName();
			String values[] = attribute.getStringValueArray();

			switch (modification.getOp()) {
			case LDAPModification.ADD:
				log.debug("add: " + attributeName);
				for (int j = 0; j < values.length; j++)
					log.debug(attributeName + ": " + values[j]);
				break;
			case LDAPModification.DELETE:
				log.debug("delete: " + attributeName);
				for (int j = 0; j < values.length; j++)
					log.debug(attributeName + ": " + values[j]);
				break;
			case LDAPModification.REPLACE:
				log.debug("replace: " + attributeName);
				for (int j = 0; j < values.length; j++)
					log.debug(attributeName + ": " + values[j]);
				break;
			}
			log.debug("-");
		}

        log.info("");

        ModifyEvent beforeModifyEvent = new ModifyEvent(this, ModifyEvent.BEFORE_MODIFY, session, dn, modifications);
        handler.postEvent(dn, beforeModifyEvent);

        int rc = performModify(session, dn, modifications);

        handler.getSearchHandler().search(
                session,
                dn,
                LDAPConnection.SCOPE_SUB,
                LDAPSearchConstraints.DEREF_NEVER,
                "(objectClass=*)",
                new ArrayList(),
                new PenroseSearchResults()
        );

        ModifyEvent afterModifyEvent = new ModifyEvent(this, ModifyEvent.AFTER_MODIFY, session, dn, modifications);
        afterModifyEvent.setReturnCode(rc);
        handler.postEvent(dn, afterModifyEvent);

        return rc;
    }

    public int performModify(PenroseSession session, String dn, Collection modifications)
			throws Exception {

		String ndn = LDAPDN.normalize(dn);

        Entry entry = handler.getSearchHandler().find(session, ndn);
        if (entry == null) return LDAPException.NO_SUCH_OBJECT;

        int rc = handler.getACLEngine().checkModify(session, entry);
        if (rc != LDAPException.SUCCESS) return rc;

        EntryMapping entryMapping = entry.getEntryMapping();
        Partition partition = handler.getPartitionManager().getPartition(entryMapping);
        if (partition.isDynamic(entryMapping)) {
            return modifyVirtualEntry(session, entry, modifications);

        } else {
            return modifyStaticEntry(entryMapping, modifications);
        }
	}

    /**
     * Apply encryption/encoding at attribute level.
     * @param entry
     * @param modifications
     * @throws Exception
     */
	public void convertValues(EntryMapping entry, Collection modifications)
			throws Exception {

		for (Iterator i = modifications.iterator(); i.hasNext();) {
			LDAPModification modification = (LDAPModification) i.next();

			LDAPAttribute attribute = modification.getAttribute();
			String attributeName = attribute.getName();
			String values[] = attribute.getStringValueArray();

			AttributeMapping attr = entry.getAttributeMapping(attributeName);
			if (attr == null) continue;

			String encryption = attr.getEncryption();
			String encoding = attr.getEncoding();

			for (int j = 0; j < values.length; j++) {
				log.debug("old " + attributeName + ": " + values[j]);
				attribute.removeValue(values[j]);

                byte[] bytes = PasswordUtil.encrypt(encryption, values[j]);
                values[j] = PasswordUtil.encode(encoding, bytes);

				log.debug("new " + attributeName + ": " + values[j]);
				attribute.addValue(values[j]);
			}
		}
	}

    public int modifyVirtualEntry(
            PenroseSession session,
            Entry entry,
			Collection modifications)
            throws Exception {

		EntryMapping entryMapping = entry.getEntryMapping();
        AttributeValues oldValues = entry.getAttributeValues();

		convertValues(entryMapping, modifications);

		log.debug("Old entry:");
		log.debug("\n"+entry.toString());

		log.debug("--- perform modification:");
		AttributeValues newValues = new AttributeValues(oldValues);

		Collection objectClasses = handler.getSchemaManager().getObjectClasses(entryMapping);
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
				//log.debug("Object Class: " + oc.getName());
				//log.debug(" - required: " + oc.getRequiredAttributes());
				//log.debug(" - optional: " + oc.getOptionalAttributes());

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

        Entry newEntry = new Entry(entry.getDn(), entryMapping, entry.getSourceValues(), newValues);

		log.debug("New entry:");
		log.debug("\n"+newEntry.toString());

        return handler.getEngine().modify(entry, newValues);
	}


        public int modifyStaticEntry(EntryMapping entry, Collection modifications)
                throws Exception {

            convertValues(entry, modifications);

            for (Iterator i = modifications.iterator(); i.hasNext();) {
                LDAPModification modification = (LDAPModification) i.next();

                LDAPAttribute attribute = modification.getAttribute();
                String attributeName = attribute.getName();

                String attributeValues[] = attribute.getStringValueArray();

                switch (modification.getOp()) {
                case LDAPModification.ADD:
                    for (int j = 0; j < attributeValues.length; j++) {
                        String v = "\"" + attributeValues[j] + "\"";
                        addAttribute(entry, attributeName, v);
                    }
                    break;

                case LDAPModification.DELETE:
                    if (attributeValues.length == 0) {
                        deleteAttribute(entry, attributeName);

                    } else {
                        for (int j = 0; j < attributeValues.length; j++) {
                            String v = "\"" + attributeValues[j] + "\"";
                            deleteAttribute(entry, attributeName, v);
                        }
                    }
                    break;
                case LDAPModification.REPLACE:
                    deleteAttribute(entry, attributeName);

                    for (int j = 0; j < attributeValues.length; j++) {
                        String v = "\"" + attributeValues[j] + "\"";
                        addAttribute(entry, attributeName, v);
                    }
                    break;
                }
            }

		/*
		 * for (Iterator i = attributes.iterator(); i.hasNext(); ) { AttributeMapping
		 * attribute = (AttributeMapping)i.next(); log.debug(attribute.getName()+":
		 * "+attribute.getExpression()); }
		 */
		return LDAPException.SUCCESS;
	}

    public void addAttribute(EntryMapping entry, String name, String value)
			throws Exception {

		AttributeMapping attribute = entry.getAttributeMapping(name);

		if (attribute == null) {
			attribute = new AttributeMapping(name, value);
			entry.addAttributeMapping(attribute);

		} else {
			attribute.setConstant(value);
		}
	}

    public void deleteAttribute(EntryMapping entry, String name) throws Exception {
		entry.removeAttributeMapping(name);
	}

    public void deleteAttribute(EntryMapping entry, String name, String value)
			throws Exception {

		AttributeMapping attributeMapping = entry.getAttributeMapping(name);
		if (attributeMapping == null) return;

		Interpreter interpreter = handler.getInterpreterFactory().newInstance();

		String attrValue = (String)interpreter.eval(attributeMapping);
		if (attrValue.equals(value)) entry.removeAttributeMapping(name);

        interpreter.clear();
	}

    public Handler getHandler() {
        return handler;
    }

    public void setHandler(Handler handler) {
        this.handler = handler;
    }
}
