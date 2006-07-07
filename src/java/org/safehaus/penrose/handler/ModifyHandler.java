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
import org.safehaus.penrose.session.PenroseSearchControls;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.schema.ObjectClass;
import org.safehaus.penrose.schema.AttributeType;
import org.safehaus.penrose.util.PasswordUtil;
import org.safehaus.penrose.util.BinaryUtil;
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.util.EntryUtil;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.service.ServiceConfig;
import org.ietf.ldap.*;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import javax.naming.directory.DirContext;
import javax.naming.directory.ModificationItem;
import javax.naming.directory.Attribute;
import javax.naming.directory.BasicAttribute;
import javax.naming.NamingEnumeration;
import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class ModifyHandler {

    Logger log = LoggerFactory.getLogger(getClass());

    private Handler handler;

    public ModifyHandler(Handler handler) {
        this.handler = handler;
    }

    public int modify(PenroseSession session, String dn, Collection modifications)
			throws Exception {

        int rc;
        try {
            log.warn("Modify entry \""+dn+"\".");

            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("MODIFY:", 80));
            if (session != null && session.getBindDn() != null) {
                log.debug(Formatter.displayLine(" - Bind DN: " + session.getBindDn(), 80));
            }
            log.debug(Formatter.displayLine(" - DN: " + dn, 80));

            log.debug(Formatter.displayLine(" - Attributes: " + dn, 80));
            for (Iterator i=modifications.iterator(); i.hasNext(); ) {
                ModificationItem mi = (ModificationItem)i.next();
                Attribute attribute = mi.getAttribute();
                String op = "replace";
                switch (mi.getModificationOp()) {
                    case DirContext.ADD_ATTRIBUTE:
                        op = "add";
                        break;
                    case DirContext.REMOVE_ATTRIBUTE:
                        op = "delete";
                        break;
                    case DirContext.REPLACE_ATTRIBUTE:
                        op = "replace";
                        break;
                }

                log.debug(Formatter.displayLine("   - "+op+": "+attribute.getID()+" => "+attribute.get(), 80));
            }

            log.debug(Formatter.displaySeparator(80));

            if (session != null && session.getBindDn() == null) {
                PenroseConfig penroseConfig = handler.getPenroseConfig();
                ServiceConfig serviceConfig = penroseConfig.getServiceConfig("LDAP");
                String s = serviceConfig == null ? null : serviceConfig.getParameter("allowAnonymousAccess");
                boolean allowAnonymousAccess = s == null ? true : new Boolean(s).booleanValue();
                if (!allowAnonymousAccess) {
                    return LDAPException.INSUFFICIENT_ACCESS_RIGHTS;
                }
            }

            String ndn = LDAPDN.normalize(dn);

            Entry entry = handler.getFindHandler().find(session, ndn);
            if (entry == null) {
                log.debug("Entry "+entry.getDn()+" not found");
                return LDAPException.NO_SUCH_OBJECT;
            }

            rc = performModify(session, entry, modifications);
            if (rc != LDAPException.SUCCESS) return rc;

            // refreshing entry cache

            handler.getEngine().getEntryCache().remove(entry);

            PenroseSession adminSession = handler.getPenrose().newSession();
            adminSession.setBindDn(handler.getPenroseConfig().getRootDn());

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

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            rc = LDAPException.OPERATIONS_ERROR;
        }

        if (rc == LDAPException.SUCCESS) {
            log.warn("Modify operation succeded.");
        } else {
            log.warn("Modify operation failed. RC="+rc);
        }

        return rc;
    }

    public int performModify(PenroseSession session, Entry entry, Collection modifications)
			throws Exception {

        log.debug("Modifying "+entry.getDn());

        int rc = handler.getACLEngine().checkModify(session, entry.getDn(), entry.getEntryMapping());
        if (rc != LDAPException.SUCCESS) {
            log.debug("Not authorized to modify "+entry.getDn());
            return rc;
        }

        Collection normalizedModifications = new ArrayList();

		for (Iterator i = modifications.iterator(); i.hasNext();) {
			ModificationItem modification = (ModificationItem) i.next();

			Attribute attribute = modification.getAttribute();
			String attributeName = attribute.getID();

            AttributeType at = handler.getSchemaManager().getAttributeType(attributeName);
            if (at == null) return LDAPException.UNDEFINED_ATTRIBUTE_TYPE;

            attributeName = at.getName();

            switch (modification.getModificationOp()) {
                case DirContext.ADD_ATTRIBUTE:
                    log.debug("add: " + attributeName);
                    break;
                case DirContext.REMOVE_ATTRIBUTE:
                    log.debug("delete: " + attributeName);
                    break;
                case DirContext.REPLACE_ATTRIBUTE:
                    log.debug("replace: " + attributeName);
                    break;
            }

            Attribute normalizedAttribute = new BasicAttribute(attributeName);
            for (NamingEnumeration j=attribute.getAll(); j.hasMore(); ) {
                Object value = j.next();
                normalizedAttribute.add(value);
                log.debug(attributeName + ": "+value);
            }

            log.debug("-");

            ModificationItem normalizedModification = new ModificationItem(modification.getModificationOp(), normalizedAttribute);
            normalizedModifications.add(normalizedModification);
		}

        modifications = normalizedModifications;

        log.info("");

        EntryMapping entryMapping = entry.getEntryMapping();

        PartitionManager partitionManager = handler.getPartitionManager();
        Partition partition = partitionManager.getPartition(entryMapping);

        if (partition.isProxy(entryMapping)) {
            log.debug("Modifying "+entry.getDn()+" via proxy");
            handler.getEngine().modifyProxy(partition, entryMapping, entry, modifications);
            return LDAPException.SUCCESS;
        }

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
			ModificationItem modification = (ModificationItem) i.next();

			Attribute attribute = modification.getAttribute();
			String attributeName = attribute.getID();

			AttributeMapping attr = entry.getAttributeMapping(attributeName);
			if (attr == null) continue;

			String encryption = attr.getEncryption();
			String encoding = attr.getEncoding();

			for (NamingEnumeration j=attribute.getAll(); j.hasMore(); ) {
                Object value = j.next();

				//log.debug("old " + attributeName + ": " + values[j]);
				attribute.remove(value);

                byte[] bytes;
                if (value instanceof byte[]) {
                    bytes = PasswordUtil.encrypt(encryption, (byte[])value);
                } else {
                    bytes = PasswordUtil.encrypt(encryption, value.toString());
                }

                value = BinaryUtil.encode(encoding, bytes);

				//log.debug("new " + attributeName + ": " + values[j]);
				attribute.add(value);
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

		//convertValues(entryMapping, modifications);

		log.debug("Old entry:");
		log.debug("\n"+EntryUtil.toString(entry));

		log.debug("--- perform modification:");
		AttributeValues newValues = new AttributeValues(oldValues);

		Collection objectClasses = handler.getSchemaManager().getObjectClasses(entryMapping);
        Collection objectClassNames = new ArrayList();
        for (Iterator i=objectClasses.iterator(); i.hasNext(); ) {
            ObjectClass oc = (ObjectClass)i.next();
            objectClassNames.add(oc.getName());
        }
        log.debug("Object classes: "+objectClassNames);

		for (Iterator i = modifications.iterator(); i.hasNext();) {
			ModificationItem modification = (ModificationItem)i.next();

			Attribute attribute = modification.getAttribute();
			String attributeName = attribute.getID();

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

				if (oc.containsRequiredAttribute(attributeName) || oc.containsOptionalAttribute(attributeName)) {
					found = true;
					break;
				}
			}

			if (!found) {
				log.debug("Can't find attribute " + attributeName
						+ " in object classes "+objectClasses);
				return LDAPException.OBJECT_CLASS_VIOLATION;
			}

			Set newAttrValues = new HashSet();
			for (NamingEnumeration j=attribute.getAll(); j.hasMore(); ) {
                Object value = j.next();
				newAttrValues.add(value);
			}

			Collection value = newValues.get(attributeName);
			log.debug("old value " + attributeName + ": "
					+ newValues.get(attributeName));

			Set newValue = new HashSet();
            if (value != null) newValue.addAll(value);

			switch (modification.getModificationOp()) {
                case DirContext.ADD_ATTRIBUTE:
                    newValue.addAll(newAttrValues);
                    break;
                case DirContext.REMOVE_ATTRIBUTE:
                    if (attribute.get() == null) {
                        newValue.clear();
                    } else {
                        newValue.removeAll(newAttrValues);
                    }
                    break;
                case DirContext.REPLACE_ATTRIBUTE:
                    newValue = newAttrValues;
                    break;
			}

			newValues.set(attributeName, newValue);

			log.debug("new value " + attributeName + ": "
					+ newValues.get(attributeName));
		}

        Entry newEntry = new Entry(entry.getDn(), entryMapping, entry.getSourceValues(), newValues);

		log.debug("New entry:");
		log.debug("\n"+EntryUtil.toString(newEntry));

        return handler.getEngine().modify(entry, newValues);
	}


    public int modifyStaticEntry(EntryMapping entry, Collection modifications)
            throws Exception {

        //convertValues(entry, modifications);

        for (Iterator i = modifications.iterator(); i.hasNext();) {
            ModificationItem modification = (ModificationItem) i.next();

            Attribute attribute = modification.getAttribute();
            String attributeName = attribute.getID();

            switch (modification.getModificationOp()) {
                case DirContext.ADD_ATTRIBUTE:
                    for (NamingEnumeration j=attribute.getAll(); j.hasMore(); ) {
                        Object v = j.next();
                        addAttribute(entry, attributeName, v);
                    }
                    break;

                case DirContext.REMOVE_ATTRIBUTE:
                    if (attribute.get() == null) {
                        deleteAttribute(entry, attributeName);

                    } else {
                        for (NamingEnumeration j=attribute.getAll(); j.hasMore(); ) {
                            Object v = j.next();
                            deleteAttribute(entry, attributeName, v);
                        }
                    }
                    break;

                case DirContext.REPLACE_ATTRIBUTE:
                    deleteAttribute(entry, attributeName);

                    for (NamingEnumeration j=attribute.getAll(); j.hasMore(); ) {
                        Object v = j.next();
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

    public void addAttribute(EntryMapping entry, String name, Object value)
			throws Exception {

		AttributeMapping attribute = entry.getAttributeMapping(name);

		if (attribute == null) {
			attribute = new AttributeMapping(name, AttributeMapping.CONSTANT, value, true);
			entry.addAttributeMapping(attribute);

		} else {
			attribute.setConstant(value);
		}
	}

    public void deleteAttribute(EntryMapping entry, String name) throws Exception {
		entry.removeAttributeMappings(name);
	}

    public void deleteAttribute(EntryMapping entryMapping, String name, Object value)
			throws Exception {

		AttributeMapping attributeMapping = entryMapping.getAttributeMapping(name);
		if (attributeMapping == null) return;

        if (!AttributeMapping.CONSTANT.equals(attributeMapping.getType())) return;

        Object attrValue = attributeMapping.getConstant();
		if (!attrValue.equals(value)) return;

        entryMapping.removeAttributeMapping(attributeMapping);
	}

    public Handler getHandler() {
        return handler;
    }

    public void setHandler(Handler handler) {
        this.handler = handler;
    }
}
