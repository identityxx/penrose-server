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
import org.safehaus.penrose.session.PenroseSearchResults;
import org.safehaus.penrose.session.PenroseSearchControls;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.schema.AttributeType;
import org.safehaus.penrose.util.PasswordUtil;
import org.safehaus.penrose.util.BinaryUtil;
import org.safehaus.penrose.util.ExceptionUtil;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.engine.Engine;
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

    public int modify(
            PenroseSession session,
            Partition partition,
            Entry entry,
            Collection modifications
    ) throws Exception {

        int rc;
        try {
            rc = performModify(session, partition, entry, modifications);
            if (rc != LDAPException.SUCCESS) return rc;

            // refreshing entry cache

            EntryMapping entryMapping = entry.getEntryMapping();
            handler.getEntryCacheManager().remove(partition, entryMapping, entry.getDn());

            PenroseSession adminSession = handler.getPenrose().newSession();
            adminSession.setBindDn(handler.getPenrose().getPenroseConfig().getRootDn());

            PenroseSearchResults results = new PenroseSearchResults();

            PenroseSearchControls sc = new PenroseSearchControls();
            sc.setScope(PenroseSearchControls.SCOPE_SUB);

            adminSession.search(
                    entry.getDn(),
                    "(objectClass=*)",
                    sc,
                    results
            );

            while (results.hasNext()) results.next();

        } catch (LDAPException e) {
            rc = e.getResultCode();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            rc = ExceptionUtil.getReturnCode(e);
        }

        if (rc == LDAPException.SUCCESS) {
            log.warn("Modify operation succeded.");
        } else {
            log.warn("Modify operation failed. RC="+rc);
        }

        return rc;
    }

    public int performModify(
            PenroseSession session,
            Partition partition,
            Entry entry,
            Collection modifications
    ) throws Exception {

        log.debug("Modifying "+entry.getDn());

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

        String engineName = "DEFAULT";
        if (partition.isProxy(entryMapping)) engineName = "PROXY";

        Engine engine = handler.getEngine(engineName);

        if (engine == null) {
            log.debug("Engine "+engineName+" not found");
            return LDAPException.OPERATIONS_ERROR;
        }

        return engine.modify(session, partition, entry, modifications);
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
                    bytes = PasswordUtil.encrypt(encryption, null, (byte[])value);
                } else {
                    bytes = PasswordUtil.encrypt(encryption, null, value.toString());
                }

                value = BinaryUtil.encode(encoding, bytes);

				//log.debug("new " + attributeName + ": " + values[j]);
				attribute.add(value);
			}
		}
	}


    public int modifyStaticEntry(Partition partition, EntryMapping entry, Collection modifications)
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

    public void addAttribute(EntryMapping entryMapping, String name, Object value)
			throws Exception {

		AttributeMapping attributeMapping = entryMapping.getAttributeMapping(name);

		if (attributeMapping == null) {
			attributeMapping = new AttributeMapping(name, AttributeMapping.CONSTANT, value, true);
			entryMapping.addAttributeMapping(attributeMapping);

		} else {
			attributeMapping.setConstant(value);
		}
	}

    public void deleteAttribute(EntryMapping entry, String name) throws Exception {
		entry.removeAttributeMappings(name);
	}

    public void deleteAttribute(EntryMapping entryMapping, String name, Object value)
			throws Exception {

		AttributeMapping attributeMapping = entryMapping.getAttributeMapping(name);
		if (attributeMapping == null) return;

        Object attrValue = attributeMapping.getConstant();
        if (attrValue == null || !attrValue.equals(value)) return;

        entryMapping.removeAttributeMapping(attributeMapping);
	}

    public Handler getHandler() {
        return handler;
    }

    public void setHandler(Handler handler) {
        this.handler = handler;
    }
}
