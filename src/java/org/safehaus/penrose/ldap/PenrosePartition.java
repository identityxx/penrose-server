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
package org.safehaus.penrose.ldap;

import org.apache.ldap.server.partition.AbstractDirectoryPartition;
import org.apache.ldap.common.filter.ExprNode;
import org.apache.log4j.Logger;
import org.ietf.ldap.*;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.session.PenroseSearchResults;
import org.safehaus.penrose.session.PenroseSession;
import org.safehaus.penrose.session.PenroseSearchResults;
import org.safehaus.penrose.session.PenroseSearchControls;

import javax.naming.*;
import javax.naming.directory.*;
import java.util.*;
import java.io.File;

/**
 * @author Endi S. Dewata
 */
public class PenrosePartition extends AbstractDirectoryPartition {

    //public Logger configLog = Logger.getLogger(Penrose.CONFIG_LOGGER);
    //public Logger engineLog = Logger.getLogger(Penrose.ENGINE_LOGGER);
    //public Logger searchLog = Logger.getLogger(Penrose.SEARCH_LOGGER);

    Penrose penrose;

    public void setPenrose(Penrose penrose) throws Exception {
        this.penrose = penrose;
    }

    public void doInit() throws NamingException {

        String name = getConfiguration().getName();

        File dir = new File("partitions/"+name);
        if (!dir.exists()) return;

        Logger log = Logger.getLogger(getClass());
        log.debug("-------------------------------------------------------------------------------");
        log.debug("Initializing "+name+" partition ...");
    }

    public void delete(Name dn) throws NamingException {
        Logger log = Logger.getLogger(getClass());
        log.info("Deleting \""+dn+"\"");

        try {
            PenroseSession session = penrose.newSession();
            if (session == null) throw new ServiceUnavailableException();

            int rc = session.delete(dn.toString());

            session.close();

            if (rc != LDAPException.SUCCESS) {
                throw new NamingException("RC: "+rc);
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new NamingException(e.getMessage());
        }
    }

    public void add(String upName, Name normName, Attributes attributes) throws NamingException {
        Logger log = Logger.getLogger(getClass());
        log.info("Adding \""+upName+"\"");

        if (getSuffix(true).equals(normName)) return;

        try {
            LDAPAttributeSet attributeSet = new LDAPAttributeSet();
            for (Enumeration e = attributes.getAll(); e.hasMoreElements(); ) {
                Attribute attribute = (Attribute)e.nextElement();
                LDAPAttribute attr = new LDAPAttribute(attribute.getID());

                for (Enumeration values = attribute.getAll(); values.hasMoreElements(); ) {
                    Object value = values.nextElement();
                    attr.addValue(value.toString());
                }

                attributeSet.add(attr);
            }

            LDAPEntry ldapEntry = new LDAPEntry(upName, attributeSet);

            PenroseSession session = penrose.newSession();
            if (session == null) throw new ServiceUnavailableException();

            int rc = session.add(ldapEntry);

            session.close();

            if (rc != LDAPException.SUCCESS) {
                throw new NamingException("RC: "+rc);
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new NamingException(e.getMessage());
        }
    }

    public void modify(Name dn, int i, Attributes attributes) throws NamingException {
        Logger log = Logger.getLogger(getClass());
        log.info("Modifying \""+dn+"\"");
        log.debug("changetype: modify");

        try {
            Collection modifications = new ArrayList();

            for (Enumeration e=attributes.getAll(); e.hasMoreElements(); ) {
                Attribute attribute = (Attribute)e.nextElement();
                String attrName = attribute.getID();

                int op = LDAPModification.REPLACE;
                LDAPAttribute attr = new LDAPAttribute(attrName);

                log.debug("replace: "+attrName);
                for (Enumeration values = attribute.getAll(); values.hasMoreElements(); ) {
                    Object value = values.nextElement();
                    log.debug(attrName+": "+value);
                    attr.addValue(value.toString());
                }
                log.debug("-");

                LDAPModification modification = new LDAPModification(op, attr);
                modifications.add(modification);
            }

            PenroseSession session = penrose.newSession();
            if (session == null) throw new ServiceUnavailableException();

            int rc = session.modify(dn.toString(), modifications);

            session.close();

            if (rc != LDAPException.SUCCESS) {
                throw new NamingException("RC: "+rc);
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new NamingException(e.getMessage());
        }
    }

    public void modify(Name dn, ModificationItem[] modificationItems) throws NamingException {
        Logger log = Logger.getLogger(getClass());
        log.info("Modifying \""+dn+"\"");
        log.debug("changetype: modify");

        try {
            Collection modifications = new ArrayList();

            for (int i=0; i<modificationItems.length; i++) {
                ModificationItem mi = modificationItems[i];
                Attribute attribute = mi.getAttribute();
                String attrName = attribute.getID();

                int op = LDAPModification.REPLACE;
                switch (mi.getModificationOp()) {
                    case DirContext.ADD_ATTRIBUTE:
                        log.debug("add: "+attrName);
                        op = LDAPModification.ADD;
                        break;
                    case DirContext.REPLACE_ATTRIBUTE:
                        log.debug("replace: "+attrName);
                        op = LDAPModification.REPLACE;
                        break;
                    case DirContext.REMOVE_ATTRIBUTE:
                        log.debug("delete: "+attrName);
                        op = LDAPModification.DELETE;
                        break;
                }

                LDAPAttribute attr = new LDAPAttribute(attrName);

                for (Enumeration values = attribute.getAll(); values.hasMoreElements(); ) {
                    Object value = values.nextElement();
                    log.debug(attrName+": "+value);
                    attr.addValue(value.toString());
                }
                log.debug("-");

                LDAPModification modification = new LDAPModification(op, attr);
                modifications.add(modification);
            }

            PenroseSession session = penrose.newSession();
            if (session == null) throw new ServiceUnavailableException();

            int rc = session.modify(dn.toString(), modifications);

            session.close();

            if (rc != LDAPException.SUCCESS) {
                throw new NamingException("RC: "+rc);
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new NamingException(e.getMessage());
        }
    }

    public NamingEnumeration list(Name dn) throws NamingException {
        Logger log = Logger.getLogger(getClass());
        log.info("Listing \""+dn+"\"");

        try {
            PenroseSession session = penrose.newSession();
            if (session == null) throw new ServiceUnavailableException();

            PenroseSearchControls sc = new PenroseSearchControls();
            sc.setScope(PenroseSearchControls.SCOPE_ONE);
            sc.setDereference(PenroseSearchControls.DEREF_ALWAYS);

            String baseDn = dn.toString();
            PenroseSearchResults results = session.search(
                    baseDn,
                    "(objectClass=*)",
                    sc);
/*
            int rc = results.getReturnCode();
            connection.close();

            if (rc != LDAPException.SUCCESS) {
                throw new NamingException("RC: "+rc);
            }
*/
            return new PenroseEnumeration(results);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new NamingException(e.getMessage());
        }
    }

    public NamingEnumeration search(Name base, Map env, ExprNode filter, SearchControls searchControls) throws NamingException {

        Logger log = Logger.getLogger(getClass());
        String deref = (String)env.get("java.naming.ldap.derefAliases");
        int scope = searchControls.getSearchScope();
        String returningAttributes[] = searchControls.getReturningAttributes();
        List attributeNames = returningAttributes == null ? new ArrayList() : Arrays.asList(returningAttributes);

        StringBuffer sb = new StringBuffer();
        filter.printToBuffer(sb);
        String newFilter = sb.toString();

        log.info("Searching \""+base+"\"");
        log.debug(" - deref: "+deref);
        log.debug(" - scope: "+scope);
        log.debug(" - filter: "+newFilter+" ("+filter.getClass().getName()+")");
        log.debug(" - attributeNames: "+attributeNames);

        try {
            PenroseSession session = penrose.newSession();
            if (session == null) throw new ServiceUnavailableException();

            PenroseSearchControls sc = new PenroseSearchControls();
            sc.setScope(searchControls.getSearchScope());
            sc.setDereference(PenroseSearchControls.DEREF_ALWAYS);
            sc.setAttributes(searchControls.getReturningAttributes());

            String baseDn = base.toString();
            PenroseSearchResults results = session.search(
                    baseDn,
                    newFilter,
                    sc);
/*
            int rc = results.getReturnCode();
            connection.close();

            if (rc != LDAPException.SUCCESS) return null;
            //throwNamingException(rc, baseDn);
*/
            return new PenroseEnumeration(results);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new NamingException(e.getMessage());
        }
    }

    public void throwNamingException(int rc, String message) throws NamingException {
        switch (rc) {
            case LDAPException.SUCCESS:
                break;

            case LDAPException.NO_SUCH_OBJECT:
                throw new NameNotFoundException(message);

            default:
                throw new NamingException("RC: "+rc);
        }
    }

    public Attributes lookup(Name dn) throws NamingException {
        Logger log = Logger.getLogger(getClass());
        log.debug("Looking up \""+dn+"\"");

        try {
            PenroseSession session = penrose.newSession();
            if (session == null) throw new ServiceUnavailableException();

            PenroseSearchControls sc = new PenroseSearchControls();
            sc.setScope(PenroseSearchControls.SCOPE_BASE);
            sc.setDereference(PenroseSearchControls.DEREF_ALWAYS);

            String baseDn = dn.toString();
            PenroseSearchResults results = session.search(
                    baseDn,
                    "(objectClass=*)",
                    sc);

            int rc = results.getReturnCode();
            session.close();

            if (rc != LDAPException.SUCCESS) return null;
            //throwNamingException(rc, baseDn);

            if (!results.hasNext()) {
                throw new NameNotFoundException("No such object.");
            }

            LDAPEntry result = (LDAPEntry)results.next();

            LDAPAttributeSet attributeSet = result.getAttributeSet();
            Attributes attributes = new BasicAttributes();

            for (Iterator j = attributeSet.iterator(); j.hasNext(); ) {
                LDAPAttribute attribute = (LDAPAttribute)j.next();
                Attribute attr = new BasicAttribute(attribute.getName());

                for (Enumeration k=attribute.getStringValues(); k.hasMoreElements(); ) {
                    String value = (String)k.nextElement();
                    attr.add(value);
                }

                attributes.put(attr);
            }

            return attributes;

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new NamingException(e.getMessage());
        }
    }

    public Attributes lookup(Name dn, String[] attrIds) throws NamingException {
        Logger log = Logger.getLogger(getClass());
        log.debug("Looking up \""+dn+"\", \""+attrIds+"\"");

        for (int i=0; attrIds != null && i<attrIds.length; i++) {
            log.debug("- Attribute: "+attrIds[i]);
        }

        return null;
    }

    public boolean hasEntry(Name name) throws NamingException {
        Logger log = Logger.getLogger(getClass());
        log.info("Checking \""+name+"\"");

        try {
            PenroseSession session = penrose.newSession();
            if (session == null) throw new ServiceUnavailableException();

            PenroseSearchControls sc = new PenroseSearchControls();
            sc.setScope(PenroseSearchControls.SCOPE_BASE);
            sc.setDereference(PenroseSearchControls.DEREF_ALWAYS);

            String base = name.toString();
            PenroseSearchResults results = session.search(
                    base,
                    "(objectClass=*)",
                    sc);

            boolean result = results.getReturnCode() == LDAPException.SUCCESS && results.size() == 1;

            session.close();

            return result;

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new NamingException(e.getMessage());
        }
    }

    public void modifyRn(Name name, String newRn, boolean deleteOldRn) throws NamingException {
        Logger log = Logger.getLogger(getClass());
        log.info("Renaming \""+name+"\"");
    }

    public void move(Name oriChildName, Name newParentName) throws NamingException {
        Logger log = Logger.getLogger(getClass());
        log.info("Moving \""+oriChildName+"\" to \""+newParentName+"\"");
    }

    public void move(Name oriChildName, Name newParentName, String newRn, boolean deleteOldRn) throws NamingException {
        Logger log = Logger.getLogger(getClass());
        log.info("Moving \""+oriChildName+"\" to \""+newParentName+"\"");
    }

    public void sync() throws NamingException {
        //log.info("sync();");
    }

    public void close() throws NamingException {
        Logger log = Logger.getLogger(getClass());
        try {
            penrose.stop();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new NamingException(e.getMessage());
        }
    }

    public boolean isClosed() {
        //Logger log = Logger.getLogger(PenrosePartition.class);
        //log.info("isClosed();");
        return false;
    }
}
