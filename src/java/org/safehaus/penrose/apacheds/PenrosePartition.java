/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.apacheds;

import org.apache.ldap.server.partition.AbstractPartition;
import org.apache.ldap.common.filter.ExprNode;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.ietf.ldap.*;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.SearchResults;
import org.safehaus.penrose.PenroseConnection;

import javax.naming.Name;
import javax.naming.NamingException;
import javax.naming.NamingEnumeration;
import javax.naming.NameNotFoundException;
import javax.naming.directory.*;
import java.util.*;
import java.io.File;

/**
 * @author Endi S. Dewata
 */
public class PenrosePartition extends AbstractPartition {

    //public Logger configLog = Logger.getLogger(Penrose.CONFIG_LOGGER);
    //public Logger engineLog = Logger.getLogger(Penrose.ENGINE_LOGGER);
    //public Logger searchLog = Logger.getLogger(Penrose.SEARCH_LOGGER);

    public static Penrose penrose;

    public void init() throws NamingException {
        try {

            if (penrose == null) createPenrose();

        } catch (Exception e) {
            e.printStackTrace();
            throw new NamingException(e.getMessage());
        }
    }

    public Penrose createPenrose() throws Exception {
        //System.out.println("[PenrosePartition] Initializing ...");

        String homeDirectory = getInitParameter(Penrose.PENROSE_HOME);
        String loggerConfig = homeDirectory == null ? null : homeDirectory+"/"+getInitParameter(Penrose.LOGGER_CONFIG);

        if (loggerConfig != null) {
            PropertyConfigurator.configure(loggerConfig);
        }

        Properties properties = new Properties();

        for (Enumeration e = getPartitionConfig().getInitParameterNames(); e.hasMoreElements(); ) {
            String name = (String)e.nextElement();
            String value = getPartitionConfig().getInitParameter(name);

            properties.setProperty(name, value);
        }

        penrose = new Penrose();
        penrose.setRoot("uid=admin,ou=system", null);
        penrose.setProperties(properties);
        penrose.init();

        Logger log = Logger.getLogger(PenrosePartition.class);

        String penroseHome = properties.getProperty(Penrose.PENROSE_HOME);
        File schemaDir = new File(penroseHome+"/schema");
        log.debug("Loading schema from "+schemaDir.getAbsolutePath());

        File schemaFiles[] = schemaDir.listFiles();
        for (int i=0; i<schemaFiles.length; i++) {
            if (schemaFiles[i].isDirectory()) continue;
            penrose.loadSchema(schemaFiles[i].getAbsolutePath());
        }

        return penrose;
    }

    public static Penrose getPenrose() throws Exception {
        return penrose;
    }
    
    public void delete(Name dn) throws NamingException {
        Logger log = Logger.getLogger(PenrosePartition.class);
        log.info("Deleting \""+dn+"\"");

        try {
            PenroseConnection connection = penrose.openConnection();

            int rc = connection.delete(dn.toString());

            connection.close();

            if (rc != LDAPException.SUCCESS) {
                throw new NamingException("RC: "+rc);
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new NamingException(e.getMessage());
        }
    }

    public void add(String upName, Name normName, Attributes entry) throws NamingException {
        Logger log = Logger.getLogger(PenrosePartition.class);
        log.info("Adding \""+upName+"\"");

        if (getNormalizedSuffix().equals(normName)) return;

        try {
            LDAPAttributeSet attributeSet = new LDAPAttributeSet();
            for (Enumeration attributes = entry.getAll(); attributes.hasMoreElements(); ) {
                Attribute attribute = (Attribute)attributes.nextElement();
                LDAPAttribute attr = new LDAPAttribute(attribute.getID());

                for (Enumeration values = attribute.getAll(); values.hasMoreElements(); ) {
                    Object value = values.nextElement();
                    attr.addValue(value.toString());
                }

                attributeSet.add(attr);
            }

            LDAPEntry ldapEntry = new LDAPEntry(upName, attributeSet);

            PenroseConnection connection = penrose.openConnection();

            int rc = connection.add(ldapEntry);

            connection.close();

            if (rc != LDAPException.SUCCESS) {
                throw new NamingException("RC: "+rc);
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new NamingException(e.getMessage());
        }
    }

    public void modify(Name dn, int i, Attributes attributes) throws NamingException {
        Logger log = Logger.getLogger(PenrosePartition.class);
        log.info("Modifying \""+dn+"\"");
        log.debug("changetype: modify");

        try {
            List modifications = new ArrayList();

            for (Enumeration e=attributes.getAll(); e.hasMoreElements(); ) {
                Attribute attribute = (Attribute)e.nextElement();
                String name = attribute.getID();

                int op = LDAPModification.REPLACE;
                LDAPAttribute attr = new LDAPAttribute(name);

                log.debug("replace: "+name);
                for (Enumeration values = attribute.getAll(); values.hasMoreElements(); ) {
                    Object value = values.nextElement();
                    log.debug(name+": "+value);
                    attr.addValue(value.toString());
                }
                log.debug("-");

                LDAPModification modification = new LDAPModification(op, attr);
                modifications.add(modification);
            }

            PenroseConnection connection = penrose.openConnection();

            int rc = connection.modify(dn.toString(), modifications);

            connection.close();

            if (rc != LDAPException.SUCCESS) {
                throw new NamingException("RC: "+rc);
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new NamingException(e.getMessage());
        }
    }

    public void modify(Name dn, ModificationItem[] modificationItems) throws NamingException {
        Logger log = Logger.getLogger(PenrosePartition.class);
        log.info("Modifying \""+dn+"\"");
        log.debug("changetype: modify");

        try {
            List modifications = new ArrayList();

            for (int i=0; i<modificationItems.length; i++) {
                ModificationItem mi = modificationItems[i];
                Attribute attribute = mi.getAttribute();
                String name = attribute.getID();

                int op = LDAPModification.REPLACE;
                switch (mi.getModificationOp()) {
                    case DirContext.ADD_ATTRIBUTE:
                        log.debug("add: "+name);
                        op = LDAPModification.ADD;
                        break;
                    case DirContext.REPLACE_ATTRIBUTE:
                        log.debug("replace: "+name);
                        op = LDAPModification.REPLACE;
                        break;
                    case DirContext.REMOVE_ATTRIBUTE:
                        log.debug("delete: "+name);
                        op = LDAPModification.DELETE;
                        break;
                }

                LDAPAttribute attr = new LDAPAttribute(name);

                for (Enumeration values = attribute.getAll(); values.hasMoreElements(); ) {
                    Object value = values.nextElement();
                    log.debug(name+": "+value);
                    attr.addValue(value.toString());
                }
                log.debug("-");

                LDAPModification modification = new LDAPModification(op, attr);
                modifications.add(modification);
            }

            PenroseConnection connection = penrose.openConnection();

            int rc = connection.modify(dn.toString(), modifications);

            connection.close();

            if (rc != LDAPException.SUCCESS) {
                throw new NamingException("RC: "+rc);
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new NamingException(e.getMessage());
        }
    }

    public NamingEnumeration list(Name dn) throws NamingException {
        Logger log = Logger.getLogger(PenrosePartition.class);
        log.info("Listing \""+dn+"\"");

        try {
            PenroseConnection connection = penrose.openConnection();

            String baseDn = dn.toString();
            SearchResults results = connection.search(
                    baseDn,
                    LDAPConnection.SCOPE_ONE,
                    LDAPSearchConstraints.DEREF_ALWAYS,
                    "(objectClass=*)",
                    new ArrayList());

            int rc = results.getReturnCode();
            connection.close();

            if (rc != LDAPException.SUCCESS) {
                throw new NamingException("RC: "+rc);
            }

            List list = new ArrayList();

            for (Iterator i=results.iterator(); i.hasNext(); ) {
                LDAPEntry result = (LDAPEntry)i.next();
                log.debug("-> "+result.getDN());

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

                javax.naming.directory.SearchResult sr = new javax.naming.directory.SearchResult(
                        result.getDN(),
                        result,
                        attributes
                );

                list.add(sr);
            }

            return new PenroseEnumeration(list);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new NamingException(e.getMessage());
        }
    }

    public NamingEnumeration search(Name base, Map env, ExprNode filter, SearchControls searchControls) throws NamingException {

        Logger log = Logger.getLogger(PenrosePartition.class);
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
            PenroseConnection connection = penrose.openConnection();

            String baseDn = base.toString();
            SearchResults results = connection.search(
                    baseDn,
                    scope,
                    LDAPSearchConstraints.DEREF_ALWAYS,
                    newFilter,
                    attributeNames);

            int rc = results.getReturnCode();
            connection.close();

            if (rc != LDAPException.SUCCESS) return null;
            //throwNamingException(rc, baseDn);

            List list = new ArrayList();

            for (Iterator i=results.iterator(); i.hasNext(); ) {
                LDAPEntry result = (LDAPEntry)i.next();
                log.debug("-> "+result.getDN());

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

                javax.naming.directory.SearchResult sr = new javax.naming.directory.SearchResult(
                        result.getDN(),
                        result,
                        attributes
                );

                list.add(sr);
            }

            return new PenroseEnumeration(list);

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
        Logger log = Logger.getLogger(PenrosePartition.class);
        log.debug("Looking up \""+dn+"\"");

        try {
            PenroseConnection connection = penrose.openConnection();

            String baseDn = dn.toString();
            SearchResults results = connection.search(
                    baseDn,
                    LDAPConnection.SCOPE_BASE,
                    LDAPSearchConstraints.DEREF_ALWAYS,
                    "(objectClass=*)",
                    new ArrayList());

            int rc = results.getReturnCode();
            connection.close();

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
        Logger log = Logger.getLogger(PenrosePartition.class);
        log.debug("Looking up \""+dn+"\", \""+attrIds+"\"");

        for (int i=0; attrIds != null && i<attrIds.length; i++) {
            log.debug("- Attribute: "+attrIds[i]);
        }

        return null;
    }

    public boolean hasEntry(Name name) throws NamingException {
        Logger log = Logger.getLogger(PenrosePartition.class);
        log.info("Checking \""+name+"\"");

        try {
            PenroseConnection connection = penrose.openConnection();

            String base = name.toString();
            SearchResults results = connection.search(
                    base,
                    LDAPConnection.SCOPE_BASE,
                    LDAPSearchConstraints.DEREF_ALWAYS,
                    "(objectClass=*)", new ArrayList());

            boolean result = results.getReturnCode() == LDAPException.SUCCESS && results.size() == 1;

            connection.close();

            return result;

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new NamingException(e.getMessage());
        }
    }

    public void modifyRn(Name name, String newRn, boolean deleteOldRn) throws NamingException {
        Logger log = Logger.getLogger(PenrosePartition.class);
        log.info("Renaming \""+name+"\"");
    }

    public void move(Name oriChildName, Name newParentName) throws NamingException {
        Logger log = Logger.getLogger(PenrosePartition.class);
        log.info("Moving \""+oriChildName+"\" to \""+newParentName+"\"");
    }

    public void move(Name oriChildName, Name newParentName, String newRn, boolean deleteOldRn) throws NamingException {
        Logger log = Logger.getLogger(PenrosePartition.class);
        log.info("Moving \""+oriChildName+"\" to \""+newParentName+"\"");
    }

    public void sync() throws NamingException {
        //log.info("sync();");
    }

    public void close() throws NamingException {
        Logger log = Logger.getLogger(PenrosePartition.class);
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
