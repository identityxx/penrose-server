/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.engine;

import org.safehaus.penrose.PenroseConnection;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.util.PasswordUtil;
import org.ietf.ldap.LDAPDN;
import org.ietf.ldap.LDAPException;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class BindHandler {

    public Logger log = Logger.getLogger(Penrose.BIND_LOGGER);

    private Engine engine;
    private EngineContext engineContext;

    public void init(Engine engine) throws Exception {
        this.engine = engine;
        this.engineContext = engine.getEngineContext();
    }
    public int bind(PenroseConnection connection, String dn, String password) throws Exception {

        String ndn = LDAPDN.normalize(dn);

        if (ndn.equals(LDAPDN.normalize(engineContext.getRootDn()))) { // bind as root

            int rc = bindAsRoot(password);
            if (rc != LDAPException.SUCCESS) return rc;

        } else {

            int rc = bindAsUser(connection, ndn, password);
            if (rc != LDAPException.SUCCESS) return rc;
        }

        connection.setBindDn(dn);
        return LDAPException.SUCCESS; // LDAP_SUCCESS
    }

    public int unbind(PenroseConnection connection) throws Exception {

        log.debug("-------------------------------------------------------------------------------");
        log.debug("UNBIND:");

        connection.setBindDn(null);

        log.debug("  dn: " + connection.getBindDn());

        return 0;
    }

    public int bindAsRoot(String password) throws Exception {
        log.debug("Comparing root's password");

        if (!PasswordUtil.comparePassword(password, engineContext.getRootPassword())) {
            log.debug("Password doesn't match => BIND FAILED");
            return LDAPException.INVALID_CREDENTIALS;
        }

        return LDAPException.SUCCESS;
    }

    public int bindAsUser(PenroseConnection connection, String dn, String password) throws Exception {
        log.debug("Searching for "+dn);

        List attributeNames = new ArrayList();
        attributeNames.add("userPassword");
        Entry sr = null;
        try {
            sr = engine.getSearchHandler().find(connection, dn);
        } catch (Exception e) {
            // ignore
        }

        //if (sr == null) return LDAPException.NO_SUCH_OBJECT;
        if (sr == null) {
            log.debug("Entry "+dn+" not found => BIND FAILED");
            return LDAPException.INVALID_CREDENTIALS;
        }

        log.debug("Found "+sr.getDn());

        return bindAsUser(connection, sr, password);
    }

    public int bindAsUser(PenroseConnection connection, Entry sr, String password) throws Exception {

        log.debug("Bind as user "+sr.getDn());

        EntryDefinition entry = sr.getEntryDefinition();
        AttributeValues attributes = sr.getAttributeValues();

        Collection set = attributes.get("userPassword");

        if (set != null && !set.isEmpty()) {
            log.debug("Entry has userPassword");
            return bindAsStaticUser(sr, password);
        }

        return bind(entry, attributes, password);
    }

    public int bindAsStaticUser(Entry sr, String cred) throws Exception {
        EntryDefinition entry = sr.getEntryDefinition();
        AttributeValues values = sr.getAttributeValues();

        Map attributes = entry.getAttributes();
        AttributeDefinition attribute = (AttributeDefinition)attributes.get("userPassword");
        String encryption = attribute.getEncryption();
        String encoding = attribute.getEncoding();

        Collection set = values.get("userPassword");

        for (Iterator i = set.iterator(); i.hasNext(); ) {
            String userPassword = (String)i.next();
            log.debug("userPassword: "+userPassword);
            if (PasswordUtil.comparePassword(cred, encryption, encoding, userPassword)) return LDAPException.SUCCESS;
        }

        return LDAPException.INVALID_CREDENTIALS;
    }

    public int bind(EntryDefinition entry, AttributeValues values, String password) throws Exception {

        Date date = new Date();

        Collection sources = entry.getSources();

        for (Iterator i=sources.iterator(); i.hasNext(); ) {
            Source source = (Source)i.next();

            int rc = getEngineContext().getSyncService().bind(source, entry, values, password, date);

            if (rc == LDAPException.SUCCESS) return rc;
        }

        return LDAPException.INVALID_CREDENTIALS;
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
}
