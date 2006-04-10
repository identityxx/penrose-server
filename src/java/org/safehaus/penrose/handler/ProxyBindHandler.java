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
import org.safehaus.penrose.event.BindEvent;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.util.PasswordUtil;
import org.safehaus.penrose.util.Formatter;
import org.ietf.ldap.LDAPDN;
import org.ietf.ldap.LDAPException;
import org.apache.log4j.Logger;

import javax.naming.Context;
import javax.naming.AuthenticationException;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import java.util.Hashtable;

/**
 * @author Endi S. Dewata
 */
public class ProxyBindHandler extends BindHandler {

    public ProxyBindHandler(Handler handler) {
        super(handler);
    }

    public int bind(PenroseSession session, String dn, String password) throws Exception {

        Hashtable env = new Hashtable();
        //env.put(Context.INITIAL_CONTEXT_FACTORY, getParameter(Context.INITIAL_CONTEXT_FACTORY));
        //env.put(Context.PROVIDER_URL, client.getUrl());
        env.put(Context.SECURITY_PRINCIPAL, dn);
        env.put(Context.SECURITY_CREDENTIALS, password);

        try {
            DirContext c = new InitialDirContext(env);
            c.close();
        } catch (AuthenticationException e) {
            log.debug("Error: "+e.getMessage());
            return LDAPException.INVALID_CREDENTIALS;
        }

        session.setBindDn(dn);
        
        return LDAPException.SUCCESS; // LDAP_SUCCESS
    }

    public int unbind(PenroseSession session) throws Exception {

        log.debug("-------------------------------------------------------------------------------");
        log.debug("UNBIND:");

        if (session == null) return 0;

        session.setBindDn(null);

        log.debug("  dn: " + session.getBindDn());

        return 0;
    }

    public int bindAsRoot(String password) throws Exception {
        log.debug("Comparing root's password");

        if (!PasswordUtil.comparePassword(password, getHandler().getRootUserConfig().getPassword())) {
            log.debug("Password doesn't match => BIND FAILED");
            return LDAPException.INVALID_CREDENTIALS;
        }

        return LDAPException.SUCCESS;
    }

    public int bindAsUser(PenroseSession session, String dn, String password) throws Exception {
        log.debug("Searching for "+dn);

        Entry entry = getHandler().getFindHandler().find(session, dn);
        if (entry == null) {
            log.debug("Entry "+dn+" not found => BIND FAILED");
            return LDAPException.INVALID_CREDENTIALS;
        }

        log.debug("Found "+entry.getDn());

        return getHandler().getEngine().bind(entry, password);
    }
}
