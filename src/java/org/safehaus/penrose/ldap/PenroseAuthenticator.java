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

import org.apache.directory.server.core.authn.AbstractAuthenticator;
import org.apache.directory.server.core.authn.LdapPrincipal;
import org.apache.directory.server.core.jndi.ServerContext;
import org.apache.directory.shared.ldap.exception.LdapAuthenticationException;
import org.apache.directory.shared.ldap.aci.AuthenticationLevel;
import org.apache.log4j.Logger;
import org.ietf.ldap.LDAPException;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.session.PenroseSession;
import org.safehaus.penrose.config.PenroseConfig;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.ServiceUnavailableException;

/**
 * @author Endi S. Dewata
 */
public class PenroseAuthenticator extends AbstractAuthenticator {

    Logger log = Logger.getLogger(getClass());

    Penrose penrose;

    public PenroseAuthenticator()
    {
        super("simple");
    }

    public void init() throws NamingException {
    }

    public void setPenrose(Penrose penrose) throws Exception {
        this.penrose = penrose;
    }

    public LdapPrincipal authenticate(ServerContext ctx) throws NamingException {

        String dn = (String)ctx.getEnvironment().get(Context.SECURITY_PRINCIPAL);

        Object credentials = ctx.getEnvironment().get(Context.SECURITY_CREDENTIALS);
        String password = new String((byte[])credentials);

        PenroseConfig penroseConfig = penrose.getPenroseConfig();
        String rootDn = penroseConfig.getRootUserConfig().getDn();
        String rootPassword = penroseConfig.getRootUserConfig().getPassword();

        //log.info("Login "+dn);

        if (rootDn != null &&
                rootPassword != null &&
                rootDn.equals(dn)) {

            throw new LdapAuthenticationException();
        }

        try {
            PenroseSession session = penrose.newSession();
            if (session == null) throw new ServiceUnavailableException();

            int rc = session.bind(dn.toString(), password);
            session.close();

            if (rc != LDAPException.SUCCESS) {
                throw new LdapAuthenticationException();
            }

            log.info("Login success.");

            return createLdapPrincipal(dn, AuthenticationLevel.SIMPLE);

        } catch (NamingException e) {
            log.info("Login failed.");
            throw e;

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new NamingException(e.getMessage());
        }
    }
}
