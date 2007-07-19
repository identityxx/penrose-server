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
package org.safehaus.penrose.apacheds;

import org.apache.directory.server.core.authn.AbstractAuthenticator;
import org.apache.directory.server.core.authn.LdapPrincipal;
import org.apache.directory.server.core.jndi.ServerContext;
import org.apache.directory.shared.ldap.exception.LdapAuthenticationException;
import org.apache.directory.shared.ldap.aci.AuthenticationLevel;
import org.apache.directory.shared.ldap.name.LdapDN;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.ietf.ldap.LDAPException;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.ServiceUnavailableException;

import com.identyx.javabackend.*;

/**
 * @author Endi S. Dewata
 */
public class PenroseAuthenticator extends AbstractAuthenticator {

    public Logger log = LoggerFactory.getLogger(getClass());

    Backend backend;

    public PenroseAuthenticator() {
        super("simple");
    }

    public Backend getBackend() {
        return backend;
    }

    public void setBackend(Backend backend) {
        this.backend = backend;
    }

    public void init() throws NamingException {
    }

    public LdapPrincipal authenticate(LdapDN name, ServerContext ctx) throws NamingException {

        String dn = name.getUpName();
        byte[] password = (byte[])ctx.getEnvironment().get(Context.SECURITY_CREDENTIALS);

        if (dn == null || "".equals(dn)) {
            throw new LdapAuthenticationException();
        }

        try {
            Session session = backend.getSession(dn);

            if (session == null) {
                session = backend.createSession(dn);
                if (session == null) throw new ServiceUnavailableException();
            }

            DN bindDn = backend.createDn(dn);

            session.bind(bindDn, password);
            log.warn("Bind operation succeeded.");

            return createLdapPrincipal(dn, AuthenticationLevel.SIMPLE);

        } catch (NamingException e) {
            log.warn("Bind operation failed: "+e.getMessage());
            throw e;

        } catch (LDAPException e) {
            log.warn("Bind operation failed: "+e.getMessage());
            throw ExceptionTool.createNamingException(e);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionTool.createNamingException(e);
        }
    }
}
