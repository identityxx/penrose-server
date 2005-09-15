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
package org.safehaus.penrose.apacheds;

import org.apache.ldap.server.authn.AbstractAuthenticator;
import org.apache.ldap.server.authn.LdapPrincipal;
import org.apache.ldap.server.jndi.ServerContext;
import org.apache.ldap.common.exception.LdapAuthenticationException;
import org.slf4j.Logger;
import org.ietf.ldap.LDAPException;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.PenroseConnection;
import org.slf4j.LoggerFactory;

import javax.naming.Context;
import javax.naming.NamingException;

/**
 * @author Endi S. Dewata
 */
public class PenroseAuthenticator extends AbstractAuthenticator {

    Penrose penrose;

    public PenroseAuthenticator( )
    {
        super( "simple" );
    }

    public void init() throws NamingException {
    }

    public void setPenrose(Penrose penrose) throws Exception {
        this.penrose = penrose;
    }

    public LdapPrincipal authenticate( ServerContext ctx ) throws NamingException {
        Logger log = LoggerFactory.getLogger(getClass());

        String dn = ( String ) ctx.getEnvironment().get( Context.SECURITY_PRINCIPAL );

        Object credentials = ctx.getEnvironment().get( Context.SECURITY_CREDENTIALS );
        String password = new String((byte[])credentials);

        String rootDn = penrose.getRootDn();
        String rootPassword = penrose.getRootPassword();

        if (rootDn != null && rootPassword != null &&
                rootDn.equals(dn) && rootPassword.equals(password)) {
            return createLdapPrincipal( dn );
        }

        if ("".equals(dn)) {
            return createLdapPrincipal( dn );
        }
        
        log.info("Login "+dn);

        try {
            PenroseConnection connection = penrose.openConnection();
            int rc = connection.bind(dn.toString(), password);
            connection.close();

            if (rc != LDAPException.SUCCESS) {
                throw new LdapAuthenticationException();
            }

            log.info("Login success.");

            return createLdapPrincipal( dn );

        } catch (NamingException e) {
            log.info("Login failed.");
            throw e;

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new NamingException(e.getMessage());
        }
    }
}
