/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.apacheds;

import org.apache.ldap.server.authn.AbstractAuthenticator;
import org.apache.ldap.server.authn.LdapPrincipal;
import org.apache.ldap.server.jndi.ServerContext;
import org.apache.ldap.common.exception.LdapAuthenticationException;
import org.apache.log4j.Logger;
import org.ietf.ldap.LDAPException;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.PenroseConnection;

import javax.naming.Context;
import javax.naming.NamingException;

/**
 * @author Endi S. Dewata
 */
public class PenroseAuthenticator extends AbstractAuthenticator {

    public Penrose penrose;

    public PenroseAuthenticator( )
    {
        super( "simple" );
    }

    public void init() throws NamingException {
    }

    public void setPenrose(Penrose penrose) {
        this.penrose = penrose;
    }

    public LdapPrincipal authenticate( ServerContext ctx ) throws NamingException {
        Logger log = Logger.getLogger(PenroseAuthenticator.class);

        try {
            String dn = ( String ) ctx.getEnvironment().get( Context.SECURITY_PRINCIPAL );
            Object credentials = ctx.getEnvironment().get( Context.SECURITY_CREDENTIALS );

            String password = new String((byte[])credentials);
            log.info("Login "+dn);

            PenroseConnection connection = penrose.openConnection();

            int rc = connection.bind(dn.toString(), password);
            connection.close();
            if (rc != LDAPException.SUCCESS) {
                log.info("Login failed.");
                throw new LdapAuthenticationException();
            }

            log.info("Login success.");

            return createLdapPrincipal( dn );

        } catch (NamingException e) {
            log.info("Login failed: "+e.getMessage());
            throw new LdapAuthenticationException();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new NamingException(e.getMessage());
        }
    }
}
