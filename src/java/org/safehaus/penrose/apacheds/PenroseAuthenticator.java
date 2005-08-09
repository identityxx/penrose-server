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

    Penrose penrose;

    public PenroseAuthenticator( )
    {
        super( "simple" );
    }

    public void init() throws NamingException {
    }

    public void setPenrose(Penrose penrose) throws Exception {
        this.penrose = penrose;
        penrose.init();
    }

    public LdapPrincipal authenticate( ServerContext ctx ) throws NamingException {
        Logger log = Logger.getLogger(PenroseAuthenticator.class);

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
