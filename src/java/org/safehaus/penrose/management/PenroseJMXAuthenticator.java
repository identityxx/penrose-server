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
package org.safehaus.penrose.management;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.Penrose;

import javax.management.remote.JMXAuthenticator;
import javax.security.auth.Subject;
import javax.naming.InitialContext;
import javax.naming.Context;
import javax.naming.directory.InitialDirContext;
import java.security.Principal;
import java.util.Properties;

/**
 * @author Endi S. Dewata
 */
public class PenroseJMXAuthenticator implements JMXAuthenticator {

    Logger log = LoggerFactory.getLogger(getClass());

    public String url;
    public String pattern;

    public PenroseJMXAuthenticator(String url, String pattern) {
        this.url = url;
        this.pattern = pattern;
        log.debug("Initializing PenroseJMXAuthenticator.");
    }

    public Subject authenticate(Object o) throws SecurityException {

        if (o instanceof String[]) {
            String s[] = (String[])o;

            //log.debug("Authenticating:");
            //for (int i=0; i<s.length; i++) {
                //log.debug(" - "+s[i]);
            //}

            final String username = s[0];
            final String password = s[1];

            String bindDn = pattern.replaceFirst("\\{0\\}", username);
            log.debug("Connecting to "+url+" as "+bindDn);

            Properties env = new Properties();
            env.put(InitialContext.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
            env.put(InitialContext.PROVIDER_URL, url);
            env.put(InitialContext.SECURITY_PRINCIPAL, bindDn);
            env.put(InitialContext.SECURITY_CREDENTIALS, password);

            try {
                Context ctx = new InitialDirContext(env);
                ctx.close();

                Subject subject = new Subject();
                Principal principal = new Principal() {
                    public String getName() {
                        return username;
                    }
                };

                subject.getPrincipals().add(principal);

                return subject;

            } catch (Exception e) {
                throw new SecurityException("Authentication failed");
            }

        } else {
            log.debug("Authenticating "+o+" ("+(o == null ? null : o.getClass())+")");
            throw new SecurityException();
        }
    }
}
