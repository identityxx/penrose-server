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
package org.safehaus.penrose.management;

import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.ldap.LDAP;
import org.safehaus.penrose.session.Session;
import org.ietf.ldap.LDAPException;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import javax.management.remote.JMXAuthenticator;
import javax.security.auth.Subject;
import java.security.Principal;

/**
 * @author Endi S. Dewata
 */
public class PenroseJMXAuthenticator implements JMXAuthenticator {

    public Logger log = LoggerFactory.getLogger(getClass());

    private Penrose penrose;

    public PenroseJMXAuthenticator(Penrose penrose) {
        this.penrose = penrose;
    }

    public Subject authenticate(Object o) throws SecurityException {

        if (!(o instanceof String[])) {
            throw new SecurityException("Invalid object: "+o+" ("+(o == null ? null : o.getClass())+")");
        }

        String s[] = (String[])o;

        //log.debug("Authenticating:");
        //for (int i=0; i<s.length; i++) {
            //log.debug(" - "+s[i]);
        //}

        final String bindDn = s[0];
        final String bindPassword = s[1];

        log.debug("Authenticating "+bindDn);

        try {
            Session session = null;
            int rc = LDAP.SUCCESS;
            try {
                session = penrose.newSession();
                if (session == null) throw new SecurityException("Unable to create session.");

                session.bind(bindDn, bindPassword);

            } catch (LDAPException e) {
                throw new SecurityException("Authentication failed");

            } finally {
                session.close();
            }

            if (rc != LDAP.SUCCESS) throw new SecurityException("Authentication failed");

            Subject subject = new Subject();
            Principal principal = new Principal() {
                public String getName() {
                    return bindDn;
                }
            };

            subject.getPrincipals().add(principal);

            return subject;

        } catch (Exception e) {
            throw new SecurityException("An error has occured: "+e.getMessage());
        }
    }

    public Penrose getPenrose() {
        return penrose;
    }

    public void setPenrose(Penrose penrose) {
        this.penrose = penrose;
    }
}
