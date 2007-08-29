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
    boolean debug = log.isDebugEnabled();

    private Penrose penrose;

    public PenroseJMXAuthenticator(Penrose penrose) {
        this.penrose = penrose;
    }

    public Subject authenticate(Object credentials) {

        if (credentials == null) {
            if (debug) log.debug("Credential is null.");
            throw new SecurityException("Authentication failed.");
        }
        
        if (!(credentials instanceof String[])) {
            if (debug) log.debug("Invalid credentials: "+credentials+" ("+credentials.getClass()+")");
            throw new SecurityException("Invalid credentials: "+credentials+" ("+credentials.getClass()+")");
        }

        String s[] = (String[])credentials;
        final String bindDn = s[0];
        final String bindPassword = s[1];

        if (debug) log.debug("Authenticating "+bindDn+".");

        Session session;
        try {
            session = penrose.newSession();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException("Error: "+e.getMessage());
        }

        if (session == null) throw new RuntimeException("Unable to create session.");

        try {
            session.bind(bindDn, bindPassword);

        } catch (LDAPException e) {
            throw new SecurityException("Authentication failed.");

        } finally {
            session.close();
        }

        Subject subject = new Subject();
        Principal principal = new Principal() {
            public String getName() {
                return bindDn;
            }
        };

        subject.getPrincipals().add(principal);

        return subject;
    }

    public Penrose getPenrose() {
        return penrose;
    }

    public void setPenrose(Penrose penrose) {
        this.penrose = penrose;
    }
}
