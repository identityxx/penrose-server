/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.handler;

import org.safehaus.penrose.PenroseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ietf.ldap.LDAPException;

/**
 * @author Endi S. Dewata
 */
public class ModRdnHandler {

    Logger log = LoggerFactory.getLogger(getClass());

    public Handler handler;

	public ModRdnHandler(Handler handler) throws Exception {
        this.handler = handler;
	}

	public int modrdn(PenroseConnection connection, String dn, String newRdn)
			throws Exception {

		return LDAPException.LDAP_NOT_SUPPORTED;
	}
}
