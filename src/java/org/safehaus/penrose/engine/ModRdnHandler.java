/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.engine;

import org.safehaus.penrose.PenroseConnection;
import org.safehaus.penrose.Penrose;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ietf.ldap.LDAPException;

/**
 * @author Endi S. Dewata
 */
public class ModRdnHandler {

    Logger log = LoggerFactory.getLogger(getClass());

    public Engine engine;

	public void init(Engine engine) throws Exception {
        this.engine = engine;
	}

	public int modrdn(PenroseConnection connection, String dn, String newRdn)
			throws Exception {

		return LDAPException.LDAP_NOT_SUPPORTED;
	}
}
