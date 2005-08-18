/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.engine;

import org.safehaus.penrose.PenroseConnection;
import org.safehaus.penrose.Penrose;
import org.apache.log4j.Logger;
import org.ietf.ldap.LDAPException;

/**
 * @author Endi S. Dewata
 */
public class ModRdnHandler {

    public Logger log = Logger.getLogger(Penrose.MODRDN_LOGGER);

    public Engine engine;

	public void init(Engine engine) throws Exception {
        this.engine = engine;
	}

	public int modrdn(PenroseConnection connection, String dn, String newRdn)
			throws Exception {

		return LDAPException.LDAP_NOT_SUPPORTED;
	}
}
