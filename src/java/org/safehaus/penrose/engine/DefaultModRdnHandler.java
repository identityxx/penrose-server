/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.engine;

import org.ietf.ldap.*;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.PenroseConnection;
import org.apache.log4j.Logger;


/**
 * @author Endi S. Dewata
 */
public class DefaultModRdnHandler extends ModRdnHandler {

    public Logger log = Logger.getLogger(Penrose.MODRDN_LOGGER);

    public DefaultEngine engine;

	public void init(Engine engine) throws Exception {
        this.engine = (DefaultEngine)engine;
	}

	public int modrdn(PenroseConnection connection, String dn, String newRdn)
			throws Exception {

		return LDAPException.LDAP_NOT_SUPPORTED;
	}
}