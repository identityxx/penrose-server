/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.engine;

import org.safehaus.penrose.PenroseConnection;
import org.ietf.ldap.LDAPEntry;

/**
 * @author Endi S. Dewata
 */
public interface AddHandler {

    public void init(Engine engine, EngineContext engineContext) throws Exception;
    public int add(PenroseConnection connection, LDAPEntry entry) throws Exception;

}
