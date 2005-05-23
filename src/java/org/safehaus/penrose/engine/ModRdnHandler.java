/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.engine;

import org.safehaus.penrose.PenroseConnection;

/**
 * @author Endi S. Dewata
 */
public interface ModRdnHandler {

    public void init(Engine engine, EngineContext engineContext) throws Exception;
    public int modrdn(PenroseConnection connection, String dn, String newRdn) throws Exception;

}
