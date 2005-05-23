/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.engine;

import org.safehaus.penrose.PenroseConnection;

/**
 * @author Endi S. Dewata
 */
public interface DeleteHandler {

    public void init(Engine engine, EngineContext engineContext) throws Exception;
    public int delete(PenroseConnection connection, String dn) throws Exception;

}
