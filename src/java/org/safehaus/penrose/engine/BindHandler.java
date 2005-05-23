/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.engine;

import org.safehaus.penrose.PenroseConnection;

/**
 * @author Endi S. Dewata
 */
public interface BindHandler {

    public void init(Engine engine, EngineContext engineContext) throws Exception;
    public int bind(PenroseConnection connection, String dn, String password) throws Exception;
    public int unbind(PenroseConnection connection) throws Exception;

}
