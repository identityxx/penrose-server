/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.engine;

import org.safehaus.penrose.PenroseConnection;

import java.util.List;

/**
 * @author Endi S. Dewata
 */
public interface ModifyHandler {

    public void init(Engine engine, EngineContext engineContext) throws Exception;
    public int modify(PenroseConnection connection, String dn, List modifications) throws Exception;

}
