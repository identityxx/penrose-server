/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.engine;

import org.safehaus.penrose.PenroseConnection;

/**
 * @author Endi S. Dewata
 */
public abstract class DeleteHandler {

    public abstract void init(Engine engine) throws Exception;
    public abstract int delete(PenroseConnection connection, String dn) throws Exception;

}
