/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.engine;

import org.safehaus.penrose.PenroseConnection;

/**
 * @author Endi S. Dewata
 */
public abstract class CompareHandler {

    public abstract void init(Engine engine) throws Exception;
    public abstract int compare(PenroseConnection connection, String dn, String attributeName, String attributeValue) throws Exception;

}
