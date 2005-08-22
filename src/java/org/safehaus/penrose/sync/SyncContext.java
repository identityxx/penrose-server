/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.sync;

import org.safehaus.penrose.engine.TransformEngine;
import org.safehaus.penrose.connection.Connection;
import org.safehaus.penrose.cache.Cache;
import org.safehaus.penrose.schema.Schema;

/**
 * @author Endi S. Dewata
 */
public interface SyncContext {

    public Cache getCache() throws Exception;
    public Schema getSchema() throws Exception;
    public TransformEngine getTransformEngine() throws Exception;
    public Connection getConnection(String connectionName) throws Exception;
}
