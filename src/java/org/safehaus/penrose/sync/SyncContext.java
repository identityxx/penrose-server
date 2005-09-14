/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 * All rights reserved.
 */
package org.safehaus.penrose.sync;

import org.safehaus.penrose.engine.TransformEngine;
import org.safehaus.penrose.connection.Connection;
import org.safehaus.penrose.cache.Cache;
import org.safehaus.penrose.schema.Schema;
import org.safehaus.penrose.mapping.Source;
import org.safehaus.penrose.config.Config;

/**
 * @author Endi S. Dewata
 */
public interface SyncContext {

    public Config getConfig(Source source) throws Exception;
    public Cache getCache() throws Exception;
    public Schema getSchema() throws Exception;
    public TransformEngine getTransformEngine() throws Exception;
    public Connection getConnection(String connectionName) throws Exception;
}
