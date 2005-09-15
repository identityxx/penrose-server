/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 * All rights reserved.
 */
package org.safehaus.penrose.engine;

import org.safehaus.penrose.config.Config;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.schema.Schema;
import org.safehaus.penrose.cache.Cache;
import org.safehaus.penrose.connection.Connection;
import org.safehaus.penrose.sync.SyncService;
import org.safehaus.penrose.mapping.Source;
import org.safehaus.penrose.mapping.EntryDefinition;
import org.safehaus.penrose.graph.Graph;

/**
 * @author Endi S. Dewata
 */
public interface EngineContext {

    public String getRootDn() throws Exception;
    public String getRootPassword() throws Exception;

    public Cache getCache() throws Exception;

    public Schema getSchema() throws Exception;
    public FilterTool getFilterTool() throws Exception;
    public Interpreter newInterpreter() throws Exception;
    public Config getConfig(String dn) throws Exception;
    public TransformEngine getTransformEngine() throws Exception;
    public SyncService getSyncService() throws Exception;
    public Connection getConnection(String connectionName) throws Exception;
}
