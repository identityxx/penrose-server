/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.handler;

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
import org.safehaus.penrose.engine.TransformEngine;
import org.safehaus.penrose.engine.Engine;
import org.safehaus.penrose.acl.ACLEngine;

/**
 * @author Endi S. Dewata
 */
public interface HandlerContext {

    public String getRootDn() throws Exception;
    public String getRootPassword() throws Exception;

    public Cache getCache() throws Exception;

    public ACLEngine getACLEngine() throws Exception;
    public Engine getEngine() throws Exception;
    public Schema getSchema() throws Exception;
    public FilterTool getFilterTool() throws Exception;
    public Interpreter newInterpreter() throws Exception;
    public Config getConfig(String dn) throws Exception;
    public TransformEngine getTransformEngine() throws Exception;
    public SyncService getSyncService() throws Exception;
    public Connection getConnection(String connectionName) throws Exception;
    public Graph getGraph(EntryDefinition entryDefinition) throws Exception;
    public Source getPrimarySource(EntryDefinition entryDefinition) throws Exception;
}
