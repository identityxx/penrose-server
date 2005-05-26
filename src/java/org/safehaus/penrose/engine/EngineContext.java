/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.engine;

import org.safehaus.penrose.config.Config;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.schema.Schema;
import org.safehaus.penrose.cache.Cache;

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
    public Config getConfig() throws Exception;
    public TransformEngine getTransformEngine() throws Exception;
}
