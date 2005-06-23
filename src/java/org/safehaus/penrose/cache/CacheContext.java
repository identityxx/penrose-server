/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.cache;

import org.safehaus.penrose.config.Config;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.engine.TransformEngine;
import org.safehaus.penrose.filter.FilterTool;

/**
 * @author Endi S. Dewata
 */
public interface CacheContext {

    public Config getConfig() throws Exception;
    public Interpreter newInterpreter() throws Exception;
    public TransformEngine getTransformEngine() throws Exception;
    public FilterTool getFilterTool() throws Exception;
}
