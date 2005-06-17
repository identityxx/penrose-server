/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.cache;

import org.safehaus.penrose.config.Config;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.engine.TransformEngine;

/**
 * @author Endi S. Dewata
 */
public interface SourceCacheContext {

    public Config getConfig() throws Exception;
    public Interpreter newInterpreter() throws Exception;
    public TransformEngine getTransformEngine() throws Exception;
}
