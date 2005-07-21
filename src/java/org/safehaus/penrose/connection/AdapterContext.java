package org.safehaus.penrose.connection;

import org.safehaus.penrose.engine.TransformEngine;
import org.safehaus.penrose.config.Config;

/**
 * @author Endi S. Dewata
 */
public interface AdapterContext {

    public Config getConfig() throws Exception;
    public TransformEngine getTransformEngine() throws Exception;
}
