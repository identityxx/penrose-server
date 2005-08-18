package org.safehaus.penrose.connection;

import org.safehaus.penrose.engine.TransformEngine;

/**
 * @author Endi S. Dewata
 */
public interface AdapterContext {

    public TransformEngine getTransformEngine() throws Exception;
}
