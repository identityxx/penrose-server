/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 * All rights reserved.
 */
package org.safehaus.penrose.cache;

import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.schema.Schema;

/**
 * @author Endi S. Dewata
 */
public interface CacheContext {

    public Interpreter newInterpreter() throws Exception;
    public FilterTool getFilterTool() throws Exception;
    public Schema getSchema() throws Exception;
}
