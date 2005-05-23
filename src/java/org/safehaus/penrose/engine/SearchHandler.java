/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.engine;

import org.safehaus.penrose.SearchResults;
import org.safehaus.penrose.PenroseConnection;

import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public interface SearchHandler {

    public void init(Engine engine, EngineContext engineContext) throws Exception;
    
    public int search(
            PenroseConnection connection,
            String base,
            int scope,
            int deref,
            String filter,
            Collection attributeNames,
            SearchResults results) throws Exception;
}
