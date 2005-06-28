/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.engine;

import org.safehaus.penrose.SearchResults;
import org.safehaus.penrose.PenroseConnection;
import org.safehaus.penrose.mapping.Entry;

import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public interface SearchHandler {

    public void init(Engine engine, EngineContext engineContext) throws Exception;

    public Entry findEntry(
            PenroseConnection connection,
            String dn) throws Exception;

    public int search(
            PenroseConnection connection,
            String base,
            int scope,
            int deref,
            String filter,
            Collection attributeNames,
            SearchResults results) throws Exception;
}
