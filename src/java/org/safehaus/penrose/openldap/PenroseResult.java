/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 * All rights reserved.
 */
package org.safehaus.penrose.openldap;

import org.openldap.backend.Result;
import org.safehaus.penrose.SearchResults;

import java.util.Collection;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class PenroseResult implements Result {

    private SearchResults results;

    public PenroseResult(SearchResults results) {
        this.results = results;
    }

    public Object next() {
        return results.next();
    }

    public void close() {
        results.close();
    }

    public Collection getAll() {
        return results.getAll();
    }

    public int size() {
        return results.size();
    }

    public synchronized Iterator iterator() {
        return results.iterator();
    }

    public int getReturnCode() {
        return results.getReturnCode();
    }

    public boolean hasNext() {
        return results.hasNext();
    }
}
