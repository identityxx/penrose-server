package org.safehaus.penrose.jdbc;

import java.util.LinkedList;

/**
 * @author Endi S. Dewata
 */
public class QueryResponse extends Response {

    LinkedList results = new LinkedList();

    public void add(Object object) throws Exception {
        results.add(object);
    }

    public void close() throws Exception {
    }

    public boolean hasNext() {
        return !results.isEmpty();
    }

    public Object next() {
        return results.removeFirst();
    }
}
