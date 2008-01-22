package org.safehaus.penrose.jdbc;

import java.util.LinkedList;

/**
 * @author Endi S. Dewata
 */
public class QueryResponse extends Response {

    protected LinkedList<Object> results = new LinkedList<Object>();
    protected long sizeLimit;
    protected long totalCount;

    protected boolean closed = false;

    public void copy(QueryResponse response) {
        results = new LinkedList<Object>();
        results.addAll(response.results);

        sizeLimit = response.sizeLimit;
        totalCount = response.totalCount;

        closed = response.closed;
    }

    public void add(Object object) throws Exception {
        results.add(object);
    }

    public void close() throws Exception {
        closed = true;
    }

    public boolean hasNext() {
        return !results.isEmpty();
    }

    public Object next() {
        return results.removeFirst();
    }

    public long getSizeLimit() {
        return sizeLimit;
    }

    public void setSizeLimit(long sizeLimit) {
        this.sizeLimit = sizeLimit;
    }

    public long getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(long totalCount) {
        this.totalCount = totalCount;
    }

    public boolean isClosed() {
        return closed;
    }
}
