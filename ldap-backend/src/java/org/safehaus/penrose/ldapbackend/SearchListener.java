package org.safehaus.penrose.ldapbackend;

/**
 * @author Endi Sukma Dewata
 */
public interface SearchListener {

    public void add(SearchResult result) throws Exception;
    public void add(SearchReference reference) throws Exception;
    public void close() throws Exception;
}
