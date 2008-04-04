package org.safehaus.penrose.ldap;

/**
 * @author Endi Sukma Dewata
 */
public class SearchReferenceException extends Exception {

    private SearchResult reference;

    public SearchReferenceException(SearchResult reference) {
        this.reference = reference;
    }

    public SearchResult getReference() {
        return reference;
    }

    public void setReference(SearchResult reference) {
        this.reference = reference;
    }
}
