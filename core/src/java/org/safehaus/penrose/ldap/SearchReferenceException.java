package org.safehaus.penrose.ldap;

/**
 * @author Endi Sukma Dewata
 */
public class SearchReferenceException extends Exception {

    private SearchReference reference;

    public SearchReferenceException(SearchReference reference) {
        this.reference = reference;
    }

    public SearchReference getReference() {
        return reference;
    }

    public void setReference(SearchReference reference) {
        this.reference = reference;
    }
}
