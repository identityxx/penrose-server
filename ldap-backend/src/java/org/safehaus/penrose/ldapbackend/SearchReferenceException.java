package org.safehaus.penrose.ldapbackend;

/**
 * @author Endi Sukma Dewata
 */
public abstract class SearchReferenceException extends Exception {
    public abstract SearchReference getReference();
}
