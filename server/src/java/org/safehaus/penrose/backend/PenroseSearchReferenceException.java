package org.safehaus.penrose.backend;

import com.identyx.javabackend.SearchReference;
import com.identyx.javabackend.SearchReferenceException;

/**
 * @author Endi Sukma Dewata
 */
public class PenroseSearchReferenceException extends SearchReferenceException {

    private SearchReference reference;

    public PenroseSearchReferenceException(SearchReference reference) {
        this.reference = reference;
    }
    
    public SearchReference getReference() {
         return reference;
    }

    public void setReference(SearchReference reference) {
        this.reference = reference;
    }
}
