package org.safehaus.penrose.backend;

import com.identyx.javabackend.SearchReferenceException;
import com.identyx.javabackend.SearchResult;

/**
 * @author Endi Sukma Dewata
 */
public class PenroseSearchReferenceException extends SearchReferenceException {

    private PenroseSearchResult reference;

    public PenroseSearchReferenceException(PenroseSearchResult reference) {
        this.reference = reference;
    }
    
    public SearchResult getReference() {
         return reference;
    }

    public void setReference(PenroseSearchResult reference) {
        this.reference = reference;
    }
}
