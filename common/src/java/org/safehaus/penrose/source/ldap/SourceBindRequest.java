package org.safehaus.penrose.source.ldap;

import org.safehaus.penrose.ldap.BindRequest;
import org.safehaus.penrose.ldap.SearchResult;

import java.util.Arrays;

/**
 * @author Endi Sukma Dewata
 */
public class SourceBindRequest extends BindRequest {

    protected SearchResult searchResult;

    public SourceBindRequest() {
    }

    public SourceBindRequest(BindRequest request) throws Exception {
        super(request);
    }

    public SourceBindRequest(SourceBindRequest request) throws Exception {
        super(request);
        copy(request);
    }

    public SearchResult getSearchResult() {
        return searchResult;
    }

    public void setSearchResult(SearchResult searchResult) {
        this.searchResult = searchResult;
    }

    private boolean equals(Object o1, Object o2) {
        if (o1 == null && o2 == null) return true;
        if (o1 != null) return o1.equals(o2);
        return o2.equals(o1);
    }

    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null) return false;
        if (object.getClass() != this.getClass()) return false;

        SourceBindRequest request = (SourceBindRequest)object;
        if (!equals(dn, request.dn)) return false;
        if (!Arrays.equals(password, request.password)) return false;
        if (!equals(searchResult, request.searchResult)) return false;
        if (!equals(controls, request.controls)) return false;

        return true;
    }

    public void copy(SourceBindRequest request) throws CloneNotSupportedException {
        searchResult = (SearchResult)request.searchResult.clone();
    }

    public Object clone() throws CloneNotSupportedException {
        SourceBindRequest request = (SourceBindRequest)super.clone();
        request.copy(this);
        return request;
    }
}
