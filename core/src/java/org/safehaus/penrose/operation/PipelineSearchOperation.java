package org.safehaus.penrose.operation;

import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.filter.Filter;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class PipelineSearchOperation extends PipelineOperation implements SearchOperation {

    protected SearchOperation searchOperation;

    public PipelineSearchOperation(SearchOperation searchOperation) {
        super(searchOperation);
        this.searchOperation = searchOperation;
    }

    public SearchRequest getSearchRequest() {
        return searchOperation.getSearchRequest();
    }

    public void setSearchRequest(SearchRequest searchRequest) {
        searchOperation.setSearchRequest(searchRequest);
    }

    public SearchResponse getSearchResponse() {
        return searchOperation.getSearchResponse();
    }

    public void setSearchResponse(SearchResponse searchResponse ) {
        searchOperation.setSearchResponse(searchResponse);
    }

    public DN getDn() {
        return searchOperation.getDn();
    }

    public void setDn(DN dn) {
        searchOperation.setDn(dn);
    }

    public int getScope() {
        return searchOperation.getScope();
    }

    public void setScope(int scope) {
        searchOperation.setScope(scope);
    }

    public Filter getFilter() {
        return searchOperation.getFilter();
    }

    public void setFilter(Filter filter) {
        searchOperation.setFilter(filter);
    }

    public Collection<String> getAttributes() {
        return searchOperation.getAttributes();
    }

    public void setAttributes(Collection<String> attributes) {
        searchOperation.setAttributes(attributes);
    }

    public long getSizeLimit() {
        return searchOperation.getSizeLimit();
    }

    public void setSizeLimit(long sizeLimit) {
        searchOperation.setSizeLimit(sizeLimit);
    }

    public void setBufferSize(long bufferSize) {
        searchOperation.setBufferSize(bufferSize);
    }

    public long getBufferSize() {
        return searchOperation.getBufferSize();
    }

    public void add(SearchResult result) throws Exception {
        searchOperation.add(result);
    }

    public void add(SearchReference reference) throws Exception {
        searchOperation.add(reference);
    }

    public void close() throws Exception {
        searchOperation.close();
    }

    public boolean isClosed() {
        return searchOperation.isClosed();
    }

    public long getTotalCount() {
        return searchOperation.getTotalCount();
    }
}