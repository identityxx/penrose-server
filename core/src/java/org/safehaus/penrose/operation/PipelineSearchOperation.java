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

    public Filter getFilter() {
        return searchOperation.getFilter();
    }

    public void setFilter(Filter filter) {
        searchOperation.setFilter(filter);
    }

    public int getScope() {
        return searchOperation.getScope();
    }

    public void setScope(int scope) {
        searchOperation.setScope(scope);
    }

    public int getDereference() {
        return searchOperation.getDereference();
    }

    public void setDereference(int dereference) {
        searchOperation.setDereference(dereference);
    }

    public boolean isTypesOnly() {
        return searchOperation.isTypesOnly();
    }

    public void setTypesOnly(boolean typesOnly) {
        searchOperation.setTypesOnly(typesOnly);
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

    public long getTimeLimit() {
        return searchOperation.getTimeLimit();
    }

    public void setTimeLimit(long timeLimit) {
        searchOperation.setTimeLimit(timeLimit);
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

    public long getCreateTimestamp() {
        return searchOperation.getCreateTimestamp();
    }

    public long getCloseTimestamp() {
        return searchOperation.getCloseTimestamp();
    }
}