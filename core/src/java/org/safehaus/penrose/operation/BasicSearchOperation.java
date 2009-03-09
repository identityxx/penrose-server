package org.safehaus.penrose.operation;

import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.schema.SchemaManager;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.statistic.StatisticManager;
import org.ietf.ldap.LDAPException;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class BasicSearchOperation extends BasicOperation implements SearchOperation {

    protected long createTimestamp;
    protected long closeTimestamp;

    protected SearchRequest searchRequest;
    protected SearchResponse searchResponse;

    protected long bufferSize;

    protected DN bindDn;

    protected DN dn;
    protected Filter filter;

    protected int scope;
    protected int dereference;
    protected boolean typesOnly;

    protected long sizeLimit;
    protected long timeLimit;

    protected Collection<String> attributes;

    protected long totalCount;

    public BasicSearchOperation(Session session) {
        super(session);
    }

    public void setRequest(Request request) {
        setSearchRequest((SearchRequest)request);
    }

    public void setResponse(Response response) {
        setSearchResponse((SearchResponse)response);
    }

    public SearchRequest getSearchRequest() {
        return searchRequest;
    }

    public void setSearchRequest(SearchRequest searchRequest) {
        super.setRequest(searchRequest);
        this.searchRequest = searchRequest;
    }

    public SearchResponse getSearchResponse() {
        return searchResponse;
    }

    public void setSearchResponse(SearchResponse searchResponse) {
        super.setResponse(searchResponse);
        this.searchResponse = searchResponse;
    }

    public DN getDn() {
        return searchRequest.getDn();
    }

    public void setDn(DN dn) {
        searchRequest.setDn(dn);
    }

    public Filter getFilter() {
        return searchRequest.getFilter();
    }

    public void setFilter(Filter filter) {
        searchRequest.setFilter(filter);
    }

    public int getScope() {
        return searchRequest.getScope();
    }

    public void setScope(int scope) {
        searchRequest.setScope(scope);
    }

    public int getDereference() {
        return searchRequest.getDereference();
    }

    public void setDereference(int dereference) {
        searchRequest.setDereference(dereference);
    }

    public boolean isTypesOnly() {
        return searchRequest.isTypesOnly();
    }

    public void setTypesOnly(boolean typesOnly) {
        searchRequest.setTypesOnly(typesOnly);
    }

    public Collection<String> getAttributes() {
        return searchRequest.getAttributes();
    }

    public void setAttributes(Collection<String> attributes) {
        searchRequest.setAttributes(attributes);
    }

    public long getSizeLimit() {
        return searchRequest.getSizeLimit();
    }

    public void setSizeLimit(long sizeLimit) {
        searchRequest.setSizeLimit(sizeLimit);
    }

    public long getTimeLimit() {
        return searchRequest.getTimeLimit();
    }

    public void setTimeLimit(long timeLimit) {
        searchRequest.setTimeLimit(timeLimit);
    }

    public void add(SearchResult result) throws Exception {

        boolean debug = log.isDebugEnabled();
        if (sizeLimit > 0 && totalCount >= sizeLimit) {
            LDAPException exception = LDAP.createException(LDAP.SIZE_LIMIT_EXCEEDED);
            response.setException(exception);
            throw exception;
        }

        if (timeLimit > 0 && System.currentTimeMillis() - createTimestamp > timeLimit) {
            LDAPException exception = LDAP.createException(LDAP.TIME_LIMIT_EXCEEDED);
            response.setException(exception);
            throw exception;
        }

        if (abandoned) {
            if (debug) log.debug("Operation "+operationName+" has been abandoned.");
            return;
        }

        if (debug) log.debug("Result: \""+result.getDn()+"\".");

        searchResponse.add(result);

        totalCount++;
    }

    public void add(SearchReference reference) throws Exception {

        boolean debug = log.isDebugEnabled();
        if (timeLimit > 0 && System.currentTimeMillis() - createTimestamp > timeLimit) {
            LDAPException exception = LDAP.createException(LDAP.TIME_LIMIT_EXCEEDED);
            response.setException(exception);
            throw exception;
        }

        if (abandoned) {
            if (debug) log.debug("Operation "+operationName+" has been abandoned.");
            return;
        }

        if (debug) log.debug("Reference: \""+ reference.getDn()+"\".");

        searchResponse.add(reference);
    }

    public void init() throws Exception {

        createTimestamp = System.currentTimeMillis();

        bindDn      = session.getBindDn();

        dn          = searchRequest.getDn();
        filter      = searchRequest.getFilter();

        scope       = searchRequest.getScope();
        dereference = searchRequest.getDereference();
        typesOnly   = searchRequest.isTypesOnly();

        sizeLimit   = searchRequest.getSizeLimit();
        timeLimit   = searchRequest.getTimeLimit();

        attributes  = searchRequest.getAttributes();

        StatisticManager statisticManager = penrose.getStatisticManager();
        SchemaManager schemaManager = session.getPenroseContext().getSchemaManager();

        statisticManager.incrementCounter(StatisticManager.SEARCH);

        dn = schemaManager.normalize(dn);
        //searchRequest.setDn(dn);

        attributes = schemaManager.normalize(attributes);
        //searchRequest.setAttributes(attributes);

        if (searchResponse != null) searchResponse.setBufferSize(bufferSize);
    }

    public void close() throws Exception {

        boolean debug = log.isDebugEnabled();
        if (searchResponse != null && searchResponse.isClosed()) {
            if (debug) log.debug("Operation "+operationName+" already closed.");
            return;
        }

        if (debug) log.debug("Closing operation "+operationName+".");

        closeTimestamp = System.currentTimeMillis();

        if (searchResponse != null) searchResponse.close();
    }

    public boolean isClosed() {
        return searchResponse.isClosed();
    }

    public long getTotalCount() {
        return searchResponse.getTotalCount();
    }

    public long getCreateTimestamp() {
        return createTimestamp;
    }

    public long getCloseTimestamp() {
        return closeTimestamp;
    }

    public void setBufferSize(long bufferSize) {
        this.bufferSize = bufferSize;
    }

    public long getBufferSize() {
        return bufferSize;
    }

    public void setException(LDAPException exception) {
        boolean debug = log.isDebugEnabled();
        if (debug) log.debug("Error: \""+exception.getMessage()+"\".");
        super.setException(exception);
    }
}
