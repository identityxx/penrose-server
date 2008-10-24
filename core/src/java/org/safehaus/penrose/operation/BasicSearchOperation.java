package org.safehaus.penrose.operation;

import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.schema.SchemaManager;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.log.Access;
import org.safehaus.penrose.statistic.StatisticManager;
import org.ietf.ldap.LDAPException;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class BasicSearchOperation extends BasicOperation implements SearchOperation {

    protected SearchRequest searchRequest;
    protected SearchResponse searchResponse;

    protected long bufferSize;

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
        searchResponse.setBufferSize(bufferSize);
    }

    public DN getDn() {
        return searchRequest.getDn();
    }

    public void setDn(DN dn) {
        searchRequest.setDn(dn);
    }

    public int getScope() {
        return searchRequest.getScope();
    }

    public void setScope(int scope) {
        searchRequest.setScope(scope);
    }

    public Filter getFilter() {
        return searchRequest.getFilter();
    }

    public void setFilter(Filter filter) {
        searchRequest.setFilter(filter);
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

    public void add(SearchResult result) throws Exception {

        if (isAbandoned()) {
            if (debug) log.debug("Operation "+operationName+" has been abandoned.");
            return;
        }

        if (debug) log.debug("Result: \""+result.getDn()+"\".");

        long sizeLimit = getSizeLimit();
        long totalCount = getTotalCount();

        if (sizeLimit > 0 && totalCount >= sizeLimit) {
            LDAPException exception = LDAP.createException(LDAP.SIZE_LIMIT_EXCEEDED);
            setException(exception);
            throw exception;
        }

        searchResponse.add(result);
    }

    public void add(SearchReference reference) throws Exception {

        if (isAbandoned()) {
            if (debug) log.debug("Operation "+operationName+" has been abandoned.");
            return;
        }

        if (debug) log.debug("Reference: \""+ reference.getDn()+"\".");

        searchResponse.add(reference);
    }

    public void close() throws Exception {

        if (searchResponse.isClosed()) {
            if (debug) log.debug("Search operation is already closed.");
            return;
        }

        if (debug) log.debug("Closing search operation "+operationName+".");
        searchResponse.close();
    }

    public boolean isClosed() {
        return searchResponse.isClosed();
    }

    public long getTotalCount() {
        return searchResponse.getTotalCount();
    }

    public void setBufferSize(long bufferSize) {
        this.bufferSize = bufferSize;
    }

    public long getBufferSize() {
        return bufferSize;
    }

    public void setException(LDAPException exception) {
        if (debug) log.debug("Error: \""+exception.getMessage()+"\".");
        super.setException(exception);
    }

    public void normalize() throws Exception {

        SchemaManager schemaManager = session.getPenroseContext().getSchemaManager();

        DN dn = getDn();
        DN normalizedDn = schemaManager.normalize(dn);
        setDn(normalizedDn);

        Collection<String> attributes = getAttributes();
        Collection<String> normalizedAttributes = schemaManager.normalize(attributes);
        setAttributes(normalizedAttributes);
    }

    public void execute() throws Exception {

        final long timestamp = System.currentTimeMillis();
        Access.log(this);

        session.addOperation(this);

        try {
            StatisticManager statisticManager = penrose.getStatisticManager();
            statisticManager.incrementCounter(StatisticManager.SEARCH);

            String sessionName = session.getSessionName();

            DN bindDn     = session.getBindDn();
            DN dn         = searchRequest.getDn();
            Filter filter = searchRequest.getFilter();
            int scope     = searchRequest.getScope();

            if (warn) log.warn("Session "+ sessionName+" ("+operationName+"): Search "+dn+" with filter "+filter+".");

            if (debug) {
                log.debug("----------------------------------------------------------------------------------");
                log.debug("SEARCH:");
                log.debug(" - Session        : "+sessionName);
                log.debug(" - Message        : "+operationName);
                log.debug(" - Bind DN        : "+(bindDn == null ? "" : bindDn));
                log.debug(" - Base DN        : "+dn);
                log.debug(" - Scope          : "+LDAP.getScope(scope));
                log.debug(" - Filter         : "+filter);
                log.debug(" - Attributes     : "+getAttributes());
                log.debug("");

                log.debug("Controls: "+getRequestControls());
            }

            normalize();
            
            PartitionManager partitionManager = penrose.getPartitionManager();
            Partition partition = partitionManager.getPartition(dn);

            SearchOperation op = new PipelineSearchOperation(this) {
                public void close() throws Exception {
                    super.close();

                    session.removeOperation(searchOperation.getOperationName());
                    Access.log(searchOperation, System.currentTimeMillis() - timestamp);
                }
            };

            partition.search(op);

        } catch (Exception e) {
            log.error(e.getMessage(), e);

            response.setException(e);
            try { close(); } catch (Exception ex) { log.error(ex.getMessage(), ex); }

            session.removeOperation(this.getOperationName());
            Access.log(this, System.currentTimeMillis() - timestamp);

            throw e;
        }
    }
}
