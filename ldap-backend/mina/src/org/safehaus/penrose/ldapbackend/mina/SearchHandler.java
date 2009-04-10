package org.safehaus.penrose.ldapbackend.mina;

import org.apache.directory.shared.ldap.message.*;
import org.apache.directory.shared.ldap.message.SearchRequest;
import org.apache.directory.shared.ldap.name.LdapDN;
import org.apache.mina.common.IoSession;
import org.apache.mina.handler.demux.MessageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.ldapbackend.*;

/**
 * @author Endi S. Dewata
 */
public class SearchHandler implements MessageHandler {

    Logger log = LoggerFactory.getLogger(getClass());

    MinaHandler handler;

    public SearchHandler(MinaHandler handler) {
        this.handler = handler;
    }

    public void messageReceived(final IoSession ioSession, Object message) throws Exception {

        final SearchRequest request = (SearchRequest)message;
        SearchResponseDone response = (SearchResponseDone)request.getResultResponse();
        LdapResult result = response.getLdapResult();

        try {
            int messageId = request.getMessageId();
            DN baseDn = handler.backend.createDn(request.getBase().toString());
            Filter filter = handler.backend.createFilter(FilterTool.convert(request.getFilter()));

            Long connectionId = handler.getConnectionId(ioSession);
            Connection connection = handler.getConnection(connectionId);

            if (connection == null) {
                log.error("Invalid connection "+connectionId+".");
                return;
            }

            org.safehaus.penrose.ldapbackend.SearchRequest searchRequest = handler.backend.createSearchRequest();
            searchRequest.setMessageId(messageId);
            searchRequest.setDn(baseDn);
            searchRequest.setFilter(filter);
            searchRequest.setSizeLimit(request.getSizeLimit() );
            searchRequest.setTimeLimit(request.getTimeLimit());
            searchRequest.setScope(request.getScope().getValue() );
            //searchRequest.setTypesOnly(request.getTypesOnly());
            searchRequest.setAttributes(request.getAttributes());
            handler.getControls(request, searchRequest);

            SearchResponse searchResponse = handler.backend.createSearchResponse();

            searchResponse.addListener(new SearchListener() {
                public void add(SearchResult result) throws Exception {
                    sendSearchResult(ioSession, request, result);
                }
                public void add(SearchReference reference) throws Exception {
                    sendSearchReference(ioSession, request, reference);
                }
                public void close() throws Exception {
                }
            });

            connection.search(searchRequest, searchResponse);
/*
            while (searchResponse.hasNext()) {
                org.safehaus.penrose.ldapbackend.SearchResult searchResult = (org.safehaus.penrose.ldapbackend.SearchResult)searchResponse.next();
                sendSearchResult(ioSession, request, searchResult);
            }
*/
            handler.setControls(searchResponse, response);

            int rc = searchResponse.getReturnCode();
            if (rc != 0) {
                result.setErrorMessage(searchResponse.getErrorMessage());
            }
            result.setResultCode(ResultCodeEnum.getResultCodeEnum(rc));

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            result.setResultCode(ResultCodeEnum.getResultCode(e));
            result.setErrorMessage(e.getMessage());

        } finally {
            ioSession.write(response);
        }
    }

    public void sendSearchResult(
            IoSession ioSession,
            SearchRequest request,
            SearchResult result
    ) throws Exception {

        DN dn = result.getDn();
        Attributes attributes = result.getAttributes();

        SearchResponseEntry response = new SearchResponseEntryImpl(request.getMessageId());
        response.setObjectName(new LdapDN(dn.toString()));
        response.setAttributes(handler.createAttributes(attributes));
        handler.setControls(result, response);

        ioSession.write(response);
    }

    public void sendSearchReference(
            IoSession ioSession,
            SearchRequest request,
            SearchReference reference
    ) throws Exception {

        ReferralImpl referral = new ReferralImpl();
        for (String url : reference.getUrls()) {
            referral.addLdapUrl(url);
        }

        SearchResponseReference response = new SearchResponseReferenceImpl(request.getMessageId());
        response.setReferral(referral);

        ioSession.write(response);
    }
}
