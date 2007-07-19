package org.safehaus.penrose.mina;

import org.apache.mina.common.IoSession;
import org.apache.mina.handler.demux.MessageHandler;
import org.apache.directory.shared.ldap.message.*;
import org.apache.directory.shared.ldap.name.LdapDN;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ietf.ldap.LDAPException;
import com.identyx.javabackend.Session;
import com.identyx.javabackend.DN;
import com.identyx.javabackend.Filter;
import com.identyx.javabackend.Entry;

/**
 * @author Endi S. Dewata
 */
public class SearchHandler implements MessageHandler {

    public Logger log = LoggerFactory.getLogger(getClass());

    MinaHandler handler;

    public SearchHandler(MinaHandler handler) {
        this.handler = handler;
    }

    public void messageReceived(IoSession ioSession, Object message) throws Exception {

        SearchRequest request = (SearchRequest)message;
        SearchResponseDone response = (SearchResponseDone)request.getResultResponse();
        LdapResult result = response.getLdapResult();

        try {
            DN baseDn = handler.backend.createDn(request.getBase().getUpName());
            Filter filter = handler.backend.createFilter(MinaFilterTool.convert(request.getFilter()));

            Session session = handler.getPenroseSession(ioSession);

            com.identyx.javabackend.SearchRequest penroseRequest = handler.backend.createSearchRequest();
            penroseRequest.setDn(baseDn);
            penroseRequest.setFilter(filter);
            penroseRequest.setSizeLimit(request.getSizeLimit() );
            penroseRequest.setTimeLimit(request.getTimeLimit());
            penroseRequest.setScope(request.getScope().getValue() );
            //penroseRequest.setTypesOnly(request.getTypesOnly());
            penroseRequest.setAttributes(request.getAttributes());
            handler.getControls(request, penroseRequest);

            com.identyx.javabackend.SearchResponse penroseResponse = handler.backend.createSearchResponse();

            session.search(penroseRequest, penroseResponse);

            while (penroseResponse.hasNext()) {
                com.identyx.javabackend.SearchResult searchResult = (com.identyx.javabackend.SearchResult)penroseResponse.next();
                sendSearchResult(ioSession, request, searchResult);
            }

            handler.setControls(penroseResponse, response);

        } catch (LDAPException e) {
            ResultCodeEnum rce = ResultCodeEnum.getResultCodeEnum(e.getResultCode());
            result.setResultCode(rce);
            result.setErrorMessage(e.getMessage());

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            ResultCodeEnum rce = ResultCodeEnum.getResultCode(e);
            result.setResultCode(rce);
            result.setErrorMessage(e.getMessage());

        } finally {
            ioSession.write(response);
        }
    }

    public void sendSearchResult(
            IoSession ioSession,
            org.apache.directory.shared.ldap.message.SearchRequest request,
            com.identyx.javabackend.SearchResult result
    ) throws Exception {

        Entry entry = result.getEntry();

        SearchResponseEntry response = new SearchResponseEntryImpl(request.getMessageId());
        response.setObjectName(new LdapDN(entry.getDn().toString()));
        response.setAttributes(handler.createAttributes(entry.getAttributes()));
        handler.setControls(result, response);

        ioSession.write(response);
    }
}
