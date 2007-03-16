package org.safehaus.penrose.mina;

import org.apache.mina.common.IoSession;
import org.apache.mina.handler.demux.MessageHandler;
import org.apache.directory.shared.ldap.message.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.entry.Entry;
import org.safehaus.penrose.util.ExceptionUtil;
import org.safehaus.penrose.util.EntryUtil;
import org.safehaus.penrose.apacheds.FilterTool;
import org.ietf.ldap.LDAPException;

/**
 * @author Endi S. Dewata
 */
public class SearchHandler implements MessageHandler {

    Logger log = LoggerFactory.getLogger(getClass());

    PenroseHandler handler;

    public SearchHandler(PenroseHandler handler) {
        this.handler = handler;
    }

    public void messageReceived(IoSession ioSession, Object message) throws Exception {

        SearchRequest request = (SearchRequest)message;
        SearchResponseDone response = (SearchResponseDone)request.getResultResponse();
        LdapResult result = response.getLdapResult();

        try {
            String baseDn = request.getBase().toString();
            String filter = FilterTool.convert(request.getFilter()).toString();

            Session session = handler.getPenroseSession(ioSession);

            org.safehaus.penrose.session.SearchRequest penroseRequest = new org.safehaus.penrose.session.SearchRequest();
            penroseRequest.setDn(baseDn);
            penroseRequest.setFilter(filter);
            penroseRequest.setSizeLimit(request.getSizeLimit() );
            penroseRequest.setTimeLimit(request.getTimeLimit());
            penroseRequest.setScope(request.getScope().getValue() );
            penroseRequest.setTypesOnly(request.getTypesOnly());
            penroseRequest.setAttributes(request.getAttributes());
            handler.getControls(request, penroseRequest);

            org.safehaus.penrose.session.SearchResponse penroseResponse = new org.safehaus.penrose.session.SearchResponse();

            session.search(penroseRequest, penroseResponse);

            while (penroseResponse.hasNext()) {
                Entry entry = (Entry)penroseResponse.next();
                ioSession.write(createEntry(request, entry));
            }

            handler.setControls(penroseResponse, response);

        } catch (LDAPException e) {
            ResultCodeEnum rce = ResultCodeEnum.getResultCodeEnum(e.getResultCode());
            result.setResultCode(rce);
            result.setErrorMessage(e.getMessage());

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            LDAPException le = ExceptionUtil.createLDAPException(e);
            ResultCodeEnum rce = ResultCodeEnum.getResultCodeEnum(le.getResultCode());
            result.setResultCode(rce);
            result.setErrorMessage(le.getMessage());

        } finally {
            ioSession.write(response);
        }
    }

    public Response createEntry(org.apache.directory.shared.ldap.message.SearchRequest request, Entry entry) throws Exception {
        SearchResponseEntry response = new SearchResponseEntryImpl(request.getMessageId());
        response.setObjectName(new PenroseDN(entry.getDn().toString()));
        response.setAttributes(EntryUtil.getAttributes(entry));

        //response.add(control);
        
        return response;
    }
}
