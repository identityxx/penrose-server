package org.safehaus.penrose.mina;

import org.apache.mina.common.IoSession;
import org.apache.mina.handler.demux.MessageHandler;
import org.apache.directory.shared.ldap.message.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.entry.Entry;
import org.safehaus.penrose.util.ExceptionUtil;
import org.safehaus.penrose.util.EntryUtil;
import org.safehaus.penrose.apacheds.FilterTool;
import org.ietf.ldap.LDAPException;

import java.util.Collection;
import java.util.Iterator;

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
        LdapResult ldapResult = response.getLdapResult();

        try {
            String baseDn = request.getBase().toString();
            String filter = FilterTool.convert(request.getFilter()).toString();

            org.safehaus.penrose.session.Session session = handler.getPenroseSession(ioSession);

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
                org.safehaus.penrose.session.SearchResult result = (org.safehaus.penrose.session.SearchResult)penroseResponse.next();
                sendSearchResult(ioSession, request, result);
            }

            handler.setControls(penroseResponse, response);

        } catch (LDAPException e) {
            ResultCodeEnum rce = ResultCodeEnum.getResultCodeEnum(e.getResultCode());
            ldapResult.setResultCode(rce);
            ldapResult.setErrorMessage(e.getMessage());

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            LDAPException le = ExceptionUtil.createLDAPException(e);
            ResultCodeEnum rce = ResultCodeEnum.getResultCodeEnum(le.getResultCode());
            ldapResult.setResultCode(rce);
            ldapResult.setErrorMessage(le.getMessage());

        } finally {
            ioSession.write(response);
        }
    }

    public void sendSearchResult(
            IoSession ioSession,
            org.apache.directory.shared.ldap.message.SearchRequest request,
            org.safehaus.penrose.session.SearchResult result
    ) throws Exception {

        Entry entry = result.getEntry();

        SearchResponseEntry response = new SearchResponseEntryImpl(request.getMessageId());
        response.setObjectName(new PenroseDN(entry.getDn().toString()));
        response.setAttributes(EntryUtil.getAttributes(entry));

        Collection controls = result.getControls();
        for (Iterator i=controls.iterator(); i.hasNext(); ) {
            org.safehaus.penrose.control.Control control = (org.safehaus.penrose.control.Control)i.next();
            Control ctrl = handler.createControl(control);
            response.add(ctrl);
        }

        ioSession.write(response);
    }
}
