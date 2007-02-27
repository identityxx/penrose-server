package org.safehaus.penrose.mina;

import org.apache.mina.common.IoSession;
import org.apache.mina.handler.demux.MessageHandler;
import org.apache.directory.shared.ldap.message.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.session.PenroseSession;
import org.safehaus.penrose.session.PenroseSearchControls;
import org.safehaus.penrose.session.PenroseSearchResults;
import org.safehaus.penrose.entry.Entry;
import org.safehaus.penrose.util.ExceptionUtil;
import org.safehaus.penrose.util.EntryUtil;
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

    public void messageReceived(IoSession session, Object message) throws Exception {

        SearchRequest request = (SearchRequest)message;
        LdapResult result = request.getResultResponse().getLdapResult();

        try {
            String baseDn = request.getBase().toString();
            String filter = request.getFilter().toString();

            PenroseSearchResults results = new PenroseSearchResults();

            PenroseSearchControls sc = new PenroseSearchControls();
            sc.setSizeLimit(request.getSizeLimit() );
            sc.setTimeLimit(request.getTimeLimit());
            sc.setScope(request.getScope().getValue() );
            sc.setTypesOnly(request.getTypesOnly());
            sc.setAttributes(request.getAttributes());

            PenroseSession penroseSession = handler.getPenroseSession(session);
            penroseSession.search(baseDn, filter, sc, results);

            while (results.hasNext()) {
                Entry entry = (Entry)results.next();
                Response response = createEntry(request, entry);
                session.write(response);
            }

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
            session.write(request.getResultResponse());
        }
    }

    public Response createEntry(SearchRequest request, Entry entry) throws Exception {
        SearchResponseEntry response = new SearchResponseEntryImpl(request.getMessageId());
        response.setObjectName(new PenroseDN(entry.getDn().toString()));
        response.setAttributes(EntryUtil.getAttributes(entry));
        return response;
    }
}
