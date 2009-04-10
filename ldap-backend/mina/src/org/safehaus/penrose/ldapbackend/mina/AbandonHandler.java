package org.safehaus.penrose.ldapbackend.mina;

import org.apache.directory.shared.ldap.message.*;
import org.apache.directory.shared.ldap.message.SearchRequest;
import org.apache.directory.shared.ldap.message.AbandonRequest;
import org.apache.directory.shared.ldap.name.LdapDN;
import org.apache.mina.common.IoSession;
import org.apache.mina.handler.demux.MessageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.ldapbackend.*;

/**
 * @author Endi S. Dewata
 */
public class AbandonHandler implements MessageHandler {

    Logger log = LoggerFactory.getLogger(getClass());

    MinaHandler handler;

    public AbandonHandler(MinaHandler handler) {
        this.handler = handler;
    }

    public void messageReceived(final IoSession ioSession, Object message) throws Exception {

        final AbandonRequest request = (AbandonRequest)message;

        try {
            int messageId = request.getMessageId();
            int idToAbandon = request.getAbandoned();

            Long connectionId = handler.getConnectionId(ioSession);
            Connection connection = handler.getConnection(connectionId);

            if (connection == null) {
                log.error("Invalid connection "+connectionId+".");
                return;
            }

            org.safehaus.penrose.ldapbackend.AbandonRequest abandonRequest = handler.backend.createAbandonRequest();
            abandonRequest.setMessageId(messageId);
            abandonRequest.setIdToAbandon(idToAbandon);
            handler.getControls(request, abandonRequest);

            AbandonResponse abandonResponse = handler.backend.createAbandonResponse();
            abandonResponse.setMessageId(messageId);

            connection.abandon(abandonRequest, abandonResponse);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
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