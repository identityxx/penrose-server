package org.safehaus.penrose.ldapbackend.mina;

import org.apache.mina.handler.demux.MessageHandler;
import org.apache.mina.common.IoSession;
import org.apache.directory.shared.ldap.message.ModifyDnRequest;
import org.apache.directory.shared.ldap.message.ModifyDnResponse;
import org.apache.directory.shared.ldap.message.LdapResult;
import org.apache.directory.shared.ldap.message.ResultCodeEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.ldapbackend.DN;
import org.safehaus.penrose.ldapbackend.RDN;
import org.safehaus.penrose.ldapbackend.*;

/**
 * @author Endi S. Dewata
 */
public class ModifyDnHandler implements MessageHandler {

    public Logger log = LoggerFactory.getLogger(getClass());

    MinaHandler handler;

    public ModifyDnHandler(MinaHandler handler) {
        this.handler = handler;
    }

    public void messageReceived(IoSession ioSession, Object message) throws Exception {

        ModifyDnRequest request = (ModifyDnRequest)message;
        ModifyDnResponse response = (ModifyDnResponse)request.getResultResponse();
        LdapResult result = response.getLdapResult();

        try {
            int messageId = request.getMessageId();
            DN dn = handler.backend.createDn(request.getName().toString());
            RDN newRdn = handler.backend.createRdn(request.getNewRdn().toString());
            boolean deleteOldRdn = request.getDeleteOldRdn();

            Long connectionId = handler.getConnectionId(ioSession);
            Connection connection = handler.getConnection(connectionId);

            if (connection == null) {
                log.error("Invalid connection "+connectionId+".");
                return;
            }

            ModRdnRequest modRdnRequest = handler.backend.createModRdnRequest();
            modRdnRequest.setMessageId(messageId);
            modRdnRequest.setDn(dn);
            modRdnRequest.setNewRdn(newRdn);
            modRdnRequest.setDeleteOldRdn(deleteOldRdn);
            handler.getControls(request, modRdnRequest);

            ModRdnResponse modRdnResponse = handler.backend.createModRdnResponse();

            connection.modrdn(modRdnRequest, modRdnResponse);

            handler.setControls(modRdnResponse, response);

            int rc = modRdnResponse.getReturnCode();
            if (rc != 0) {
                result.setErrorMessage(modRdnResponse.getErrorMessage());
            }
            result.setResultCode(ResultCodeEnum.getResultCodeEnum(rc));

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            result.setResultCode(ResultCodeEnum.getResultCode(e));
            result.setErrorMessage(e.getMessage());

        } finally {
            ioSession.write(request.getResultResponse());
        }
    }
}
