package org.safehaus.penrose.ldapbackend.mina;

import org.apache.mina.handler.demux.MessageHandler;
import org.apache.mina.common.IoSession;
import org.apache.directory.shared.ldap.message.ModifyRequest;
import org.apache.directory.shared.ldap.message.ModifyResponse;
import org.apache.directory.shared.ldap.message.LdapResult;
import org.apache.directory.shared.ldap.message.ResultCodeEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.ldapbackend.DN;
import org.safehaus.penrose.ldapbackend.Connection;
import org.safehaus.penrose.ldapbackend.Modification;

import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class ModifyHandler implements MessageHandler {

    Logger log = LoggerFactory.getLogger(getClass());

    MinaHandler handler;

    public ModifyHandler(MinaHandler handler) {
        this.handler = handler;
    }

    public void messageReceived(IoSession ioSession, Object message) throws Exception {

        ModifyRequest request = (ModifyRequest)message;
        ModifyResponse response = (ModifyResponse)request.getResultResponse();
        LdapResult result = response.getLdapResult();

        try {
            int messageId = request.getMessageId();
            DN dn = handler.backend.createDn(request.getName().toString());
            Collection<Modification> modifications = handler.createModifications(request.getModificationItems());

            Long connectionId = handler.getConnectionId(ioSession);
            Connection connection = handler.getConnection(connectionId);

            if (connection == null) {
                log.error("Invalid connection "+connectionId+".");
                return;
            }

            org.safehaus.penrose.ldapbackend.ModifyRequest modifyRequest = handler.backend.createModifyRequest();
            modifyRequest.setMessageId(messageId);
            modifyRequest.setDn(dn);
            modifyRequest.setModifications(modifications);
            handler.getControls(request, modifyRequest);

            org.safehaus.penrose.ldapbackend.ModifyResponse modifyResponse = handler.backend.createModifyResponse();

            connection.modify(modifyRequest, modifyResponse);

            handler.setControls(modifyResponse, response);

            int rc = modifyResponse.getReturnCode();
            if (rc != 0) {
                result.setErrorMessage(modifyResponse.getErrorMessage());
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
