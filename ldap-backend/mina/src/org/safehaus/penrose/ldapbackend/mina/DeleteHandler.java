package org.safehaus.penrose.ldapbackend.mina;

import org.apache.mina.handler.demux.MessageHandler;
import org.apache.mina.common.IoSession;
import org.apache.directory.shared.ldap.message.DeleteRequest;
import org.apache.directory.shared.ldap.message.DeleteResponse;
import org.apache.directory.shared.ldap.message.LdapResult;
import org.apache.directory.shared.ldap.message.ResultCodeEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.ldapbackend.DN;
import org.safehaus.penrose.ldapbackend.Connection;

/**
 * @author Endi S. Dewata
 */
public class DeleteHandler implements MessageHandler {

    public Logger log = LoggerFactory.getLogger(getClass());

    MinaHandler handler;

    public DeleteHandler(MinaHandler handler) {
        this.handler = handler;
    }

    public void messageReceived(IoSession ioSession, Object message) throws Exception {

        DeleteRequest request = (DeleteRequest)message;
        DeleteResponse response = (DeleteResponse)request.getResultResponse();
        LdapResult result = response.getLdapResult();

        try {
            int messageId = request.getMessageId();
            DN dn = handler.backend.createDn(request.getName().toString());

            Long connectionId = handler.getConnectionId(ioSession);
            Connection connection = handler.getConnection(connectionId);

            if (connection == null) {
                log.error("Invalid connection "+connectionId+".");
                return;
            }

            org.safehaus.penrose.ldapbackend.DeleteRequest deleteRequest = handler.backend.createDeleteRequest();
            deleteRequest.setMessageId(messageId);
            deleteRequest.setDn(dn);
            handler.getControls(request, deleteRequest);

            org.safehaus.penrose.ldapbackend.DeleteResponse deleteResponse = handler.backend.createDeleteResponse();

            connection.delete(deleteRequest, deleteResponse);

            handler.setControls(deleteResponse, response);

            int rc = deleteResponse.getReturnCode();
            if (rc != 0) {
                result.setErrorMessage(deleteResponse.getErrorMessage());
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
