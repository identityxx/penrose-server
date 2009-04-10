package org.safehaus.penrose.ldapbackend.mina;

import org.apache.mina.handler.demux.MessageHandler;
import org.apache.mina.common.IoSession;
import org.apache.directory.shared.ldap.message.UnbindRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.ldapbackend.Connection;
import org.safehaus.penrose.ldapbackend.UnbindResponse;

/**
 * @author Endi S. Dewata
 */
public class UnbindHandler implements MessageHandler {

    Logger log = LoggerFactory.getLogger(getClass());

    MinaHandler handler;

    public UnbindHandler(MinaHandler handler) {
        this.handler = handler;
    }

    public void messageReceived(IoSession ioSession, Object message) throws Exception {

        UnbindRequest request = (UnbindRequest)message;
        //UnbindResponse response = (UnbindResponse)request.getResultResponse();

        int messageId = request.getMessageId();
        
        Long connectionId = handler.getConnectionId(ioSession);
        Connection connection = handler.getConnection(connectionId);

        if (connection == null) {
            log.error("Invalid connection "+connectionId+".");
            return;
        }

        org.safehaus.penrose.ldapbackend.UnbindRequest unbindRequest = handler.backend.createUnbindRequest();
        unbindRequest.setMessageId(messageId);
        handler.getControls(request, unbindRequest);

        UnbindResponse unbindResponse = handler.backend.createUnbindResponse();

        connection.unbind(unbindRequest, unbindResponse);

        //handler.setControls(unbindResponse, response);
/*
        int rc = unbindResponse.getReturnCode();
        if (rc != 0) {
            result.setErrorMessage(unbindResponse.getErrorMessage());
        }
        result.setResultCode(ResultCodeEnum.getResultCodeEnum(rc));
*/
    }
}
