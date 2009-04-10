package org.safehaus.penrose.ldapbackend.mina;

import org.apache.mina.handler.demux.MessageHandler;
import org.apache.mina.common.IoSession;
import org.apache.directory.shared.ldap.message.BindRequest;
import org.apache.directory.shared.ldap.message.BindResponse;
import org.apache.directory.shared.ldap.message.LdapResult;
import org.apache.directory.shared.ldap.message.ResultCodeEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.ldapbackend.DN;
import org.safehaus.penrose.ldapbackend.Connection;

/**
 * @author Endi S. Dewata
 */
public class BindHandler implements MessageHandler {

    public Logger log = LoggerFactory.getLogger(getClass());

    MinaHandler handler;

    public BindHandler(MinaHandler handler) {
        this.handler = handler;
    }

    public void messageReceived(IoSession ioSession, Object message) throws Exception {

        BindRequest request = (BindRequest)message;
        BindResponse response = (BindResponse)request.getResultResponse();
        LdapResult result = response.getLdapResult();

        try {
            int messageId = request.getMessageId();
            DN bindDn = handler.backend.createDn(request.getName().toString());
            byte[] password = request.getCredentials();

            Long connectionId = handler.getConnectionId(ioSession);
            Connection connection = handler.getConnection(connectionId);

            if (connection == null) {
                log.error("Invalid connection "+connectionId+".");
                return;
            }

            org.safehaus.penrose.ldapbackend.BindRequest bindRequest = handler.backend.createBindRequest();
            bindRequest.setMessageId(messageId);
            bindRequest.setDn(bindDn);
            bindRequest.setPassword(password);
            handler.getControls(request, bindRequest);

            org.safehaus.penrose.ldapbackend.BindResponse bindResponse = handler.backend.createBindResponse();

            connection.bind(bindRequest, bindResponse);

            handler.setControls(bindResponse, response);

            int rc = bindResponse.getReturnCode();
            if (rc != 0) {
                result.setErrorMessage(bindResponse.getErrorMessage());
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
