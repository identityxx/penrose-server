package org.safehaus.penrose.ldapbackend.mina;

import org.apache.mina.handler.demux.MessageHandler;
import org.apache.mina.common.IoSession;
import org.apache.directory.shared.ldap.message.CompareRequest;
import org.apache.directory.shared.ldap.message.CompareResponse;
import org.apache.directory.shared.ldap.message.LdapResult;
import org.apache.directory.shared.ldap.message.ResultCodeEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.ldapbackend.DN;
import org.safehaus.penrose.ldapbackend.Connection;

/**
 * @author Endi S. Dewata
 */
public class CompareHandler implements MessageHandler {

    public Logger log = LoggerFactory.getLogger(getClass());

    MinaHandler handler;

    public CompareHandler(MinaHandler handler) {
        this.handler = handler;
    }

    public void messageReceived(IoSession ioSession, Object message) throws Exception {

        CompareRequest request = (CompareRequest)message;
        CompareResponse response = (CompareResponse)request.getResultResponse();
        LdapResult result = response.getLdapResult();

        try {
            int messageId = request.getMessageId();
            DN dn = handler.backend.createDn(request.getName().toString());
            String name = request.getAttributeId();
            Object value = request.getAssertionValue();

            Long connectionId = handler.getConnectionId(ioSession);
            Connection connection = handler.getConnection(connectionId);

            if (connection == null) {
                log.error("Invalid connection "+connectionId+".");
                return;
            }

            org.safehaus.penrose.ldapbackend.CompareRequest compareRequest = handler.backend.createCompareRequest();
            compareRequest.setMessageId(messageId);
            compareRequest.setDn(dn);
            compareRequest.setAttributeName(name);
            compareRequest.setAttributeValue(value);
            handler.getControls(request, compareRequest);

            org.safehaus.penrose.ldapbackend.CompareResponse compareResponse = handler.backend.createCompareResponse();

            handler.setControls(compareResponse, response);

            int rc = compareResponse.getReturnCode();
            if (rc != 0) {
                result.setErrorMessage(compareResponse.getErrorMessage());
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
