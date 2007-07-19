package org.safehaus.penrose.mina;

import org.apache.mina.handler.demux.MessageHandler;
import org.apache.mina.common.IoSession;
import org.apache.directory.shared.ldap.message.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ietf.ldap.LDAPException;
import com.identyx.javabackend.DN;
import com.identyx.javabackend.Session;

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
            DN bindDn = handler.backend.createDn(request.getName().getUpName());
            byte[] password = request.getCredentials();

            Session session = handler.getPenroseSession(ioSession);

            com.identyx.javabackend.BindRequest penroseRequest = handler.backend.createBindRequest();
            penroseRequest.setDn(bindDn);
            penroseRequest.setPassword(password);
            handler.getControls(request, penroseRequest);

            com.identyx.javabackend.BindResponse penroseResponse = handler.backend.createBindResponse();

            session.bind(penroseRequest, penroseResponse);

            handler.setControls(penroseResponse, response);

        } catch (LDAPException e) {
            ResultCodeEnum rce = ResultCodeEnum.getResultCodeEnum(e.getResultCode());
            result.setResultCode(rce);
            result.setErrorMessage(e.getMessage());

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            ResultCodeEnum rce = ResultCodeEnum.getResultCode(e);
            result.setResultCode(rce);
            result.setErrorMessage(e.getMessage());

        } finally {
            ioSession.write(request.getResultResponse());
        }
    }
}
