package org.safehaus.penrose.mina;

import org.apache.mina.handler.demux.MessageHandler;
import org.apache.mina.common.IoSession;
import org.apache.directory.shared.ldap.message.DeleteRequest;
import org.apache.directory.shared.ldap.message.ResultCodeEnum;
import org.apache.directory.shared.ldap.message.LdapResult;
import org.apache.directory.shared.ldap.message.DeleteResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ietf.ldap.LDAPException;
import com.identyx.javabackend.DN;
import com.identyx.javabackend.Session;

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
            DN dn = handler.backend.createDn(request.getName().getUpName());

            Session session = handler.getPenroseSession(ioSession);

            com.identyx.javabackend.DeleteRequest penroseRequest = handler.backend.createDeleteRequest();
            penroseRequest.setDn(dn);
            handler.getControls(request, penroseRequest);

            com.identyx.javabackend.DeleteResponse penroseResponse = handler.backend.createDeleteResponse();

            session.delete(penroseRequest, penroseResponse);

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
