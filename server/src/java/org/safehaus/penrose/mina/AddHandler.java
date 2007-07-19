package org.safehaus.penrose.mina;

import org.apache.mina.handler.demux.MessageHandler;
import org.apache.mina.common.IoSession;
import org.apache.directory.shared.ldap.message.AddRequest;
import org.apache.directory.shared.ldap.message.LdapResult;
import org.apache.directory.shared.ldap.message.ResultCodeEnum;
import org.apache.directory.shared.ldap.message.AddResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ietf.ldap.LDAPException;
import com.identyx.javabackend.Attributes;
import com.identyx.javabackend.DN;
import com.identyx.javabackend.Session;

/**
 * @author Endi S. Dewata
 */
public class AddHandler implements MessageHandler {

    public Logger log = LoggerFactory.getLogger(getClass());

    MinaHandler handler;

    public AddHandler(MinaHandler handler) {
        this.handler = handler;
    }

    public void messageReceived(IoSession ioSession, Object message) throws Exception {

        AddRequest request = (AddRequest)message;
        AddResponse response = (AddResponse)request.getResultResponse();
        LdapResult result = response.getLdapResult();

        try {
            DN dn = handler.backend.createDn(request.getEntry().getUpName());
            Attributes attributes = handler.createAttributes(request.getAttributes());

            Session session = handler.getPenroseSession(ioSession);

            com.identyx.javabackend.AddRequest penroseRequest = handler.backend.createAddRequest();
            penroseRequest.setDn(dn);
            penroseRequest.setAttributes(attributes);
            handler.getControls(request, penroseRequest);

            com.identyx.javabackend.AddResponse penroseResponse = handler.backend.createAddResponse();

            session.add(penroseRequest, penroseResponse);

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
