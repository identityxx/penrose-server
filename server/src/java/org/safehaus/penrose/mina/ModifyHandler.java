package org.safehaus.penrose.mina;

import org.apache.mina.handler.demux.MessageHandler;
import org.apache.mina.common.IoSession;
import org.apache.directory.shared.ldap.message.ResultCodeEnum;
import org.apache.directory.shared.ldap.message.LdapResult;
import org.apache.directory.shared.ldap.message.ModifyRequest;
import org.apache.directory.shared.ldap.message.ModifyResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ietf.ldap.LDAPException;

import java.util.Collection;

import com.identyx.javabackend.DN;
import com.identyx.javabackend.Session;

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
            DN dn = handler.backend.createDn(request.getName().toString());
            Collection modifications = handler.createModifications(request.getModificationItems());

            Session session = handler.getPenroseSession(ioSession);

            com.identyx.javabackend.ModifyRequest penroseRequest = handler.backend.createModifyRequest();
            penroseRequest.setDn(dn);
            penroseRequest.setModifications(modifications);
            handler.getControls(request, penroseRequest);

            com.identyx.javabackend.ModifyResponse penroseResponse = handler.backend.createModifyResponse();

            session.modify(penroseRequest, penroseResponse);

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
