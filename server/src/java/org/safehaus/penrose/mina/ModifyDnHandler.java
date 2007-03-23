package org.safehaus.penrose.mina;

import org.apache.mina.handler.demux.MessageHandler;
import org.apache.mina.common.IoSession;
import org.apache.directory.shared.ldap.message.LdapResult;
import org.apache.directory.shared.ldap.message.ResultCodeEnum;
import org.apache.directory.shared.ldap.message.ModifyDnRequest;
import org.apache.directory.shared.ldap.message.ModifyDnResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ietf.ldap.LDAPException;
import com.identyx.javabackend.DN;
import com.identyx.javabackend.RDN;
import com.identyx.javabackend.Session;

/**
 * @author Endi S. Dewata
 */
public class ModifyDnHandler implements MessageHandler {

    Logger log = LoggerFactory.getLogger(getClass());

    PenroseHandler handler;

    public ModifyDnHandler(PenroseHandler handler) {
        this.handler = handler;
    }

    public void messageReceived(IoSession ioSession, Object message) throws Exception {

        ModifyDnRequest request = (ModifyDnRequest)message;
        ModifyDnResponse response = (ModifyDnResponse)request.getResultResponse();
        LdapResult result = response.getLdapResult();

        try {
            DN dn = handler.backend.createDn(request.getName().toString());
            RDN newRdn = handler.backend.createRdn(request.getNewRdn().toString());
            boolean deleteOldRdn = request.getDeleteOldRdn();

            Session session = handler.getPenroseSession(ioSession);

            com.identyx.javabackend.ModRdnRequest penroseRequest = handler.backend.createModRdnRequest();
            penroseRequest.setDn(dn);
            penroseRequest.setNewRdn(newRdn);
            penroseRequest.setDeleteOldRdn(deleteOldRdn);
            handler.getControls(request, penroseRequest);

            com.identyx.javabackend.ModRdnResponse penroseResponse = handler.backend.createModRdnResponse();

            session.modrdn(penroseRequest, penroseResponse);

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
