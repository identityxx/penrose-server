package org.safehaus.penrose.mina;

import org.apache.mina.handler.demux.MessageHandler;
import org.apache.mina.common.IoSession;
import org.apache.directory.shared.ldap.message.LdapResult;
import org.apache.directory.shared.ldap.message.ResultCodeEnum;
import org.apache.directory.shared.ldap.message.ModifyDnRequest;
import org.apache.directory.shared.ldap.message.ModifyDnResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.util.ExceptionUtil;
import org.safehaus.penrose.entry.DN;
import org.safehaus.penrose.entry.RDN;
import org.ietf.ldap.LDAPException;

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
            DN dn = new DN(request.getName().toString());
            RDN newRdn = new RDN(request.getNewRdn().toString());
            boolean deleteOldRdn = request.getDeleteOldRdn();

            Session session = handler.getPenroseSession(ioSession);

            org.safehaus.penrose.session.ModRdnRequest penroseRequest = new org.safehaus.penrose.session.ModRdnRequest();
            penroseRequest.setDn(dn);
            penroseRequest.setNewRdn(newRdn);
            penroseRequest.setDeleteOldRdn(deleteOldRdn);
            handler.getControls(request, penroseRequest);

            org.safehaus.penrose.session.ModRdnResponse penroseResponse = new org.safehaus.penrose.session.ModRdnResponse();

            session.modrdn(penroseRequest, penroseResponse);

            handler.setControls(penroseResponse, response);

        } catch (LDAPException e) {
            ResultCodeEnum rce = ResultCodeEnum.getResultCodeEnum(e.getResultCode());
            result.setResultCode(rce);
            result.setErrorMessage(e.getMessage());

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            LDAPException le = ExceptionUtil.createLDAPException(e);
            ResultCodeEnum rce = ResultCodeEnum.getResultCodeEnum(le.getResultCode());
            result.setResultCode(rce);
            result.setErrorMessage(le.getMessage());

        } finally {
            ioSession.write(request.getResultResponse());
        }
    }
}
