package org.safehaus.penrose.mina;

import org.apache.mina.handler.demux.MessageHandler;
import org.apache.mina.common.IoSession;
import org.apache.directory.shared.ldap.message.LdapResult;
import org.apache.directory.shared.ldap.message.ResultCodeEnum;
import org.apache.directory.shared.ldap.message.ModifyDnRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.session.PenroseSession;
import org.safehaus.penrose.util.ExceptionUtil;
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

    public void messageReceived(IoSession session, Object message) throws Exception {

        ModifyDnRequest request = (ModifyDnRequest)message;
        LdapResult result = request.getResultResponse().getLdapResult();

        try {
            String dn = request.getName().toString();
            String newRdn = request.getNewRdn().toString();
            boolean deleteOldRdn = request.getDeleteOldRdn();

            PenroseSession penroseSession = handler.getPenroseSession(session);
            penroseSession.modrdn(dn, newRdn, deleteOldRdn);

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
            session.write(request.getResultResponse());
        }
    }
}
