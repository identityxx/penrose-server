package org.safehaus.penrose.mina;

import org.apache.mina.handler.demux.MessageHandler;
import org.apache.mina.common.IoSession;
import org.apache.directory.shared.ldap.message.ResultCodeEnum;
import org.apache.directory.shared.ldap.message.LdapResult;
import org.apache.directory.shared.ldap.message.ModifyRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.util.ExceptionUtil;
import org.safehaus.penrose.session.PenroseSession;
import org.ietf.ldap.LDAPException;

import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class ModifyHandler implements MessageHandler {

    Logger log = LoggerFactory.getLogger(getClass());

    PenroseHandler handler;

    public ModifyHandler(PenroseHandler handler) {
        this.handler = handler;
    }

    public void messageReceived(IoSession session, Object message) throws Exception {

        ModifyRequest request = (ModifyRequest)message;
        LdapResult result = request.getResultResponse().getLdapResult();

        try {
            String dn = request.getName().toString();
            Collection modifications = request.getModificationItems();
            
            PenroseSession penroseSession = handler.getPenroseSession(session);
            penroseSession.modify(dn, modifications);

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
