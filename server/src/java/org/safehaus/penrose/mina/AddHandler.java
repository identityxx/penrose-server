package org.safehaus.penrose.mina;

import org.apache.mina.handler.demux.MessageHandler;
import org.apache.mina.common.IoSession;
import org.apache.directory.shared.ldap.message.AddRequest;
import org.apache.directory.shared.ldap.message.LdapResult;
import org.apache.directory.shared.ldap.message.ResultCodeEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.session.PenroseSession;
import org.safehaus.penrose.util.ExceptionUtil;
import org.ietf.ldap.LDAPException;

import javax.naming.directory.Attributes;

/**
 * @author Endi S. Dewata
 */
public class AddHandler implements MessageHandler {

    Logger log = LoggerFactory.getLogger(getClass());

    PenroseHandler handler;

    public AddHandler(PenroseHandler handler) {
        this.handler = handler;
    }

    public void messageReceived(IoSession session, Object message) throws Exception {

        AddRequest request = (AddRequest)message;
        LdapResult result = request.getResultResponse().getLdapResult();

        try {
            String dn = request.getEntry().toString();
            Attributes attributes = request.getAttributes();

            PenroseSession penroseSession = handler.getPenroseSession(session);
            penroseSession.add(dn, attributes);

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
