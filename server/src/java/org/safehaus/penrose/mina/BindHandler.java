package org.safehaus.penrose.mina;

import org.apache.mina.handler.demux.MessageHandler;
import org.apache.mina.common.IoSession;
import org.apache.directory.shared.ldap.message.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.util.ExceptionUtil;
import org.safehaus.penrose.session.Session;
import org.ietf.ldap.LDAPException;

/**
 * @author Endi S. Dewata
 */
public class BindHandler implements MessageHandler {

    Logger log = LoggerFactory.getLogger(getClass());

    PenroseHandler handler;

    public BindHandler(PenroseHandler handler) {
        this.handler = handler;
    }

    public void messageReceived(IoSession ioSession, Object message) throws Exception {

        BindRequest request = (BindRequest)message;
        BindResponse response = (BindResponse)request.getResultResponse();
        LdapResult result = response.getLdapResult();

        try {
            String bindDn = request.getName().toString();
            String password = new String(request.getCredentials());

            Session session = handler.getPenroseSession(ioSession);

            org.safehaus.penrose.session.BindRequest penroseRequest = new org.safehaus.penrose.session.BindRequest();
            penroseRequest.setDn(bindDn);
            penroseRequest.setPassword(password);
            handler.getControls(request, penroseRequest);

            org.safehaus.penrose.session.BindResponse penroseResponse = new org.safehaus.penrose.session.BindResponse();

            session.bind(penroseRequest, penroseResponse);

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
