package org.safehaus.penrose.mina;

import org.apache.mina.handler.demux.MessageHandler;
import org.apache.mina.common.IoSession;
import org.apache.directory.shared.ldap.message.AddRequest;
import org.apache.directory.shared.ldap.message.LdapResult;
import org.apache.directory.shared.ldap.message.ResultCodeEnum;
import org.apache.directory.shared.ldap.message.AddResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.session.Session;
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

    public void messageReceived(IoSession ioSession, Object message) throws Exception {

        AddRequest request = (AddRequest)message;
        AddResponse response = (AddResponse)request.getResultResponse();
        LdapResult result = response.getLdapResult();

        try {
            String dn = request.getEntry().toString();
            Attributes attributes = request.getAttributes();

            Session session = handler.getPenroseSession(ioSession);

            org.safehaus.penrose.session.AddRequest penroseRequest = new org.safehaus.penrose.session.AddRequest();
            penroseRequest.setDn(dn);
            penroseRequest.setAttributeValues(attributes);
            handler.getControls(request, penroseRequest);

            org.safehaus.penrose.session.AddResponse penroseResponse = new org.safehaus.penrose.session.AddResponse();

            session.add(penroseRequest, penroseResponse);

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
