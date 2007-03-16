package org.safehaus.penrose.mina;

import org.apache.mina.handler.demux.MessageHandler;
import org.apache.mina.common.IoSession;
import org.apache.directory.shared.ldap.message.CompareRequest;
import org.apache.directory.shared.ldap.message.LdapResult;
import org.apache.directory.shared.ldap.message.ResultCodeEnum;
import org.apache.directory.shared.ldap.message.CompareResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.util.ExceptionUtil;
import org.safehaus.penrose.session.Session;
import org.ietf.ldap.LDAPException;

/**
 * @author Endi S. Dewata
 */
public class CompareHandler implements MessageHandler {

    Logger log = LoggerFactory.getLogger(getClass());

    PenroseHandler handler;

    public CompareHandler(PenroseHandler handler) {
        this.handler = handler;
    }

    public void messageReceived(IoSession ioSession, Object message) throws Exception {

        CompareRequest request = (CompareRequest)message;
        CompareResponse response = (CompareResponse)request.getResultResponse();
        LdapResult result = response.getLdapResult();

        try {
            String dn = request.getName().toString();
            String name = request.getAttributeId();
            Object value = request.getAssertionValue();

            Session session = handler.getPenroseSession(ioSession);

            org.safehaus.penrose.session.CompareRequest penroseRequest = new org.safehaus.penrose.session.CompareRequest();
            penroseRequest.setDn(dn);
            penroseRequest.setAttributeName(name);
            penroseRequest.setAttributeValue(value);
            handler.getControls(request, penroseRequest);

            org.safehaus.penrose.session.CompareResponse penroseResponse = new org.safehaus.penrose.session.CompareResponse();

            boolean b = session.compare(penroseRequest, penroseResponse);
            result.setResultCode(b ? ResultCodeEnum.COMPARETRUE : ResultCodeEnum.COMPAREFALSE);

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
