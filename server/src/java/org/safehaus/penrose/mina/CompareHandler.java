package org.safehaus.penrose.mina;

import org.apache.mina.handler.demux.MessageHandler;
import org.apache.mina.common.IoSession;
import org.apache.directory.shared.ldap.message.CompareRequest;
import org.apache.directory.shared.ldap.message.LdapResult;
import org.apache.directory.shared.ldap.message.ResultCodeEnum;
import org.apache.directory.shared.ldap.message.CompareResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ietf.ldap.LDAPException;
import com.identyx.javabackend.Session;
import com.identyx.javabackend.DN;

/**
 * @author Endi S. Dewata
 */
public class CompareHandler implements MessageHandler {

    public Logger log = LoggerFactory.getLogger(getClass());

    MinaHandler handler;

    public CompareHandler(MinaHandler handler) {
        this.handler = handler;
    }

    public void messageReceived(IoSession ioSession, Object message) throws Exception {

        CompareRequest request = (CompareRequest)message;
        CompareResponse response = (CompareResponse)request.getResultResponse();
        LdapResult result = response.getLdapResult();

        try {
            DN dn = handler.backend.createDn(request.getName().getUpName());
            String name = request.getAttributeId();
            Object value = request.getAssertionValue();

            Session session = handler.getPenroseSession(ioSession);

            com.identyx.javabackend.CompareRequest penroseRequest = handler.backend.createCompareRequest();
            penroseRequest.setDn(dn);
            penroseRequest.setAttributeName(name);
            penroseRequest.setAttributeValue(value);
            handler.getControls(request, penroseRequest);

            com.identyx.javabackend.CompareResponse penroseResponse = handler.backend.createCompareResponse();

            boolean b = session.compare(penroseRequest, penroseResponse);
            result.setResultCode(b ? ResultCodeEnum.COMPARETRUE : ResultCodeEnum.COMPAREFALSE);

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
