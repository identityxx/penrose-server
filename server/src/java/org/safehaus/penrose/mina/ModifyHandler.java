package org.safehaus.penrose.mina;

import org.apache.mina.handler.demux.MessageHandler;
import org.apache.mina.common.IoSession;
import org.apache.directory.shared.ldap.message.ResultCodeEnum;
import org.apache.directory.shared.ldap.message.LdapResult;
import org.apache.directory.shared.ldap.message.ModifyRequest;
import org.apache.directory.shared.ldap.message.ModifyResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.util.ExceptionUtil;
import org.safehaus.penrose.util.LDAPUtil;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.session.Modification;
import org.safehaus.penrose.entry.Attribute;
import org.safehaus.penrose.entry.DN;
import org.ietf.ldap.LDAPException;

import javax.naming.NamingEnumeration;
import java.util.Collection;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class ModifyHandler implements MessageHandler {

    Logger log = LoggerFactory.getLogger(getClass());

    PenroseHandler handler;

    public ModifyHandler(PenroseHandler handler) {
        this.handler = handler;
    }

    public void messageReceived(IoSession ioSession, Object message) throws Exception {

        ModifyRequest request = (ModifyRequest)message;
        ModifyResponse response = (ModifyResponse)request.getResultResponse();
        LdapResult result = response.getLdapResult();

        try {
            DN dn = new DN(request.getName().toString());
            Collection modifications = handler.createModifications(request.getModificationItems());

            Session session = handler.getPenroseSession(ioSession);

            org.safehaus.penrose.session.ModifyRequest penroseRequest = new org.safehaus.penrose.session.ModifyRequest();
            penroseRequest.setDn(dn);
            penroseRequest.setModifications(modifications);
            handler.getControls(request, penroseRequest);

            org.safehaus.penrose.session.ModifyResponse penroseResponse = new org.safehaus.penrose.session.ModifyResponse();

            session.modify(penroseRequest, penroseResponse);

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
