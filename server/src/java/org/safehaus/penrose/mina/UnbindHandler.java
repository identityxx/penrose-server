package org.safehaus.penrose.mina;

import org.apache.mina.handler.demux.MessageHandler;
import org.apache.mina.common.IoSession;
import org.apache.directory.shared.ldap.message.UnbindRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.session.Session;

/**
 * @author Endi S. Dewata
 */
public class UnbindHandler implements MessageHandler {

    Logger log = LoggerFactory.getLogger(getClass());

    PenroseHandler handler;

    public UnbindHandler(PenroseHandler handler) {
        this.handler = handler;
    }

    public void messageReceived(IoSession ioSession, Object message) throws Exception {

        UnbindRequest request = (UnbindRequest)message;
        //UnbindResponse response = (UnbindResponse)request.getResultResponse();

        Session session = handler.getPenroseSession(ioSession);

        org.safehaus.penrose.session.UnbindRequest penroseRequest = new org.safehaus.penrose.session.UnbindRequest();
        handler.getControls(request, penroseRequest);

        org.safehaus.penrose.session.UnbindResponse penroseResponse = new org.safehaus.penrose.session.UnbindResponse();

        session.unbind(penroseRequest, penroseResponse);

        //handler.setControls(penroseResponse, response);
    }
}
