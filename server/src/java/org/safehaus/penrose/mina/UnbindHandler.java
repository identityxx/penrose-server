package org.safehaus.penrose.mina;

import org.apache.mina.handler.demux.MessageHandler;
import org.apache.mina.common.IoSession;
import org.apache.directory.shared.ldap.message.UnbindRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.identyx.javabackend.Session;

/**
 * @author Endi S. Dewata
 */
public class UnbindHandler implements MessageHandler {

    Logger log = LoggerFactory.getLogger(getClass());

    MinaHandler handler;

    public UnbindHandler(MinaHandler handler) {
        this.handler = handler;
    }

    public void messageReceived(IoSession ioSession, Object message) throws Exception {

        UnbindRequest request = (UnbindRequest)message;
        //UnbindResponse response = (UnbindResponse)request.getResultResponse();

        Session session = handler.getPenroseSession(ioSession);

        com.identyx.javabackend.UnbindRequest penroseRequest = handler.backend.createUnbindRequest();
        handler.getControls(request, penroseRequest);

        com.identyx.javabackend.UnbindResponse penroseResponse = handler.backend.createUnbindResponse();

        session.unbind(penroseRequest, penroseResponse);

        //handler.setControls(penroseResponse, response);
    }
}
