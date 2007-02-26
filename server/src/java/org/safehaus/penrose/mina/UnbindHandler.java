package org.safehaus.penrose.mina;

import org.apache.mina.handler.demux.MessageHandler;
import org.apache.mina.common.IoSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.session.PenroseSession;

/**
 * @author Endi S. Dewata
 */
public class UnbindHandler implements MessageHandler {

    Logger log = LoggerFactory.getLogger(getClass());

    PenroseHandler handler;

    public UnbindHandler(PenroseHandler handler) {
        this.handler = handler;
    }

    public void messageReceived(IoSession session, Object message) throws Exception {

        PenroseSession penroseSession = handler.getPenroseSession(session);
        penroseSession.unbind();
    }
}
