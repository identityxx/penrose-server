package org.safehaus.penrose.mina;

import org.apache.mina.common.IoSession;
import org.apache.mina.common.IoFilterChain;
import org.apache.mina.handler.demux.DemuxingIoHandler;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.ProtocolCodecFactory;
import org.apache.directory.shared.ldap.message.*;
import org.apache.directory.shared.ldap.message.extended.NoticeOfDisconnect;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.session.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class PenroseHandler extends DemuxingIoHandler {

    public Logger log = LoggerFactory.getLogger(getClass());

    public Penrose penrose;
    public ProtocolCodecFactory codecFactory;

    public Map sessions = new HashMap();
    int counter;

    public PenroseHandler(Penrose penrose, ProtocolCodecFactory codecFactory) throws Exception {
        this.setPenrose(penrose);
        this.setCodecFactory(codecFactory);

        BindHandler bindHandler = new BindHandler(this);
        addMessageHandler(BindRequest.class, bindHandler);
        addMessageHandler(BindRequestImpl.class, bindHandler);

        UnbindHandler unbindHandler = new UnbindHandler(this);
        addMessageHandler(UnbindRequest.class, unbindHandler);
        addMessageHandler(UnbindRequestImpl.class, unbindHandler);

        AddHandler addHandler = new AddHandler(this);
        addMessageHandler(AddRequest.class, addHandler);
        addMessageHandler(AddRequestImpl.class, addHandler);

        CompareHandler compareHandler = new CompareHandler(this);
        addMessageHandler(CompareRequest.class, compareHandler);
        addMessageHandler(CompareRequestImpl.class, compareHandler);

        DeleteHandler deleteHandler = new DeleteHandler(this);
        addMessageHandler(DeleteRequest.class, deleteHandler);
        addMessageHandler(DeleteRequestImpl.class, deleteHandler);

        ModifyHandler modifyHandler = new ModifyHandler(this);
        addMessageHandler(ModifyRequest.class, modifyHandler);
        addMessageHandler(ModifyRequestImpl.class, modifyHandler);

        ModifyDnHandler modifyDnHandler = new ModifyDnHandler(this);
        addMessageHandler(ModifyDnRequest.class, modifyDnHandler);
        addMessageHandler(ModifyDnRequestImpl.class, modifyDnHandler);

        SearchHandler searchHandler = new SearchHandler(this);
        addMessageHandler(SearchRequest.class, searchHandler);
        addMessageHandler(SearchRequestImpl.class, searchHandler);
    }

    public void sessionCreated(IoSession session) throws Exception {
        IoFilterChain filters = session.getFilterChain();
        filters.addLast("codec", new ProtocolCodecFilter(getCodecFactory()));

        sessions.put(session, penrose.createSession(""+counter));
        
        if (counter == Integer.MAX_VALUE) {
            counter = 0;
        } else {
            counter++;
        }

        super.sessionCreated(session);
    }

    public void sessionClosed(IoSession ioSession) throws Exception {

        Session session = (Session)sessions.get(ioSession);
        if (session != null) {
            session.close();
        }
        
        super.sessionClosed(ioSession);
    }

    public void exceptionCaught(IoSession session, Throwable cause) throws Exception {

        log.debug(cause.getMessage(), cause);

        if (cause.getCause() instanceof ResponseCarryingMessageException) {
            ResponseCarryingMessageException rcme = (ResponseCarryingMessageException)cause.getCause();
            session.write(rcme.getResponse());
            return;
        }

        session.write(NoticeOfDisconnect.PROTOCOLERROR);
        session.close();

        super.exceptionCaught(session, cause);
    }

    public Penrose getPenrose() {
        return penrose;
    }

    public void setPenrose(Penrose penrose) {
        this.penrose = penrose;
    }

    public ProtocolCodecFactory getCodecFactory() {
        return codecFactory;
    }

    public void setCodecFactory(ProtocolCodecFactory codecFactory) {
        this.codecFactory = codecFactory;
    }

    public Session getPenroseSession(IoSession session) {
        return (Session)sessions.get(session);
    }

    public void getControls(
            Message message,
            org.safehaus.penrose.session.Request request
    ) {
        Map controls = message.getControls();
        Collection list = new ArrayList();
        for (Iterator i=controls.values().iterator(); i.hasNext(); ) {
            Control control = (Control)i.next();

            String oid = control.getID();
            byte[] value = control.getValue();
            boolean critical = control.isCritical();

            org.safehaus.penrose.control.Control ctrl = new org.safehaus.penrose.control.Control(oid, value, critical);
            list.add(ctrl);

        }
        request.setControls(list);
    }

    public void setControls(
            org.safehaus.penrose.session.Response penroseResponse,
            ResultResponse response
    ) {
        Collection controls = penroseResponse.getControls();
        for (Iterator i=controls.iterator(); i.hasNext(); ) {
            org.safehaus.penrose.control.Control control = (org.safehaus.penrose.control.Control)i.next();

            Control ctrl = new ControlImpl() {
                public byte[] getEncodedValue() {
                    return getValue();
                }
            };

            ctrl.setType(control.getOid());
            ctrl.setValue(control.getValue());
            ctrl.setCritical(control.isCritical());

            response.add(ctrl);
        }
    }
}
