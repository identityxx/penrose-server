package org.safehaus.penrose.mina;

import org.apache.mina.common.IoSession;
import org.apache.mina.common.IoFilterChain;
import org.apache.mina.handler.demux.DemuxingIoHandler;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.ProtocolCodecFactory;
import org.apache.directory.shared.ldap.message.*;
import org.apache.directory.shared.ldap.message.AddRequest;
import org.apache.directory.shared.ldap.message.BindRequest;
import org.apache.directory.shared.ldap.message.CompareRequest;
import org.apache.directory.shared.ldap.message.Control;
import org.apache.directory.shared.ldap.message.DeleteRequest;
import org.apache.directory.shared.ldap.message.ModifyRequest;
import org.apache.directory.shared.ldap.message.SearchRequest;
import org.apache.directory.shared.ldap.message.UnbindRequest;
import org.apache.directory.shared.ldap.message.extended.NoticeOfDisconnect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.NamingEnumeration;
import javax.naming.directory.*;
import java.util.*;

import com.identyx.javabackend.*;
import com.identyx.javabackend.Request;
import com.identyx.javabackend.Attribute;
import com.identyx.javabackend.Attributes;
import com.identyx.javabackend.SearchResult;

/**
 * @author Endi S. Dewata
 */
public class PenroseHandler extends DemuxingIoHandler {

    public Logger log = LoggerFactory.getLogger(getClass());

    public Backend backend;
    public ProtocolCodecFactory codecFactory;

    public Map<IoSession,Session> sessions = new HashMap<IoSession,Session>();
    long counter;

    public PenroseHandler(Backend penrose, ProtocolCodecFactory codecFactory) throws Exception {
        this.backend = penrose;
        this.codecFactory = codecFactory;

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

    public void sessionCreated(IoSession ioSession) throws Exception {
        IoFilterChain filters = ioSession.getFilterChain();
        filters.addLast("codec", new ProtocolCodecFilter(getCodecFactory()));

        sessions.put(ioSession, backend.createSession(counter));
        
        if (counter == Long.MAX_VALUE) {
            counter = 0;
        } else {
            counter++;
        }

        super.sessionCreated(ioSession);
    }

    public void sessionClosed(IoSession ioSession) throws Exception {

        Session session = sessions.get(ioSession);
        if (session != null) {
            session.close();
            sessions.remove(ioSession);
        }
        
        super.sessionClosed(ioSession);
    }

    public void exceptionCaught(IoSession ioSession, Throwable cause) throws Exception {

        log.debug(cause.getMessage(), cause);

        if (cause.getCause() instanceof ResponseCarryingMessageException) {
            ResponseCarryingMessageException rcme = (ResponseCarryingMessageException)cause.getCause();
            ioSession.write(rcme.getResponse());
            return;
        }

        ioSession.write(NoticeOfDisconnect.PROTOCOLERROR);
        ioSession.close();
        sessions.remove(ioSession);

        super.exceptionCaught(ioSession, cause);
    }

    public Backend getBackend() {
        return backend;
    }

    public void setBackend(Backend backend) {
        this.backend = backend;
    }

    public ProtocolCodecFactory getCodecFactory() {
        return codecFactory;
    }

    public void setCodecFactory(ProtocolCodecFactory codecFactory) {
        this.codecFactory = codecFactory;
    }

    public Session getPenroseSession(IoSession session) {
        return sessions.get(session);
    }

    public void getControls(
            Message message,
            Request request
    ) throws Exception {
        Map controls = message.getControls();
        Collection<com.identyx.javabackend.Control> list = new ArrayList<com.identyx.javabackend.Control>();
        for (Object o : controls.values()) {
            Control control = (Control) o;

            String oid = control.getID();
            byte[] value = control.getValue();
            boolean critical = control.isCritical();

            com.identyx.javabackend.Control ctrl = backend.createControl(oid, value, critical);
            list.add(ctrl);

        }
        request.setControls(list);
    }

    public void setControls(
            SearchResult result,
            SearchResponseEntry response
    ) throws Exception {
        Collection<com.identyx.javabackend.Control> controls = result.getControls();
        for (com.identyx.javabackend.Control control : controls) {
            Control ctrl = createControl(control);
            response.add(ctrl);
        }
    }

    public void setControls(
            com.identyx.javabackend.Response penroseResponse,
            ResultResponse response
    ) throws Exception {
        Collection<com.identyx.javabackend.Control> controls = penroseResponse.getControls();
        for (com.identyx.javabackend.Control control : controls) {
            Control ctrl = createControl(control);
            response.add(ctrl);
        }
    }

    public Control createControl(com.identyx.javabackend.Control control) throws Exception {
        Control ctrl = new ControlImpl() {
            public byte[] getEncodedValue() {
                return getValue();
            }
        };

        ctrl.setType(control.getOid());
        ctrl.setValue(control.getValue());
        ctrl.setCritical(control.isCritical());

        return ctrl;
    }

    public Collection<Modification> createModifications(Collection<javax.naming.directory.ModificationItem> modificationItems) throws Exception {

        Collection<Modification> modifications = new ArrayList<Modification>();

        for (ModificationItem mi : modificationItems) {
            modifications.add(createModification(mi));
        }

        return modifications;
    }

    public Modification createModification(javax.naming.directory.ModificationItem modificationItem) throws Exception {
        int type = modificationItem.getModificationOp();
        Attribute attribute = createAttribute(modificationItem.getAttribute());
        return backend.createModification(type, attribute);
    }

    public Attributes createAttributes(javax.naming.directory.Attributes attrs) throws Exception {

        Attributes attributes = backend.createAttributes();

        for (NamingEnumeration ne = attrs.getAll(); ne.hasMore(); ) {
            javax.naming.directory.Attribute attr = (javax.naming.directory.Attribute)ne.next();
            Attribute attribute = createAttribute(attr);
            attributes.add(attribute);
        }

        return attributes;
    }

    public Attribute createAttribute(javax.naming.directory.Attribute attr) throws Exception {

        String name = attr.getID();
        Attribute attribute = backend.createAttribute(name);

        for (NamingEnumeration ne2 = attr.getAll(); ne2.hasMore(); ) {
            Object value = ne2.next();
            attribute.addValue(value);
        }

        return attribute;
    }
    
    public javax.naming.directory.Attributes createAttributes(Attributes attributes) throws Exception {

        javax.naming.directory.Attributes attrs = new javax.naming.directory.BasicAttributes();

        for (Attribute attribute : attributes.getAll()) {

            String name = attribute.getName();
            Collection values = attribute.getValues();

            javax.naming.directory.Attribute attr = new BasicAttribute(name);
            for (Object value : values) {
                if (value instanceof byte[]) {
                    attr.add(value);

                } else {
                    attr.add(value.toString());
                }
            }

            attrs.put(attr);
        }

        return attrs;
    }
}
