package org.safehaus.penrose.ldapbackend.mina;

import org.apache.mina.handler.demux.DemuxingIoHandler;
import org.apache.mina.filter.codec.ProtocolCodecFactory;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.common.IoSession;
import org.apache.mina.common.IoFilterChain;
import org.apache.directory.shared.ldap.message.*;
import org.apache.directory.shared.ldap.message.AddRequest;
import org.apache.directory.shared.ldap.message.BindRequest;
import org.apache.directory.shared.ldap.message.CompareRequest;
import org.apache.directory.shared.ldap.message.Control;
import org.apache.directory.shared.ldap.message.DeleteRequest;
import org.apache.directory.shared.ldap.message.ModifyRequest;
import org.apache.directory.shared.ldap.message.SearchRequest;
import org.apache.directory.shared.ldap.message.UnbindRequest;
import org.apache.directory.shared.ldap.message.AbandonRequest;
import org.apache.directory.shared.ldap.message.extended.NoticeOfDisconnect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.ldapbackend.*;
import org.safehaus.penrose.ldapbackend.Request;
import org.safehaus.penrose.ldapbackend.Response;

import javax.naming.directory.ModificationItem;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.DirContext;
import javax.naming.NamingEnumeration;
import java.util.*;
import java.net.InetSocketAddress;

/**
 * @author Endi S. Dewata
 */
public class MinaHandler extends DemuxingIoHandler {

    public Logger log = LoggerFactory.getLogger(getClass());

    public Backend backend;
    public ProtocolCodecFactory codecFactory;

    long nextConnectionId;
    public Map<IoSession,Long> connectionIds = Collections.synchronizedMap(new HashMap<IoSession,Long>());

    public MinaHandler(Backend backend, ProtocolCodecFactory codecFactory) throws Exception {
        this.backend = backend;
        this.codecFactory = codecFactory;

        AbandonHandler abandonHandler = new AbandonHandler(this);
        addMessageHandler(AbandonRequest.class, abandonHandler);
        addMessageHandler(AbandonRequestImpl.class, abandonHandler);

        AddHandler addHandler = new AddHandler(this);
        addMessageHandler(AddRequest.class, addHandler);
        addMessageHandler(AddRequestImpl.class, addHandler);

        BindHandler bindHandler = new BindHandler(this);
        addMessageHandler(BindRequest.class, bindHandler);
        addMessageHandler(BindRequestImpl.class, bindHandler);

        CompareHandler compareHandler = new CompareHandler(this);
        addMessageHandler(CompareRequest.class, compareHandler);
        addMessageHandler(CompareRequestImpl.class, compareHandler);

        DeleteHandler deleteHandler = new DeleteHandler(this);
        addMessageHandler(DeleteRequest.class, deleteHandler);
        addMessageHandler(DeleteRequestImpl.class, deleteHandler);

        ExtendedHandler extendedHandler = new ExtendedHandler(this);
        addMessageHandler(ExtendedRequest.class, extendedHandler);
        addMessageHandler(ExtendedRequestImpl.class, extendedHandler);

        ModifyHandler modifyHandler = new ModifyHandler(this);
        addMessageHandler(ModifyRequest.class, modifyHandler);
        addMessageHandler(ModifyRequestImpl.class, modifyHandler);

        ModifyDnHandler modifyDnHandler = new ModifyDnHandler(this);
        addMessageHandler(ModifyDnRequest.class, modifyDnHandler);
        addMessageHandler(ModifyDnRequestImpl.class, modifyDnHandler);

        SearchHandler searchHandler = new SearchHandler(this);
        addMessageHandler(SearchRequest.class, searchHandler);
        addMessageHandler(SearchRequestImpl.class, searchHandler);

        UnbindHandler unbindHandler = new UnbindHandler(this);
        addMessageHandler(UnbindRequest.class, unbindHandler);
        addMessageHandler(UnbindRequestImpl.class, unbindHandler);
    }

    public synchronized Long getNextConnectionId() {

        Long connectionId = nextConnectionId;

        if (nextConnectionId == Long.MAX_VALUE) {
            nextConnectionId = 0L;
        } else {
            nextConnectionId++;
        }

        return connectionId;
    }

    public void sessionCreated(IoSession ioSession) throws Exception {
        IoFilterChain filters = ioSession.getFilterChain();
        filters.addLast("codec", new ProtocolCodecFilter(getCodecFactory()));

        Long connectionId = getNextConnectionId();
        connectionIds.put(ioSession, connectionId);

        ConnectRequest request = backend.createConnectRequest();
        request.setConnectionId(connectionId);

        InetSocketAddress remoteAddress = (InetSocketAddress)ioSession.getRemoteAddress();
        request.setClientAddress(remoteAddress.getHostName());

        InetSocketAddress serviceAddress = (InetSocketAddress)ioSession.getServiceAddress();
        request.setServerAddress(serviceAddress.getHostName());
            
        backend.connect(request);

        super.sessionCreated(ioSession);
    }

    public Long getConnectionId(IoSession ioSession) {
        return connectionIds.get(ioSession);
    }

    public Connection getConnection(Long connectionId) throws Exception {
        return backend.getConnection(connectionId);
    }
                  
    public void sessionClosed(IoSession ioSession) throws Exception {

        Long connectionId = connectionIds.remove(ioSession);

        DisconnectRequest request = backend.createDisconnectRequest();
        request.setConnectionId(connectionId);

        backend.disconnect(request);

        super.sessionClosed(ioSession);
    }

    public void exceptionCaught(IoSession ioSession, Throwable cause) throws Exception {

        Long connectionId = getConnectionId(ioSession);

        log.error(cause.getMessage(), cause);

        if (cause.getCause() instanceof ResponseCarryingMessageException) {
            ResponseCarryingMessageException rcme = (ResponseCarryingMessageException)cause.getCause();
            ioSession.write(rcme.getResponse());
            return;
        }

        ioSession.write(NoticeOfDisconnect.PROTOCOLERROR);
        ioSession.close();

        DisconnectRequest request = backend.createDisconnectRequest();
        request.setConnectionId(connectionId);

        backend.disconnect(request);

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

    public void getControls(
            Message message,
            Request request
    ) throws Exception {
        Map controls = message.getControls();
        Collection<org.safehaus.penrose.ldapbackend.Control> list = new ArrayList<org.safehaus.penrose.ldapbackend.Control>();
        for (Object o : controls.values()) {
            Control control = (Control) o;

            String oid = control.getID();
            byte[] value = control.getValue();
            boolean critical = control.isCritical();

            org.safehaus.penrose.ldapbackend.Control ctrl = backend.createControl(oid, value, critical);
            list.add(ctrl);

        }
        request.setControls(list);
    }

    public void setControls(
            SearchResult result,
            SearchResponseEntry response
    ) throws Exception {
        Collection<org.safehaus.penrose.ldapbackend.Control> controls = result.getControls();
        for (org.safehaus.penrose.ldapbackend.Control control : controls) {
            Control ctrl = createControl(control);
            response.add(ctrl);
        }
    }

    public void setControls(
            Response backendResponse,
            ResultResponse response
    ) throws Exception {
        Collection<org.safehaus.penrose.ldapbackend.Control> controls = backendResponse.getControls();
        for (org.safehaus.penrose.ldapbackend.Control control : controls) {
            Control ctrl = createControl(control);
            response.add(ctrl);
        }
    }

    public Control createControl(org.safehaus.penrose.ldapbackend.Control control) throws Exception {
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

    public Modification createModification(ModificationItem modificationItem) throws Exception {
        int modOp = modificationItem.getModificationOp();

        int type;
        switch (modOp) {
            case DirContext.ADD_ATTRIBUTE:
                type = Modification.ADD;
                break;
            case DirContext.REMOVE_ATTRIBUTE:
                type = Modification.DELETE;
                break;
            default:
                type = Modification.REPLACE;
                break;
        }

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
