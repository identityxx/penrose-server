package org.safehaus.penrose.mina;

import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.schema.SchemaManager;
import org.safehaus.penrose.schema.AttributeType;
import org.safehaus.penrose.schema.attributeSyntax.AttributeSyntax;
import org.safehaus.penrose.ldap.LDAPService;
import org.apache.mina.transport.socket.nio.SocketAcceptor;
import org.apache.mina.transport.socket.nio.SocketAcceptorConfig;
import org.apache.mina.transport.socket.nio.SocketSessionConfig;
import org.apache.mina.common.ExecutorThreadModel;

import java.net.InetSocketAddress;
import java.util.*;

import edu.emory.mathcs.backport.java.util.concurrent.TimeUnit;
import edu.emory.mathcs.backport.java.util.concurrent.LinkedBlockingQueue;
import edu.emory.mathcs.backport.java.util.concurrent.ThreadPoolExecutor;

/**
 * @author Endi S. Dewata
 */
public class MinaLDAPService extends LDAPService {

    SocketAcceptorConfig acceptorConfig;
    SocketAcceptor acceptor;

    PenroseProtocolCodecFactory codecFactory;
    PenroseHandler handler;

    ExecutorThreadModel threadModel;
    ThreadPoolExecutor threadPoolExecutor;

    public void init() throws Exception {
        super.init();

        Penrose penrose = getPenroseServer().getPenrose();
        PenroseContext penroseContext = penrose.getPenroseContext();

        SchemaManager schemaManager = penroseContext.getSchemaManager();
        Collection attributeTypes = schemaManager.getAttributeTypes();
        Set binaryAttributes = new HashSet();

        log.debug("Binary attributes:");
        for (Iterator i=attributeTypes.iterator(); i.hasNext(); ) {
            AttributeType attributeType = (AttributeType)i.next();

            String name = attributeType.getName();
            //log.debug("Attribute type "+name);

            String syntax = attributeType.getSyntax();
            //log.debug("Attribute syntax: "+syntax);
            if (syntax == null) {
                log.debug("Attribute syntax for "+name+" not defined.");
                continue;
            }

            String oid = syntax;

            int p = oid.indexOf('{');
            if (p >= 0) {
                oid = oid.substring(0, p);
            }
            //log.debug("OID: "+oid);

            AttributeSyntax as = AttributeSyntax.getAttributeSyntax(oid);
            if (as == null) {
                log.debug("Attribute syntax "+oid+" not found.");
                continue;
            }

            if (as.isHumanReadable()) {
                //log.debug("Attribute is not binary.");
                continue;
            }

            log.debug(" - "+name);
            for (Iterator j=attributeType.getNames().iterator(); j.hasNext(); ) {
                name = (String)j.next();
                binaryAttributes.add(name.toLowerCase());
            }
        }

        Hashtable env = new Hashtable();
        env.put("java.naming.ldap.attributes.binary", binaryAttributes);

        codecFactory = new PenroseProtocolCodecFactory(env);
        handler = new PenroseHandler(penrose, codecFactory);

        threadPoolExecutor = new ThreadPoolExecutor(
                maxThreads,
                maxThreads,
                60,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue()
        );

        threadModel = ExecutorThreadModel.getInstance("Mina");
        threadModel.setExecutor(threadPoolExecutor);

        acceptorConfig = new SocketAcceptorConfig();
        acceptorConfig.setDisconnectOnUnbind(false);
        acceptorConfig.setReuseAddress(true);
        acceptorConfig.setThreadModel(threadModel);

        ((SocketSessionConfig)(acceptorConfig.getSessionConfig())).setTcpNoDelay(true);

        acceptor = new SocketAcceptor();
    }

    public void start() throws Exception {
        //acceptor.setLocalAddress();
        //acceptor.setHandler(handler);
        acceptor.bind(new InetSocketAddress(ldapPort), handler, acceptorConfig);

        log.warn("Listening to port "+ldapPort+" (LDAP).");
    }

    public void stop() throws Exception {
        acceptor.unbind(new InetSocketAddress(ldapPort));
    }
}
