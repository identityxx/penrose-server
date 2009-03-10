package org.safehaus.penrose.ldif.connection;

import com.novell.ldap.LDAPEntry;
import com.novell.ldap.LDAPMessage;
import com.novell.ldap.LDAPSearchResult;
import com.novell.ldap.util.LDIFReader;
import org.safehaus.penrose.connection.Connection;
import org.safehaus.penrose.ldif.LDIFClient;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.session.SessionListener;

import java.io.File;
import java.io.FileInputStream;

/**
 * @author Endi S. Dewata
 */
public class LDIFConnection extends Connection {

    public final static String FILE = "file";

    public File file;

    public void init() throws Exception {

        log.debug("Initializing connection "+getName()+".");

        String s = getParameter(FILE);
        file = new File(s);

        load();
        
        log.debug("Connection "+getName()+" initialized.");
    }

    public void load() throws Exception {

        FileInputStream is = new FileInputStream(file);
        LDIFReader reader = new LDIFReader(is);

        LDAPMessage message;
        while ((message = reader.readMessage()) != null) {
/*
            log.debug("Message ID: "+message.getMessageID());
            log.debug(" - type: "+message.getType());
            log.debug(" - class: "+message.getClass().getName());
            log.debug(" - value: "+message);
*/
            if (!(message instanceof LDAPSearchResult)) continue;

            LDAPSearchResult result = (LDAPSearchResult)message;
            LDAPEntry entry = result.getEntry();

        }

        is.close();
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Client
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public LDIFClient createClient() throws Exception {

        boolean debug = log.isDebugEnabled();
        if (debug) log.debug("Creating new LDIF client.");

        return new LDIFClient(this);
    }

    public LDIFClient getClient(final Session session) throws Exception {

        final boolean debug = log.isDebugEnabled();
        if (debug) log.debug("Getting LDIF client from session.");
        final String attributeName = getPartition().getName()+".connection."+getName();

        LDIFClient client = (LDIFClient)session.getAttribute(attributeName);
        if (client != null) return client;

        final LDIFClient newClient = createClient();

        if (debug) log.debug("Storing LDIF client in session.");
        session.setAttribute(attributeName, newClient);

        session.addListener(new SessionListener() {
            public void sessionClosed() throws Exception {

                if (debug) log.debug("Closing LDIF client.");

                session.removeAttribute(attributeName);
                newClient.close();
            }
        });

        return newClient;
    }

    public void closeClient(Session session) throws Exception {
    }
}