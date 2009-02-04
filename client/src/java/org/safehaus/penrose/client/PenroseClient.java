/**
 * Copyright (c) 2000-2006, Identyx Corporation.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.safehaus.penrose.client;

import org.apache.log4j.Logger;
import org.safehaus.penrose.PenroseConfig;
import org.safehaus.penrose.ldap.DN;
import org.safehaus.penrose.partition.PartitionManagerClient;
import org.safehaus.penrose.schema.SchemaManagerClient;
import org.safehaus.penrose.service.ServiceManagerClient;
import org.safehaus.penrose.user.UserConfig;
import org.safehaus.penrose.management.PenroseServiceMBean;
import org.safehaus.penrose.session.SessionManagerClient;

import javax.management.Attribute;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.naming.Context;
import javax.naming.InitialContext;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Collection;
import java.util.Hashtable;

public class PenroseClient implements PenroseServiceMBean {

    public static Logger log = Logger.getLogger(PenroseClient.class);

    public final static String PENROSE = "PENROSE";
    public final static String JBOSS   = "JBOSS";

    public final static int DEFAULT_RMI_PORT            = 1099;
    public final static int DEFAULT_RMI_TRANSPORT_PORT  = 0;
    public final static int DEFAULT_HTTP_PORT           = 8112;
    public final static String DEFAULT_PROTOCOL         = "rmi";

	public String url;
    public String type;
    private String protocol      = DEFAULT_PROTOCOL;
    public String host;
    public int port              = DEFAULT_RMI_PORT;
    private int rmiTransportPort = DEFAULT_RMI_TRANSPORT_PORT;

    public String bindDn;
    public String bindPassword;

    public Context context;
	public JMXConnector connector;

	private MBeanServerConnection connection;
	public String domain;
	public ObjectName objectName;

    public PenroseClient(String host, int port, String bindDn, String bindPassword) throws Exception {
        this(PENROSE, host, port, bindDn, bindPassword);
    }

	public PenroseClient(String type, String host, int port, String bindDn, String bindPassword) throws Exception {
        this.type = type;
        this.host = host;
        this.port = port;

		this.bindDn = bindDn;
		this.bindPassword = bindPassword;
	}

    public PenroseClient(String type, String protocol, String host, int port, String bindDn, String bindPassword) throws Exception {
        this.type = type;
        this.protocol = protocol;
        this.host = host;
        this.port = port;

        this.bindDn = bindDn;
        this.bindPassword = bindPassword;
    }

	public void connect() throws Exception {

        if (JBOSS.equals(type)) {

            String url = "jnp://"+host+":"+port;
            log.debug("Connecting to JBoss server at "+url);

            Hashtable<String,Object> parameters = new Hashtable<String,Object>();
            parameters.put(Context.INITIAL_CONTEXT_FACTORY, "org.jnp.interfaces.NamingContextFactory");
            parameters.put(Context.PROVIDER_URL, url);
            parameters.put(Context.URL_PKG_PREFIXES, "org.jboss.naming:org.jnp.interfaces");
            parameters.put(Context.SECURITY_PRINCIPAL, bindDn);
            parameters.put(Context.SECURITY_CREDENTIALS, bindPassword);

            context = new InitialContext(parameters);
            connection = (MBeanServerConnection)context.lookup("jmx/invoker/RMIAdaptor");

        } else {

            String url = "service:jmx:"+protocol+"://"+host;
            if (rmiTransportPort != DEFAULT_RMI_TRANSPORT_PORT) url += ":"+rmiTransportPort;

            url += "/jndi/"+protocol+"://"+host;
            if (port != DEFAULT_RMI_PORT) url += ":"+port;

            url += "/penrose";
            //String url = "service:jmx:rmi://localhost:rmiTransportProtocol/jndi/rmi://localhost:rmiProtocol/penrose";

            log.debug("Connecting to Penrose Server.");
            log.debug("URL: "+url);

            JMXServiceURL serviceURL = new JMXServiceURL(url);

            Hashtable<String,Object> parameters = new Hashtable<String,Object>();

            if (bindDn != null && bindPassword != null) {
                log.debug("Bind DN: "+bindDn);
                log.debug("Bind password: "+bindPassword);

                String[] credentials = new String[] {
                        bindDn,
                        bindPassword
                };

                parameters.put(JMXConnector.CREDENTIALS, credentials);
            }

            connector = JMXConnectorFactory.connect(serviceURL, parameters);
            connection = connector.getMBeanServerConnection();
        }

		domain = connection.getDefaultDomain();
		objectName = ObjectName.getInstance(getObjectName());
	}

    public void close() {
        try {
            if (JBOSS.equals(type)) {
                if (context == null) return;
                context.close();
            } else {
                if (connector == null) return;
                connector.close();
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public int getRmiTransportPort() {
        return rmiTransportPort;
    }

    public void setRmiTransportPort(int rmiTransportPort) {
        this.rmiTransportPort = rmiTransportPort;
    }

    public Object invoke(String method, Object[] paramValues, String[] paramClassNames) throws Exception {

        log.debug("Invoking method "+method+"() on "+ objectName +".");

        return connection.invoke(objectName, method, paramValues, paramClassNames);
    }

    public Object getAttribute(String attributeName) throws Exception {

        log.debug("Getting attribute "+ attributeName +" from "+objectName+".");

        return connection.getAttribute(objectName, attributeName);
    }

    public void setAttribute(String attributeName, Object attributeValue) throws Exception {

        log.debug("Setting attribute "+ attributeName +" from "+objectName+".");

        Attribute attribute = new Attribute(attributeName, attributeValue);
        connection.setAttribute(objectName, attribute);
    }

    public String getProductVendor() throws Exception {
        return (String)getAttribute("ProductVendor");
    }

    public String getProductName() throws Exception {
        return (String)getAttribute("ProductName");
    }

    public String getProductVersion() throws Exception {
        return (String)getAttribute("ProductVersion");
    }

    public String getHome() throws Exception {
        return (String)getAttribute("Home");
    }
    
    public void start() throws Exception {
        invoke("start",
                new Object[] { },
                new String[] { }
        );
    }

    public void stop() throws Exception {
        invoke("stop",
                new Object[] { },
                new String[] { }
        );
    }

    public void reload() throws Exception {
        invoke("reload",
                new Object[] { },
                new String[] { }
        );
    }

    public void store() throws Exception {
        invoke("store",
                new Object[] { },
                new String[] { }
        );
    }

    public void renameEntryConfig(String oldDn, String newDn) throws Exception {
        invoke("renameEntryConfig",
                new Object[] { oldDn, newDn },
                new String[] { String.class.getName(), String.class.getName() }
        );
    }

    public void restart() throws Exception {
        invoke("restart",
                new Object[] { },
                new String[] { }
        );
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Schemas
    ////////////////////////////////////////////////////////////////////////////////

    public SchemaManagerClient getSchemaManagerClient() throws Exception {
        return new SchemaManagerClient(this);
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Partitions
    ////////////////////////////////////////////////////////////////////////////////

    public PartitionManagerClient getPartitionManagerClient() throws Exception {
        return new PartitionManagerClient(this);
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Sessions
    ////////////////////////////////////////////////////////////////////////////////

    public SessionManagerClient getSessionManagerClient() throws Exception {
        return new SessionManagerClient(this);
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Services
    ////////////////////////////////////////////////////////////////////////////////

    public ServiceManagerClient getServiceManagerClient() throws Exception {
        return new ServiceManagerClient(this);
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Files
    ////////////////////////////////////////////////////////////////////////////////

    public Collection<String> listFiles(String directory) throws Exception {
        return (Collection<String>)invoke("listFiles",
                new Object[] { directory },
                new String[] { String.class.getName() }
        );
    }

    public void createDirectory(String path) throws Exception {

        log.debug("Creating remote directory "+path+".");

        invoke("createDirectory",
                new Object[] { path },
                new String[] { String.class.getName() }
        );
    }

    public void removeDirectory(String path) throws Exception {

        log.debug("Removing remote directory "+path+".");

        invoke("removeDirectory",
                new Object[] { path },
                new String[] { String.class.getName() }
        );
    }

    public void download(File localDir, String path) throws Exception {

        log.debug("Downloading "+path+" into "+localDir+".");

        for (String filename : listFiles(path)) {
            File file = new File(localDir, filename);

            if (filename.endsWith("/")) {
                file.mkdirs();
            } else {
                downloadFile(file, filename);
            }
        }
    }

    public void downloadFile(File file, String path) throws Exception {

        byte content[] = download(path);
        if (content == null) return;

        file.getParentFile().mkdirs();

        FileOutputStream out = new FileOutputStream(file);
		out.write(content);
		out.close();
	}

    public byte[] download(String filename) throws Exception {

        log.debug("Downloading file "+filename+".");

        return (byte[])invoke("download",
                new Object[] { filename },
                new String[] { String.class.getName() }
        );
    }

    public void upload(File dir) throws Exception {
        upload(dir, null);
    }

    public void upload(File file, String path) throws Exception {
        if (path != null) file = new File(file, path);

        if (file.isDirectory()) {
            uploadFolder(file, path);
        } else {
            uploadFile(file, path);
        }
    }

    public void uploadFolder(File dir, String path) throws Exception {

        log.debug("Uploading directory "+dir.getAbsolutePath()+" to "+path+".");

        createDirectory(path);
        
        for (File file : dir.listFiles()) {
            String newPath = (path == null ? "" : path + "/") + file.getName();

            if (file.isDirectory()) {
                uploadFolder(file, newPath);

            } else {
                uploadFile(file, newPath);
            }
        }
    }

    public void uploadFile(File file, String path) throws Exception {
        FileInputStream in = new FileInputStream(file);
        byte content[] = new byte[(int)file.length()];
        in.read(content);
        in.close();

        upload(path, content);
    }

    public void upload(String filename, byte content[]) throws Exception {

        log.debug("Uploading file "+filename+".");

        invoke("upload",
                new Object[] { filename, content },
                new String[] { String.class.getName(), "[B" }
        );
    }

    public MBeanServerConnection getConnection() {
        return connection;
    }

    public void setConnection(MBeanServerConnection connection) {
        this.connection = connection;
    }

    public static String getObjectName() {
        return "Penrose:service=Penrose";
    }

    public DN getRootDn() throws Exception {
        return (DN)getAttribute("RootDn");
    }

    public void setRootDn(DN dn) throws Exception {
        setAttribute("RootDn", dn);
    }

    public byte[] getRootPassword() throws Exception {
        return (byte[])getAttribute("RootPassword");
    }

    public void setRootPassword(byte[] password) throws Exception {
        setAttribute("RootPassword", password);
    }

    public UserConfig getRootUserConfig() throws Exception {
        return (UserConfig)getAttribute("RootUserConfig");
    }

    public void setRootUserConfig(UserConfig userConfig) throws Exception {
        setAttribute("RootUserConfig", userConfig);
    }

    public PenroseConfig getPenroseConfig() throws Exception {
        return (PenroseConfig)getAttribute("PenroseConfig");
    }

    public void setPenroseConfig(PenroseConfig penroseConfig) throws Exception {
        setAttribute("PenroseConfig", penroseConfig);
    }
}
