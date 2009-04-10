/**
 * Copyright 2009 Red Hat, Inc.
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
package org.safehaus.penrose.ldapbackend.opends;

import org.safehaus.penrose.ldapbackend.Backend;
import org.safehaus.penrose.ldapbackend.ConnectRequest;
import org.safehaus.penrose.ldapbackend.Connection;
import org.opends.messages.Message;
import org.opends.server.admin.std.server.PluginCfg;
import org.opends.server.api.ClientConnection;
import org.opends.server.api.plugin.*;
import org.opends.server.config.ConfigEntry;
import org.opends.server.config.ConfigException;
import org.opends.server.core.DirectoryServer;
import org.opends.server.interop.LazyDN;
import org.opends.server.protocols.asn1.ASN1OctetString;
import org.opends.server.types.*;
import org.opends.server.types.Attribute;
import org.opends.server.types.Control;
import org.opends.server.types.DN;
import org.opends.server.types.operation.*;
import org.safehaus.penrose.ldapbackend.*;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class LDAPBackendPlugin extends DirectoryServerPlugin<PluginCfg> {

    public final static boolean debug = false;

    public String className;
    public Collection<String> classpaths = new ArrayList<String>();
    public Collection<String> libpaths   = new ArrayList<String>();
    public Map<String,String> properties = new LinkedHashMap<String,String>();
    public Collection<String> suffixes   = new ArrayList<String>();

    public Backend backend;

    public AbandonHandler  abandonHandler;
    public AddHandler      addHandler;
    public BindHandler     bindHandler;
    public CompareHandler  compareHandler;
    public DeleteHandler   deleteHandler;
    public ExtendedHandler extendedHandler;
    public ModifyHandler   modifyHandler;
    public ModifyDNHandler modifyDNHandler;
    public SearchHandler   searchHandler;
    public UnbindHandler   unbindHandler;

    //public Map<Long, Session> sessions = new HashMap<Long,Session>();

    public void log(String method, String message) {
        System.out.println("["+method+"] "+message);
    }

    public void log(String method, Throwable exception) {
        System.out.println("["+method+"] "+exception.getClass().getName()+": "+exception.getMessage());
        exception.printStackTrace();
    }

    public void initializePlugin(
            Set<PluginType> pluginTypes,
            PluginCfg configuration
    ) throws ConfigException, InitializationException {

        ConfigEntry configEntry = DirectoryServer.getConfigEntry(configuration.dn());

        Entry entry = configEntry.getEntry();
        if (debug) log("init", "Initializing plugin "+entry.getDN()+".");

        try {
            parseConfiguration(configEntry);
        } catch (Exception e) {
            log("init", e);
            throw new ConfigException(Message.raw(e.getMessage()));
        }

        try {
            initBackend();
        } catch (Throwable e) {
            log("init", e);
            throw new InitializationException(Message.raw(e.getMessage()));
        }

        abandonHandler  = new AbandonHandler(this);
        addHandler      = new AddHandler(this);
        bindHandler     = new BindHandler(this);
        compareHandler  = new CompareHandler(this);
        deleteHandler   = new DeleteHandler(this);
        modifyHandler   = new ModifyHandler(this);
        modifyDNHandler = new ModifyDNHandler(this);
        searchHandler   = new SearchHandler(this);
        unbindHandler   = new UnbindHandler(this);

        if (debug) log("init", "Plugin initialized.");
    }

    void parseConfiguration(ConfigEntry configEntry) {

        Entry entry = configEntry.getEntry();

        List list = entry.getAttribute("ldap-backend-class");
        if (list != null) {
            Attribute attribute = (Attribute)list.get(0);
            AttributeValue attributeValue = attribute.getValues().iterator().next();
            className = attributeValue.toString();
            if (debug) log("init", "Class name: "+className);
        }

        list = entry.getAttribute("ldap-backend-classpath");
        if (list != null) {
            Attribute attribute = (Attribute)list.get(0);
            if (debug) log("init", "Classpaths ("+attribute.getValues().size()+"):");

            for (AttributeValue av : attribute.getValues()) {
                String classpath = av.toString();
                if (debug) log("init", " - " + classpath);
                classpaths.add(classpath);
            }
        }

        list = entry.getAttribute("ldap-backend-libpath");
        if (list != null) {
            Attribute attribute = (Attribute)list.get(0);
            if (debug) log("init", "Libpaths ("+attribute.getValues().size()+"):");

            for (AttributeValue av : attribute.getValues()) {
                String libpath = av.toString();
                if (debug) log("init", " - " + libpath);
                libpaths.add(libpath);
            }
        }

        list = entry.getAttribute("ldap-backend-property");
        if (list != null) {
            Attribute attribute = (Attribute)list.get(0);
            if (debug) log("init", "Properties ("+attribute.getValues().size()+"):");

            for (AttributeValue av : attribute.getValues()) {
                String property = av.toString();
                int p = property.indexOf("=");
                String name = property.substring(0, p);
                String value = property.substring(p + 1);
                if (debug) log("init", " - " + name + ": " + value);
                properties.put(name, value);
            }
        }

        list = entry.getAttribute("ldap-backend-suffix");
        if (list != null) {
            Attribute attribute = (Attribute)list.get(0);
            if (debug) log("init", "Suffixes ("+attribute.getValues().size()+"):");

            for (AttributeValue av : attribute.getValues()) {
                String suffix = av.toString();
                if (debug) log("init", " - " + suffix);
                suffixes.add(suffix);
            }
        }
    }

    void initBackend() throws Exception {
        if (debug) log("init", "Class Loader:");
        List<URL> urls = new ArrayList<URL>();

        for (String classpath : classpaths) {
            File file = new File(classpath);
            String url = file.toURL().toString()+"/";
            if (debug) log("init", " - "+url);
            urls.add(new URL(url));
        }

        for (String libpath : libpaths) {
            File path = new File(libpath);
            File files[] = path.listFiles();

            for (File file : files) {
                if (!file.getName().endsWith(".jar")) continue;
                URL url = file.toURL();
                if (debug) log("init", " - "+url);
                urls.add(url);
            }
        }

        for (String key : properties.keySet()) {
            String value = properties.get(key);
            System.setProperty(key, value);
        }

        if (className != null) {
            if (debug) log("init", "Loading class "+className);
            URLClassLoader classLoader = new URLClassLoader(urls.toArray(new URL[urls.size()]));
            Class clazz = classLoader.loadClass(className);

            if (debug) log("init", "Initializing backend "+clazz.getName());
            backend = (Backend)clazz.newInstance();
            backend.init();
        }
    }

    public PostConnectPluginResult doPostConnect(ClientConnection clientConnection) {
        long connectionId = clientConnection.getConnectionID();
        String clientAddress = clientConnection.getClientAddress();
        String serverAddress = clientConnection.getServerAddress();
        String protocol = clientConnection.getProtocol();
        
        if (debug) log("connect", "connect("+connectionId+"): "+clientAddress);

        try {
            ConnectRequest request = backend.createConnectRequest();
            request.setConnectionId(connectionId);
            request.setClientAddress(clientAddress);
            request.setServerAddress(serverAddress);
            request.setProtocol(protocol);

            backend.connect(request);

        } catch (Exception e) {
            log("connect", e);
        }

        return PostConnectPluginResult.SUCCESS;
    }

    public Connection getConnection(long connectionId) throws Exception {
        return backend.getConnection(connectionId);
    }

    public PostDisconnectPluginResult doPostDisconnect(
            ClientConnection clientConnection,
            DisconnectReason disconnectReason,
            Message message
    ) {
        long connectionId = clientConnection.getConnectionID();
        if (debug) log("disconnect", "disconnect("+connectionId+")");

        try {
            DisconnectRequest request = backend.createDisconnectRequest();
            request.setConnectionId(connectionId);

            backend.disconnect(request);

        } catch (Exception e) {
            log("connect", e);
        }

        return PostDisconnectPluginResult.SUCCESS;
    }

    public PreParsePluginResult doPreParse(PreParseAbandonOperation operation) {
        return abandonHandler.process(operation);
    }

    public PreParsePluginResult doPreParse(PreParseAddOperation operation) {
        return addHandler.process(operation);
    }

    public PreParsePluginResult doPreParse(PreParseBindOperation operation) {
        return bindHandler.process(operation);
    }

    public PreParsePluginResult doPreParse(PreParseCompareOperation operation) {
        return compareHandler.process(operation);
    }

    public PreParsePluginResult doPreParse(PreParseDeleteOperation operation) {
        return deleteHandler.process(operation);
    }

    public PreParsePluginResult doPreParse(PreParseExtendedOperation operation) {
        return extendedHandler.process(operation);
    }

    public PreParsePluginResult doPreParse(PreParseModifyOperation operation) {
        return modifyHandler.process(operation);
    }

    public PreParsePluginResult doPreParse(PreParseModifyDNOperation operation) {
        return modifyDNHandler.process(operation);
    }

    public PreParsePluginResult doPreParse(PreParseSearchOperation operation) {
        return searchHandler.process(operation);
    }

    public PreParsePluginResult doPreParse(PreParseUnbindOperation operation) {
        return unbindHandler.process(operation);
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public Collection<String> getClasspaths() {
        return classpaths;
    }

    public void setClasspaths(List<String> classpaths) {
        this.classpaths = classpaths;
    }

    public Collection<String> getLibpaths() {
        return libpaths;
    }

    public void setLibpaths(List<String> libpaths) {
        this.libpaths = libpaths;
    }

    public Map<String,String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String,String> properties) {
        this.properties = properties;
    }

    public Collection<String> getSuffixes() {
        return suffixes;
    }

    public void setSuffixes(List<String> suffixes) {
        this.suffixes = suffixes;
    }

    public Backend getBackend() {
        return backend;
    }

    public void setBackend(Backend backend) {
        this.backend = backend;
    }

    public DN createDn(org.safehaus.penrose.ldapbackend.DN oldDn) throws Exception {
        return new LazyDN(oldDn.toString());
    }

    public Entry createEntry(org.safehaus.penrose.ldapbackend.DN oldDn, Attributes attributes) throws Exception {

        DN ldapDn = createDn(oldDn);

        Map<ObjectClass,String> objectClasses                    = new LinkedHashMap<ObjectClass,String>();
        Map<AttributeType,List<Attribute>> userAttributes        = new LinkedHashMap<AttributeType,List<Attribute>>();
        Map<AttributeType,List<Attribute>> operationalAttributes = new LinkedHashMap<AttributeType,List<Attribute>>();

        for (String name : attributes.getNames()) {
            //if (debug) log("createEntry", "Creating attribute " + name);

            org.safehaus.penrose.ldapbackend.Attribute attribute = attributes.get(name);

            if ("objectClass".equalsIgnoreCase(name)) {

                for (Object value : attribute.getValues()) {
                    String ocName = (String) value;
                    ObjectClass oc = DirectoryConfig.getObjectClass(ocName.toLowerCase(), true);
                    objectClasses.put(oc, ocName);
                }

            } else {

                AttributeType at = DirectoryConfig.getAttributeType(name.toLowerCase(), true);

                LinkedHashSet<AttributeValue> values = new LinkedHashSet<AttributeValue>();

                for (Object value : attribute.getValues()) {
                    if (value instanceof byte[]) {
                        ByteString bs = new ASN1OctetString((byte[]) value);
                        AttributeValue av = new AttributeValue(at, bs);
                        values.add(av);

                    } else {
                        AttributeValue av = new AttributeValue(at, value.toString());
                        values.add(av);
                    }
                }

                List<Attribute> attrList = new ArrayList<Attribute>();
                attrList.add(new Attribute(at, at.getNameOrOID(), values));

                if (at.isOperational()) {
                    operationalAttributes.put(at, attrList);

                } else {
                    userAttributes.put(at, attrList);
                }
            }
        }

        return new Entry(ldapDn, objectClasses, userAttributes, operationalAttributes);
    }

    public List<Control> createControls(Collection<org.safehaus.penrose.ldapbackend.Control> controls) throws Exception {
        List<Control> newControls = new ArrayList<Control>();
        for (org.safehaus.penrose.ldapbackend.Control control : controls) {
            Control openDsControl = createControl(control);
            newControls.add(openDsControl);
        }
        return newControls;
    }

    public Control createControl(org.safehaus.penrose.ldapbackend.Control control) throws Exception {

        String oid = control.getOid();
        boolean critical = control.isCritical();
        byte[] value = control.getValue();
        ASN1OctetString os = value == null ? null : new ASN1OctetString(value);

        if (debug) log("response", " - "+oid+": "+critical);

        return new Control(oid, critical, os);
    }
}
