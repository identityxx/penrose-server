package org.safehaus.penrose.opends;

import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.backend.PenroseBackend;
import org.safehaus.penrose.ldap.LDAPService;
import org.safehaus.penrose.server.PenroseServer;
import org.opends.server.core.DirectoryServer;
import org.opends.server.core.PluginConfigManager;
import org.opends.server.api.ConfigHandler;
import org.opends.server.types.*;
import org.opends.server.config.ConfigEntry;
import org.opends.server.tasks.ShutdownTaskThread;
import org.opends.server.extensions.ConfigFileHandler;

import java.io.File;
import java.util.List;
import java.util.Set;

import com.identyx.javabackend.opends.JavaBackendPlugin;

/**
 * @author Endi S. Dewata
 */
public class OpenDSLDAPService extends LDAPService {

    public void start() throws Exception {

        PenroseServer penroseServer = getPenroseServer();
        PenroseConfig penroseConfig = penroseServer.getPenroseConfig();
        System.setProperty("org.opends.server.ServerRoot", penroseServer.getHome());
        
        String configClass = ConfigFileHandler.class.getName();
        String configFile = penroseServer.getHome()+File.separator+"config"+File.separator+"config.ldif";

        try {
            String logs = penroseServer.getHome()+File.separator+"logs";
            String pidFilePath = logs+File.separator+"server.pid";
            String startingFilePath = logs+File.separator+"server.starting";

            File pidFile = new File(pidFilePath);
            if (pidFile.exists()) {
                pidFile.deleteOnExit();
            }

            File startingFile = new File(startingFilePath);
            if (startingFile.exists()) {
                startingFile.deleteOnExit();
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

        DirectoryServer directoryServer = DirectoryServer.getInstance();
        directoryServer.bootstrapServer();

        log.debug("Config file: "+configFile);

        directoryServer.initializeConfiguration(
                configClass,
                configFile
        );

        ConfigHandler configHandler = DirectoryServer.getConfigHandler();
        ConfigEntry configEntry = configHandler.getConfigEntry(DN.decode("cn=LDAP Connection Handler,cn=Connection Handlers,cn=config"));

        Entry entry = configEntry.getEntry();

        String attributeName = "ds-cfg-listen-port";
        List attributes = entry.getAttribute(attributeName);
        Attribute attribute = (Attribute)attributes.get(0);

        Set values = attribute.getValues();
        AttributeValue value = (AttributeValue)values.iterator().next();
/*
        values.clear();

        AttributeType attributeType = DirectoryServer.getAttributeType(attributeName, true);
        log.debug("Attribute Type: "+attributeType);

        values.add(new AttributeValue(attributeType, ""+ldapPort));
        log.debug("New LDAP port: "+ldapPort);
*/
        directoryServer.startServer();

        PluginConfigManager pluginConfigManager = DirectoryServer.getPluginConfigManager();
        JavaBackendPlugin javaBackend = (JavaBackendPlugin)pluginConfigManager.getRegisteredPlugin(DN.decode("cn=Java Backend,cn=Plugins,cn=config"));
        javaBackend.setBackend(new PenroseBackend(getPenroseServer()));

        log.warn("Listening to port "+value.getStringValue()+" (LDAP).");
    }

    public void stop() throws Exception {
        ShutdownTaskThread shutdownThread = new ShutdownTaskThread("Shutdown");
        shutdownThread.start();

        log.warn("LDAP Service has been shutdown.");
    }
}
