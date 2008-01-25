package org.safehaus.penrose.opends;

import org.safehaus.penrose.backend.PenroseBackend;
import org.safehaus.penrose.ldap.LDAPService;
import org.opends.server.core.DirectoryServer;
import org.opends.server.core.PluginConfigManager;
import org.opends.server.api.ConfigHandler;
import org.opends.server.types.*;
import org.opends.server.config.ConfigEntry;
import org.opends.server.tasks.ShutdownTaskThread;
import org.opends.server.extensions.ConfigFileHandler;
import org.opends.server.util.ServerConstants;
import org.opends.messages.MessageBuilder;

import java.io.File;
import java.util.List;
import java.util.Set;

import com.identyx.javabackend.opends.JavaBackendPlugin;

/**
 * @author Endi S. Dewata
 */
public class OpenDSLDAPService extends LDAPService {

    String configClass = ConfigFileHandler.class.getName();
    File configFile;
    File schemaPath;

    public void init() throws Exception {
        super.init();

        File serviceDir = serviceContext.getPath();
        log.debug("Service path: "+serviceDir);

        configFile = new File(serviceDir, "config"+File.separator+"config.ldif");
        log.debug("Config file: "+configFile);

        schemaPath = new File(serviceDir, "config"+File.separator+"schema");
        log.debug("Schema path: "+schemaPath);

        try {
            File logs = new File(serviceDir, "logs");

            File pidFile = new File(logs, "server.pid");
            if (pidFile.exists()) {
                pidFile.deleteOnExit();
            }

            File startingFile = new File(logs, "server.starting");
            if (startingFile.exists()) {
                startingFile.deleteOnExit();
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

        DirectoryEnvironmentConfig environmentConfig = new DirectoryEnvironmentConfig();
        environmentConfig.setProperty(ServerConstants.PROPERTY_SERVER_ROOT, serviceDir.getAbsolutePath());
        environmentConfig.setProperty(ServerConstants.PROPERTY_CONFIG_CLASS, configClass);
        environmentConfig.setProperty(ServerConstants.PROPERTY_CONFIG_FILE, configFile.getAbsolutePath());
        environmentConfig.setProperty(ServerConstants.PROPERTY_SCHEMA_DIRECTORY, schemaPath.getAbsolutePath());

        DirectoryServer directoryServer = DirectoryServer.getInstance();
        directoryServer.setEnvironmentConfig(environmentConfig);
        directoryServer.bootstrapServer();

        directoryServer.initializeConfiguration(
                configClass,
                configFile.getAbsolutePath()
        );

        directoryServer.startServer();

        PluginConfigManager pluginConfigManager = DirectoryServer.getPluginConfigManager();
        JavaBackendPlugin javaBackend = (JavaBackendPlugin)pluginConfigManager.getRegisteredPlugin(DN.decode("cn=Java Backend,cn=Plugins,cn=config"));

        if (javaBackend != null) {
            javaBackend.setBackend(new PenroseBackend(serviceContext.getPenroseServer()));
        }

        ConfigHandler configHandler = DirectoryServer.getConfigHandler();

        Entry ldapEntry = configHandler.getConfigEntry(DN.decode("cn=LDAP Connection Handler,cn=Connection Handlers,cn=config")).getEntry();

        Attribute ldapEnabledAttribute = ldapEntry.getAttribute("ds-cfg-enabled").get(0);
        String ldapEnabled = ldapEnabledAttribute.getValues().iterator().next().getStringValue();

        if ("true".equals(ldapEnabled)) {
            Attribute ldapPortAttribute = ldapEntry.getAttribute("ds-cfg-listen-port").get(0);
            String ldapPort = ldapPortAttribute.getValues().iterator().next().getStringValue();
            log.warn("Listening to port "+ldapPort+" (LDAP).");
        }

        Entry ldapsEntry = configHandler.getConfigEntry(DN.decode("cn=LDAPS Connection Handler,cn=Connection Handlers,cn=config")).getEntry();

        Attribute ldapsEnabledAttribute = ldapsEntry.getAttribute("ds-cfg-enabled").get(0);
        String ldapsEnabled = ldapsEnabledAttribute.getValues().iterator().next().getStringValue();

        if ("true".equals(ldapsEnabled)) {
            Attribute ldapsPortAttribute = ldapsEntry.getAttribute("ds-cfg-listen-port").get(0);
            String ldapsPort = ldapsPortAttribute.getValues().iterator().next().getStringValue();
            log.warn("Listening to port "+ldapsPort+" (LDAPS).");
        }
    }

    public void destroy() throws Exception {

        ShutdownTaskThread shutdownThread = new ShutdownTaskThread(new MessageBuilder("Shutdown").toMessage());
        shutdownThread.start();

        log.warn("LDAP Service has been shutdown.");
    }
}
