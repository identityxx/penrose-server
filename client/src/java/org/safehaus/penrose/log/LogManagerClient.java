package org.safehaus.penrose.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.client.PenroseClient;
import org.safehaus.penrose.client.BaseClient;
import org.safehaus.penrose.log.log4j.AppenderConfig;
import org.safehaus.penrose.log.log4j.LoggerConfig;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class LogManagerClient extends BaseClient implements LogManagerServiceMBean {

    public static Logger log = LoggerFactory.getLogger(LogManagerClient.class);

    public LogManagerClient(PenroseClient client) throws Exception {
        super(client, "LogManager", getStringObjectName());
    }

    public static String getStringObjectName() {
        return "Penrose:name=LogManager";
    }

    public Collection<AppenderConfig> getAppenderConfigs() throws Exception {
        return (Collection<AppenderConfig>)getAttribute("AppenderConfigs");
    }

    public Collection<String> getAppenderConfigNames() throws Exception {
        return (Collection<String>)getAttribute("AppenderConfigNames");
    }

    public AppenderConfig getAppenderConfig(String appenderName) throws Exception {
        return (AppenderConfig)invoke("getAppenderConfig", new Object[] { appenderName }, new String[] { String.class.getName() });
    }

    public void addAppenderConfig(AppenderConfig appenderConfig) throws Exception {
        invoke("addAppenderConfig", new Object[] { appenderConfig }, new String[] { AppenderConfig.class.getName() });
    }

    public void updateAppenderConfig(String appenderName, AppenderConfig appenderConfig) throws Exception {
        invoke(
                "updateAppenderConfig",
                new Object[] { appenderName, appenderConfig },
                new String[] { String.class.getName(), AppenderConfig.class.getName() }
        );
    }

    public AppenderConfig removeAppenderConfig(String appenderName) throws Exception {
        return (AppenderConfig)invoke("removeAppenderConfig", new Object[] { appenderName }, new String[] { String.class.getName() });
    }

    public Collection<LoggerConfig> getLoggerConfigs() throws Exception {
        return (Collection<LoggerConfig>)getAttribute("LoggerConfigs");
    }

    public Collection<String> getLoggerConfigNames() throws Exception {
        return (Collection<String>)getAttribute("LoggerConfigNames");
    }

    public LoggerConfig getLoggerConfig(String loggerName) throws Exception {
        return (LoggerConfig)invoke("getLoggerConfig", new Object[] { loggerName }, new String[] { String.class.getName() });
    }

    public void addLoggerConfig(LoggerConfig loggerConfig) throws Exception {
        invoke("addLoggerConfig", new Object[] { loggerConfig }, new String[] { LoggerConfig.class.getName() });
    }

    public void updateLoggerConfig(String loggerName, LoggerConfig loggerConfig) throws Exception {
        invoke(
                "updateLoggerConfig",
                new Object[] { loggerName, loggerConfig },
                new String[] { String.class.getName(), LoggerConfig.class.getName() }
        );
    }

    public LoggerConfig removeLoggerConfig(String loggerName) throws Exception {
        return (LoggerConfig)invoke("removeLoggerConfig", new Object[] { loggerName }, new String[] { String.class.getName() });
    }

}
