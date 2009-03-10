package org.safehaus.penrose.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.client.PenroseClient;
import org.safehaus.penrose.client.BaseClient;
import org.safehaus.penrose.log.log4j.AppenderConfig;
import org.safehaus.penrose.log.log4j.LoggerConfig;
import org.safehaus.penrose.log.log4j.RootLoggerConfig;

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

    public Collection<String> getAppenderNames() throws Exception {
        return (Collection<String>)getAttribute("AppenderNames");
    }

    public AppenderConfig getAppenderConfig(String appenderName) throws Exception {
        return (AppenderConfig)invoke("getAppenderConfig", new Object[] { appenderName }, new String[] { String.class.getName() });
    }

    public void addAppender(AppenderConfig appenderConfig) throws Exception {
        invoke("addAppender", new Object[] { appenderConfig }, new String[] { AppenderConfig.class.getName() });
    }

    public void updateAppender(String appenderName, AppenderConfig appenderConfig) throws Exception {
        invoke(
                "updateAppender",
                new Object[] { appenderName, appenderConfig },
                new String[] { String.class.getName(), AppenderConfig.class.getName() }
        );
    }

    public void removeAppender(String appenderName) throws Exception {
        invoke("removeAppender", new Object[] { appenderName }, new String[] { String.class.getName() });
    }

    public Collection<LoggerConfig> getLoggerConfigs() throws Exception {
        return (Collection<LoggerConfig>)getAttribute("LoggerConfigs");
    }

    public Collection<String> getLoggerNames() throws Exception {
        return (Collection<String>)getAttribute("LoggerNames");
    }

    public LoggerConfig getLoggerConfig(String loggerName) throws Exception {
        return (LoggerConfig)invoke("getLoggerConfig", new Object[] { loggerName }, new String[] { String.class.getName() });
    }

    public void addLogger(LoggerConfig loggerConfig) throws Exception {
        invoke("addLogger", new Object[] { loggerConfig }, new String[] { LoggerConfig.class.getName() });
    }

    public void updateLogger(String loggerName, LoggerConfig loggerConfig) throws Exception {
        invoke(
                "updateLogger",
                new Object[] { loggerName, loggerConfig },
                new String[] { String.class.getName(), LoggerConfig.class.getName() }
        );
    }

    public void removeLogger(String loggerName) throws Exception {
        invoke("removeLogger", new Object[] { loggerName }, new String[] { String.class.getName() });
    }

    public RootLoggerConfig getRootLoggerConfig() throws Exception {
        return (RootLoggerConfig)getAttribute("RootLoggerConfig");
    }

    public void updateRootLogger(RootLoggerConfig rootLoggerConfig) throws Exception {
        invoke(
                "updateRootLogger", 
                new Object[] { rootLoggerConfig },
                new String[] { RootLoggerConfig.class.getName() }
        );
    }

    public void store() throws Exception {
        invoke("store");
    }
}
