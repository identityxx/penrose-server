package org.safehaus.penrose.log;

import org.safehaus.penrose.log.log4j.AppenderConfig;
import org.safehaus.penrose.log.log4j.LoggerConfig;
import org.safehaus.penrose.log.log4j.RootLoggerConfig;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public interface LogManagerServiceMBean {

    public Collection<AppenderConfig> getAppenderConfigs() throws Exception;
    public Collection<String> getAppenderNames() throws Exception;
    public AppenderConfig getAppenderConfig(String appenderName) throws Exception;

    public void addAppender(AppenderConfig appenderConfig) throws Exception;
    public void updateAppender(String appenderName, AppenderConfig appenderConfig) throws Exception;
    public void removeAppender(String appenderName) throws Exception;

    public Collection<LoggerConfig> getLoggerConfigs() throws Exception;
    public Collection<String> getLoggerNames() throws Exception;
    public LoggerConfig getLoggerConfig(String loggerName) throws Exception;

    public void addLogger(LoggerConfig loggerConfig) throws Exception;
    public void updateLogger(String loggerName, LoggerConfig loggerConfig) throws Exception;
    public void removeLogger(String loggerName) throws Exception;

    public RootLoggerConfig getRootLoggerConfig() throws Exception;
    public void updateRootLogger(RootLoggerConfig rootLoggerConfig) throws Exception;

    public void store() throws Exception;
}
