package org.safehaus.penrose.log4j;

import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Collection;
import java.io.File;

/**
 * @author Endi S. Dewata
 */
public class Log4jConfig {

    Map appenderConfigs = new LinkedHashMap();
    Map loggerConfigs = new LinkedHashMap();

    RootConfig rootConfig;

    public Collection getAppenderConfigs() {
        return appenderConfigs.values();
    }

    public Collection getAppenderConfigNames() {
        return appenderConfigs.keySet();
    }

    public void addAppenderConfig(AppenderConfig appenderConfig) {
        appenderConfigs.put(appenderConfig.getName(), appenderConfig);
    }

    public AppenderConfig getAppenderConfig(String name) {
        return (AppenderConfig)appenderConfigs.get(name);
    }

    public AppenderConfig removeAppenderConfig(String name) {
        return (AppenderConfig)appenderConfigs.remove(name);
    }

    public Collection getLoggerConfigs() {
        return loggerConfigs.values();
    }

    public Collection getLoggerConfigNames() {
        return loggerConfigs.keySet();
    }

    public void addLoggerConfig(LoggerConfig loggerConfig) {
        loggerConfigs.put(loggerConfig.getName(), loggerConfig);
    }

    public LoggerConfig getLoggerConfig(String name) {
        return (LoggerConfig)loggerConfigs.get(name);
    }

    public LoggerConfig removeLoggerConfig(String name) {
        return (LoggerConfig)loggerConfigs.remove(name);
    }

    public RootConfig getRootConfig() {
        return rootConfig;
    }

    public void setRootConfig(RootConfig rootConfig) {
        this.rootConfig = rootConfig;
    }

    public static void main(String args[]) throws Exception {

        Log4jConfigReader configReader = new Log4jConfigReader(new File(args[0]));
        Log4jConfig config = configReader.read();

        Log4jConfigWriter configWriter = new Log4jConfigWriter();
        configWriter.write(config);
    }
}
