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
package org.safehaus.penrose.logger.log4j;

import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Collection;
import java.io.File;

/**
 * @author Endi S. Dewata
 */
public class Log4jConfig {

    boolean debug;

    Map<String,AppenderConfig> appenderConfigs = new LinkedHashMap<String,AppenderConfig>();
    Map<String,LoggerConfig> loggerConfigs = new LinkedHashMap<String,LoggerConfig>();

    RootConfig rootConfig;

    public Collection<AppenderConfig> getAppenderConfigs() {
        return appenderConfigs.values();
    }

    public Collection<String> getAppenderConfigNames() {
        return appenderConfigs.keySet();
    }

    public void addAppenderConfig(AppenderConfig appenderConfig) {
        appenderConfigs.put(appenderConfig.getName(), appenderConfig);
    }

    public AppenderConfig getAppenderConfig(String name) {
        return appenderConfigs.get(name);
    }

    public AppenderConfig removeAppenderConfig(String name) {
        return appenderConfigs.remove(name);
    }

    public Collection<LoggerConfig> getLoggerConfigs() {
        return loggerConfigs.values();
    }

    public Collection<String> getLoggerConfigNames() {
        return loggerConfigs.keySet();
    }

    public void addLoggerConfig(LoggerConfig loggerConfig) {
        loggerConfigs.put(loggerConfig.getName(), loggerConfig);
    }

    public LoggerConfig getLoggerConfig(String name) {
        return loggerConfigs.get(name);
    }

    public LoggerConfig removeLoggerConfig(String name) {
        return loggerConfigs.remove(name);
    }

    public RootConfig getRootConfig() {
        return rootConfig;
    }

    public void setRootConfig(RootConfig rootConfig) {
        this.rootConfig = rootConfig;
    }

    public boolean isDebug() {
        return debug;
    }

    public void setDebug(boolean debug) {
        this.debug = debug;
    }

    public static void main(String args[]) throws Exception {

        Log4jConfigReader configReader = new Log4jConfigReader(new File(args[0]));
        Log4jConfig config = configReader.read();

        Log4jConfigWriter configWriter = new Log4jConfigWriter();
        configWriter.write(config);
    }
}
