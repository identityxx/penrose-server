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
package org.safehaus.penrose.management;

import org.apache.log4j.*;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.partition.PartitionConfigs;
import org.safehaus.penrose.server.PenroseServer;
import org.safehaus.penrose.service.Services;
import org.safehaus.penrose.service.ServiceConfigs;

import java.util.Collection;
import java.util.ArrayList;
import java.util.Enumeration;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

/**
 * @author Endi S. Dewata
 */
public class PenroseService implements PenroseServiceMBean {

    Logger log = Logger.getLogger(PenroseService.class);

    private PenroseServer penroseServer;

    public PenroseService(PenroseServer penroseServer) throws Exception {
        this.penroseServer = penroseServer;
    }

    public PenroseService(String home) throws Exception {
        penroseServer = new PenroseServer(home);
    }

    public String getProductName() {
        return Penrose.PRODUCT_NAME;
    }

    public String getProductVersion() {
        return Penrose.PRODUCT_VERSION;
    }

    public String getHome() throws Exception {
        try {
            return penroseServer.getHome().getAbsolutePath();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        }
    }

    public void start() throws Exception {
        try {
            penroseServer.start();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        }
    }

    public void stop() throws Exception {
        try {
            penroseServer.stop();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        }
    }

    public void reload() throws Exception {
        try {
            penroseServer.reload();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        }
    }

    public void restart() throws Exception {
        try {
            penroseServer.stop();
            penroseServer.reload();
            penroseServer.start();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        }
    }

    public Collection<String> getPartitionNames() throws Exception {
        try {
            Collection<String> partitionNames = new ArrayList<String>();
            Penrose penrose = penroseServer.getPenrose();
            PartitionConfigs partitionConfigs = penrose.getPartitionConfigs();
            partitionNames.addAll(partitionConfigs.getPartitionNames());
            return partitionNames;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        }
    }

    public Collection<String> getServiceNames() throws Exception {
        try {
            Collection<String> serviceNames = new ArrayList<String>();
            ServiceConfigs serviceConfigs = penroseServer.getServiceConfigs();
            serviceNames.addAll(serviceConfigs.getServiceNames());
            return serviceNames;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        }
    }

    public void start(String serviceName) throws Exception {
        try {
            Services serviceManager = penroseServer.getServices();
            serviceManager.start(serviceName);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        }
    }

    public void stop(String serviceName) throws Exception {
        try {
            Services serviceManager = penroseServer.getServices();
            serviceManager.stop(serviceName);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        }
    }

    public String getStatus(String serviceName) throws Exception {
        try {
            Services serviceManager = penroseServer.getServices();
            return serviceManager.getStatus(serviceName);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        }
    }

    public Collection<String> listFiles(String directory) throws Exception {
        try {
            Collection<String> results = new ArrayList<String>();

            File home = penroseServer.getHome();
            File file = new File(home, directory);
            if (!file.exists()) return results;

            File children[] = file.listFiles();
            for (File child : children) {
                if (child.isDirectory()) {
                    results.addAll(listFiles(directory + File.separator + child.getName()));
                } else {
                    results.add(directory + File.separator + child.getName());
                }
            }
            return results;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        }
    }

    public byte[] download(String filename) throws Exception {
        try {
            File home = penroseServer.getHome();
            File file = new File(home, filename);
            if (!file.exists()) return null;

            log.debug("Downloading "+file);

            FileInputStream in = new FileInputStream(file);
            byte content[] = new byte[(int)file.length()];
            in.read(content);
            in.close();

            return content;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        }
    }

    public void upload(String filename, byte content[]) throws Exception {
        try {
            File home = penroseServer.getHome();
            File file = new File(home, filename);
            file.getParentFile().mkdirs();

            log.debug("Uploading "+file);

            FileOutputStream out = new FileOutputStream(file);
            out.write(content);
            out.close();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        }
    }

    public Collection<String> getLoggerNames() throws Exception {
        Collection<String> loggerNames = new ArrayList<String>();
        for (Enumeration e = log.getLoggerRepository().getCurrentLoggers(); e.hasMoreElements(); ) {
            Logger logger = (Logger)e.nextElement();
            loggerNames.add(logger.getName());
        }
        return loggerNames;
    }

    public String getLoggerLevel(String name) throws Exception {
        Logger logger = name == null || "".equals(name) ? Logger.getRootLogger() : Logger.getLogger(name);
        Level level = logger.getLevel();
        //log.debug("Logger "+name+": "+level);
        return level == null ? null : level.toString();
    }

    public void setLoggerLevel(String name, String level) throws Exception {
        Logger logger = name == null || "".equals(name) ? Logger.getRootLogger() : Logger.getLogger(name);
        //log.debug("Logger "+name+": "+Level.toLevel(level));
        logger.setLevel(Level.toLevel(level));
    }

    public PenroseServer getPenroseServer() {
        return penroseServer;
    }

    public void setPenroseServer(PenroseServer penroseServer) {
        this.penroseServer = penroseServer;
    }
}
