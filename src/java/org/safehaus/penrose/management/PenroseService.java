/**
 * Copyright (c) 2000-2005, Identyx Corporation.
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
import org.safehaus.penrose.PenroseServer;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.service.ServiceManager;

import java.util.Collection;
import java.util.ArrayList;
import java.util.TreeSet;
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
            return penroseServer.getPenroseConfig().getHome();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        }
    }

    public void setHome(String home) throws Exception {
        try {
            PenroseConfig penroseConfig = penroseServer.getPenroseConfig();
            penroseConfig.setHome(home);
            penroseServer.reload();
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

    public void store() throws Exception {
        try {
            penroseServer.store();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        }
    }

    public Collection getServiceNames() throws Exception {
        try {
            Collection serviceNames = new ArrayList();
            ServiceManager serviceManager = penroseServer.getServiceManager();
            serviceNames.addAll(serviceManager.getServiceNames());
            return serviceNames;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        }
    }

    public void start(String serviceName) throws Exception {
        try {
            ServiceManager serviceManager = penroseServer.getServiceManager();
            serviceManager.start(serviceName);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        }
    }

    public void stop(String serviceName) throws Exception {
        try {
            ServiceManager serviceManager = penroseServer.getServiceManager();
            serviceManager.stop(serviceName);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        }
    }

    public String getStatus(String serviceName) throws Exception {
        try {
            ServiceManager serviceManager = penroseServer.getServiceManager();
            return serviceManager.getStatus(serviceName);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        }
    }

    public Collection listFiles(String directory) throws Exception {
        try {
            Collection results = new ArrayList();

            String homeDirectory = penroseServer.getPenroseConfig().getHome();
            File file = new File((homeDirectory == null ? "" : homeDirectory+File.separator)+directory);
            if (!file.exists()) return results;

            File children[] = file.listFiles();
            for (int i=0; i<children.length; i++) {
                if (children[i].isDirectory()) {
                    results.addAll(listFiles(directory+File.separator+children[i].getName()));
                } else {
                    results.add(directory+File.separator+children[i].getName());
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
            String homeDirectory = penroseServer.getPenroseConfig().getHome();
            File file = new File((homeDirectory == null ? "" : homeDirectory+File.separator)+filename);
            if (!file.exists()) return null;

            log.debug("Downloading "+file.getAbsolutePath());

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
            String homeDirectory = penroseServer.getPenroseConfig().getHome();
            File file = new File((homeDirectory == null ? "" : homeDirectory+File.separator)+filename);
            file.getParentFile().mkdirs();

            log.debug("Uploading "+file.getAbsolutePath());

            FileOutputStream out = new FileOutputStream(file);
            out.write(content);
            out.close();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        }
    }

    public Collection getLoggerNames() throws Exception {
        Collection loggerNames = new ArrayList();
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
