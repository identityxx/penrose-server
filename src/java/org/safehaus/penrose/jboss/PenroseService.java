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
package org.safehaus.penrose.jboss;

import org.apache.log4j.Logger;
import org.safehaus.penrose.PenroseServer;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.service.ServiceManager;
import org.safehaus.penrose.config.PenroseConfig;

import java.util.Collection;
import java.util.ArrayList;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

/**
 * @author Endi S. Dewata
 */
public class PenroseService implements PenroseServiceMBean {

    Logger log = Logger.getLogger(getClass());

    private String home;
    PenroseServer penroseServer;

    public String getProductName() throws Exception {
        return Penrose.PRODUCT_NAME;
    }

    public String getProductVersion() throws Exception {
        return Penrose.PRODUCT_VERSION;
    }

    public String getHome() throws Exception {
        if (penroseServer == null) return home;
        
        return penroseServer.getPenroseConfig().getHome();
    }

    public void setHome(String home) throws Exception {
        this.home = home;

        if (penroseServer != null) {
            PenroseConfig penroseConfig = penroseServer.getPenroseConfig();
            penroseConfig.setHome(home);
            penroseServer.reload();
        }
    }

    public void create() throws Exception {
        penroseServer = new PenroseServer(home);
    }

    public void start() throws Exception {
        penroseServer.start();
    }

    public void stop() {
        try {
            penroseServer.stop();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public void reload() throws Exception {
        penroseServer.reload();
    }

    public void restart() throws Exception {
        penroseServer.stop();
        penroseServer.reload();
        penroseServer.start();
    }

    public void store() throws Exception {
        penroseServer.store();
    }

    public void destroy() {
    }

    public Collection getServiceNames() throws Exception {
        Collection serviceNames = new ArrayList();
        ServiceManager serviceManager = penroseServer.getServiceManager();
        serviceNames.addAll(serviceManager.getServiceNames());
        return serviceNames;
    }

    public void start(String serviceName) throws Exception {
        ServiceManager serviceManager = penroseServer.getServiceManager();
        serviceManager.start(serviceName);
    }

    public void stop(String serviceName) throws Exception {
        ServiceManager serviceManager = penroseServer.getServiceManager();
        serviceManager.stop(serviceName);
    }

    public String getStatus(String serviceName) throws Exception {
        ServiceManager serviceManager = penroseServer.getServiceManager();
        return serviceManager.getStatus(serviceName);
    }

    public Collection listFiles(String directory) throws Exception {
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
    }

    public byte[] download(String filename) throws Exception {
        String homeDirectory = penroseServer.getPenroseConfig().getHome();
        File file = new File((homeDirectory == null ? "" : homeDirectory+File.separator)+filename);
        if (!file.exists()) return null;

        log.debug("Downloading "+file.getAbsolutePath());

        FileInputStream in = new FileInputStream(file);
        byte content[] = new byte[(int)file.length()];
        in.read(content);
        in.close();

        return content;
    }

    public void upload(String filename, byte content[]) throws Exception {
        String homeDirectory = penroseServer.getPenroseConfig().getHome();
        File file = new File((homeDirectory == null ? "" : homeDirectory+File.separator)+filename);
        file.getParentFile().mkdirs();

        log.debug("Uploading "+file.getAbsolutePath());

        FileOutputStream out = new FileOutputStream(file);
        out.write(content);
        out.close();
    }
}