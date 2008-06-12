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
package org.safehaus.penrose.jboss;

import org.safehaus.penrose.server.PenroseServer;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.service.ServiceManager;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.ArrayList;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

/**
 * @author Endi S. Dewata
 */
public class PenroseService implements PenroseServiceMBean {

    public Logger log = LoggerFactory.getLogger(getClass());

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
        
        return penroseServer.getHome().getAbsolutePath();
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

    public void destroy() {
    }

    public Collection<String> getServiceNames() throws Exception {
        Collection<String> serviceNames = new ArrayList<String>();
        ServiceManager serviceManager = penroseServer.getServiceManager();
        serviceNames.addAll(serviceManager.getServiceNames());
        return serviceNames;
    }

    public Collection<String> listFiles(String path) throws Exception {
        Collection<String> results = new ArrayList<String>();

        File home = penroseServer.getHome();
        File file = new File(home, path);
        if (!file.exists()) return results;

        File files[] = file.listFiles();
        if (files == null) return results;

        for (File child : files) {
            String p = path+File.separator+child.getName();
            if (child.isDirectory()) {
                results.addAll(listFiles(p));
            } else {
                results.add(p);
            }
        }

        return results;
    }

    public byte[] download(String filename) throws Exception {
        File home = penroseServer.getHome();
        File file = new File(home, filename);
        if (!file.exists()) return null;

        log.debug("Downloading "+file);

        FileInputStream in = new FileInputStream(file);
        byte content[] = new byte[(int)file.length()];
        in.read(content);
        in.close();

        return content;
    }

    public void upload(String filename, byte content[]) throws Exception {
        File home = penroseServer.getHome();
        File file = new File(home, filename);
        file.getParentFile().mkdirs();

        log.debug("Uploading "+file);

        FileOutputStream out = new FileOutputStream(file);
        out.write(content);
        out.close();
    }
}