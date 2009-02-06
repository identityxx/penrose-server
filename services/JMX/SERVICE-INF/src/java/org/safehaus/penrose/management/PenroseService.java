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

import org.apache.log4j.Logger;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.management.PenroseServiceMBean;
import org.safehaus.penrose.PenroseConfig;
import org.safehaus.penrose.session.SessionManager;
import org.safehaus.penrose.client.PenroseClient;
import org.safehaus.penrose.ldap.DN;
import org.safehaus.penrose.management.partition.PartitionManagerService;
import org.safehaus.penrose.management.schema.SchemaManagerService;
import org.safehaus.penrose.management.service.ServiceManagerService;
import org.safehaus.penrose.management.session.SessionManagerService;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.schema.SchemaManager;
import org.safehaus.penrose.server.PenroseServer;
import org.safehaus.penrose.service.ServiceManager;
import org.safehaus.penrose.user.UserConfig;

import javax.management.StandardMBean;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class PenroseService extends StandardMBean implements PenroseServiceMBean {

    public Logger log = Logger.getLogger(getClass());

    protected PenroseJMXService jmxService;
    protected PenroseServer penroseServer;

    protected SchemaManagerService schemaManagerService;
    protected PartitionManagerService partitionManagerService;
    protected ServiceManagerService serviceManagerService;
    protected SessionManagerService sessionManagerService;

    public PenroseService(PenroseJMXService jmxService, PenroseServer penroseServer) throws Exception {
        super(PenroseServiceMBean.class);
        
        this.jmxService = jmxService;
        this.penroseServer = penroseServer;
    }

    public void init() throws Exception {

        jmxService.register(getObjectName(), this);

        Penrose penrose = penroseServer.getPenrose();

        SchemaManager schemaManager = penrose.getSchemaManager();

        schemaManagerService = new SchemaManagerService(jmxService, schemaManager);
        schemaManagerService.init();
        schemaManagerService.register();

        PartitionManager partitionManager = penrose.getPartitionManager();

        partitionManagerService = new PartitionManagerService(jmxService, partitionManager);
        partitionManagerService.init();
        partitionManagerService.register();

        ServiceManager serviceManager = penroseServer.getServiceManager();

        serviceManagerService = new ServiceManagerService(jmxService, serviceManager);
        serviceManagerService.init();
        serviceManagerService.register();

        SessionManager sessionManager = penrose.getSessionContext().getSessionManager();

        sessionManagerService = new SessionManagerService(jmxService, sessionManager);
        sessionManagerService.init();
        sessionManagerService.register();
    }

    public void destroy() throws Exception {

        sessionManagerService.unregister();
        serviceManagerService.unregister();
        partitionManagerService.unregister();
        schemaManagerService.unregister();

        jmxService.unregister(getObjectName());
    }

    public PenroseService(String home) throws Exception {
        super(PenroseServiceMBean.class);
        penroseServer = new PenroseServer(home);
    }

    public String getProductVendor() {
        return penroseServer.getProductVendor();
    }

    public String getProductName() {
        return penroseServer.getProductName();
    }

    public String getProductVersion() {
        return penroseServer.getProductVersion();
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

    public DN getRootDn() throws Exception {
        Penrose penrose = penroseServer.getPenrose();
        return penrose.getPenroseConfig().getRootDn();
    }

    public void setRootDn(DN dn) throws Exception {
        Penrose penrose = penroseServer.getPenrose();
        penrose.getPenroseConfig().setRootDn(dn);
        penrose.store();
    }

    public byte[] getRootPassword() throws Exception {
        Penrose penrose = penroseServer.getPenrose();
        return penrose.getPenroseConfig().getRootPassword();
    }

    public void setRootPassword(byte[] password) throws Exception {
        Penrose penrose = penroseServer.getPenrose();
        penrose.getPenroseConfig().setRootPassword(password);
        penrose.store();
    }

    public UserConfig getRootUserConfig() throws Exception {
        Penrose penrose = penroseServer.getPenrose();
        return penrose.getPenroseConfig().getRootUserConfig();
    }

    public void setRootUserConfig(UserConfig userConfig) throws Exception {
        Penrose penrose = penroseServer.getPenrose();
        penrose.getPenroseConfig().setRootUserConfig(userConfig);
        penrose.store();
    }

    public PenroseConfig getPenroseConfig() throws Exception {
        Penrose penrose = penroseServer.getPenrose();
        return penrose.getPenroseConfig();
    }

    public void setPenroseConfig(PenroseConfig penroseConfig) throws Exception {
        Penrose penrose = penroseServer.getPenrose();
        penrose.setPenroseConfig(penroseConfig);
        penrose.store();
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Schemas
    ////////////////////////////////////////////////////////////////////////////////

    public SchemaManagerService getSchemaManagerService() throws Exception {
        return schemaManagerService;
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Partitions
    ////////////////////////////////////////////////////////////////////////////////

    public PartitionManagerService getPartitionManagerService() throws Exception {
        return partitionManagerService;
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Services
    ////////////////////////////////////////////////////////////////////////////////

    public ServiceManagerService getServiceManagerService() throws Exception {
        return serviceManagerService;
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Sessions
    ////////////////////////////////////////////////////////////////////////////////

    public SessionManagerService getSessionManagerService() throws Exception {
        return sessionManagerService;
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Files
    ////////////////////////////////////////////////////////////////////////////////

    public Collection<String> listFiles(String path) throws Exception {
        try {
            Collection<String> results = new ArrayList<String>();

            File home = penroseServer.getHome();
            File dir = new File(home, path);
            if (!dir.exists()) return results;

            results.add(path+"/");
            
            File files[] = dir.listFiles();
            if (files == null) return results;

            for (File file : files) {
                String p = path+"/"+file.getName(); 
                if (file.isDirectory()) {
                    results.add(p+"/");
                    results.addAll(listFiles(p));
                } else {
                    results.add(p);
                }
            }
            return results;
            
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        }
    }

    public void createDirectory(String path) throws Exception {
        log.debug("Creating directory "+path+".");
        try {
            File home = penroseServer.getHome();
            File dir = new File(home, path);
            dir.mkdirs();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        }
    }

    public void removeDirectory(String path) throws Exception {
        log.debug("Removing directory "+path+".");
        try {
            File home = penroseServer.getHome();
            delete(new File(home, path));

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        }
    }

    public void delete(File dir) {
        File files[] = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                delete(file);
            }
        }
        dir.delete();
    }

    public byte[] download(String filename) throws Exception {
        log.debug("Sending file "+filename+".");
        try {
            File home = penroseServer.getHome();
            File file = new File(home, filename);
            if (!file.exists()) return null;

            int length = (int)file.length();
            FileInputStream in = new FileInputStream(file);
            byte content[] = new byte[length];
            int len = in.read(content);
            in.close();

            if (length != len) throw new Exception("Unable to read "+filename+".");

            return content;

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        }
    }

    public void upload(String filename, byte content[]) throws Exception {
        log.debug("Receiving file "+filename+".");
        try {
            File home = penroseServer.getHome();
            File file = new File(home, filename);
            file.getParentFile().mkdirs();

            FileOutputStream out = new FileOutputStream(file);
            out.write(content);
            out.close();
            
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        }
    }

    public PenroseServer getPenroseServer() {
        return penroseServer;
    }

    public void setPenroseServer(PenroseServer penroseServer) {
        this.penroseServer = penroseServer;
    }

    public String getObjectName() {
        return PenroseClient.getObjectName();
    }

    public PenroseJMXService getJmxService() {
        return jmxService;
    }

    public void setJmxService(PenroseJMXService jmxService) {
        this.jmxService = jmxService;
    }
}
