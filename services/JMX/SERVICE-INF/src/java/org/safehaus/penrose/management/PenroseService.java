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
import org.safehaus.penrose.partition.Partitions;
import org.safehaus.penrose.server.PenroseServer;
import org.safehaus.penrose.service.Services;
import org.safehaus.penrose.service.ServiceConfigs;

import javax.management.StandardMBean;
import java.util.Collection;
import java.util.ArrayList;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

/**
 * @author Endi S. Dewata
 */
public class PenroseService extends StandardMBean implements PenroseServiceMBean {

    public Logger log = Logger.getLogger(getClass());

    private PenroseJMXService jmxService;
    private PenroseServer penroseServer;

    public PenroseService(PenroseJMXService jmxService, PenroseServer penroseServer) throws Exception {
        super(PenroseServiceMBean.class);
        
        this.jmxService = jmxService;
        this.penroseServer = penroseServer;
    }

    public PenroseService(String home) throws Exception {
        super(PenroseServiceMBean.class);
        penroseServer = new PenroseServer(home);
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

    ////////////////////////////////////////////////////////////////////////////////
    // Partitions
    ////////////////////////////////////////////////////////////////////////////////

    public Collection<String> getPartitionNames() throws Exception {
        try {
            Collection<String> list = new ArrayList<String>();

            Penrose penrose = penroseServer.getPenrose();
            PartitionConfigs partitionConfigs = penrose.getPartitionConfigs();
            list.addAll(partitionConfigs.getAvailablePartitionNames());

            return list;

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        }
    }

    public PartitionService getPartitionService(String partitionName) throws Exception {
        Penrose penrose = penroseServer.getPenrose();
        Partitions partitions = penrose.getPartitions();
        return getPartitionService(partitions, partitionName);
    }

    public PartitionService getPartitionService(Partitions partitions, String partitionName) throws Exception {

        PartitionService partitionService = new PartitionService();
        partitionService.setPartitions(partitions);
        partitionService.setName(partitionName);
        partitionService.setJmxService(jmxService);

        return partitionService;
    }

    public Collection<PartitionService> getPartitionServices() throws Exception {

        Collection<PartitionService> list = new ArrayList<PartitionService>();

        Penrose penrose = penroseServer.getPenrose();
        PartitionConfigs partitionConfigs = penrose.getPartitionConfigs();
        Partitions partitions = penrose.getPartitions();

        for (String partitionName : partitionConfigs.getAvailablePartitionNames()) {
            list.add(getPartitionService(partitions, partitionName));
        }

        return list;
    }

    public void startPartition(String partitionName) throws Exception {

        log.debug("Starting partition "+partitionName);

        Penrose penrose = penroseServer.getPenrose();
        penrose.startPartition(partitionName);

        PartitionService partitionService = getPartitionService(partitionName);
        partitionService.register();
    }

    public void stopPartition(String partitionName) throws Exception {

        log.debug("Stoppinh partition "+partitionName);

        PartitionService partitionService = getPartitionService(partitionName);
        partitionService.unregister();

        Penrose penrose = penroseServer.getPenrose();
        penrose.stopPartition(partitionName);
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Services
    ////////////////////////////////////////////////////////////////////////////////

    public Collection<String> getServiceNames() throws Exception {
        try {
            Collection<String> list = new ArrayList<String>();

            ServiceConfigs serviceConfigs = penroseServer.getServiceConfigs();
            list.addAll(serviceConfigs.getAvailableServiceNames());

            return list;

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        }
    }

    public ServiceService getServiceService(String serviceName) throws Exception {
        Services services = penroseServer.getServices();
        return getServiceService(services, serviceName);
    }

    public ServiceService getServiceService(
            Services services,
            String serviceName
    ) throws Exception {

        ServiceService serviceService = new ServiceService();
        serviceService.setJmxService(jmxService);
        serviceService.setServices(services);
        serviceService.setName(serviceName);

        return serviceService;
    }

    public Collection<ServiceService> getServiceServices() throws Exception {

        Collection<ServiceService> list = new ArrayList<ServiceService>();

        ServiceConfigs serviceConfigs = penroseServer.getServiceConfigs();
        Services services = penroseServer.getServices();

        for (String serviceName : serviceConfigs.getAvailableServiceNames()) {
            list.add(getServiceService(services, serviceName));
        }

        return list;
    }

    public void startService(String serviceName) throws Exception {
        penroseServer.startService(serviceName);

        ServiceService serviceService = getServiceService(serviceName);
        serviceService.register();
    }

    public void stopService(String serviceName) throws Exception {
        ServiceService serviceService = getServiceService(serviceName);
        serviceService.unregister();
        
        penroseServer.stopService(serviceName);
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

    public void register() throws Exception {
        jmxService.register(getObjectName(), this);

        for (PartitionService partitionService : getPartitionServices()) {
            partitionService.register();
        }

        for (ServiceService serviceService : getServiceServices()) {
            serviceService.register();
        }
    }

    public void unregister() throws Exception {
        for (ServiceService serviceService : getServiceServices()) {
            serviceService.unregister();
        }

        for (PartitionService partitionService : getPartitionServices()) {
            partitionService.unregister();
        }

        jmxService.unregister(getObjectName());
    }
}
