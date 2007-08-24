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
import org.safehaus.penrose.partition.Partition;
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

    public Logger log = Logger.getLogger(getClass());

    private PenroseJMXService service;
    private PenroseServer penroseServer;

    public PenroseService(PenroseJMXService service, PenroseServer penroseServer) throws Exception {
        this.service = service;
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

    ////////////////////////////////////////////////////////////////////////////////
    // Partitions
    ////////////////////////////////////////////////////////////////////////////////

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

    public void startPartition(String partitionName) throws Exception {
        try {
            Penrose penrose = penroseServer.getPenrose();
            penrose.startPartition(partitionName);

            PartitionService partitionService = getPartitionService(partitionName);
            partitionService.register();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        }
    }

    public void stopPartition(String partitionName) throws Exception {
        try {
            PartitionService partitionService = getPartitionService(partitionName);
            partitionService.unregister();

            Penrose penrose = penroseServer.getPenrose();
            penrose.stopPartition(partitionName);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        }
    }

    public String getPartitionStatus(String partitionName) throws Exception {
        try {
            Penrose penrose = penroseServer.getPenrose();
            return penrose.getPartitionStatus(partitionName);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        }
    }

    public PartitionService getPartitionService(String partitionName) {
        Penrose penrose = penroseServer.getPenrose();
        Partitions partitions = penrose.getPartitions();
        Partition partition = partitions.getPartition(partitionName);
        if (partition == null) return null;
        
        PartitionService partitionService = new PartitionService();
        partitionService.setService(service);
        partitionService.setPartition(partition);

        return partitionService;
    }

    public Collection<PartitionService> getPartitionServices() {

        Collection<PartitionService> list = new ArrayList<PartitionService>();

        Penrose penrose = penroseServer.getPenrose();
        Partitions partitions = penrose.getPartitions();

        for (Partition partition : partitions.getPartitions()) {
            PartitionService partitionService = new PartitionService();
            partitionService.setService(service);
            partitionService.setPartition(partition);
            list.add(partitionService);
        }

        return list;
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Services
    ////////////////////////////////////////////////////////////////////////////////

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

    public void startService(String serviceName) throws Exception {
        try {
            Services serviceManager = penroseServer.getServices();
            serviceManager.start(serviceName);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        }
    }

    public void stopService(String serviceName) throws Exception {
        try {
            Services serviceManager = penroseServer.getServices();
            serviceManager.stop(serviceName);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        }
    }

    public String getServiceStatus(String serviceName) throws Exception {
        try {
            Services serviceManager = penroseServer.getServices();
            return serviceManager.getStatus(serviceName);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        }
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

    public String getObjectName() {
        return PenroseClient.getObjectName();
    }

    public PenroseJMXService getService() {
        return service;
    }

    public void setService(PenroseJMXService service) {
        this.service = service;
    }

    public void register() throws Exception {
        service.register(getObjectName(), this);

        for (PartitionService partitionService : getPartitionServices()) {
            partitionService.register();
        }
    }

    public void unregister() throws Exception {
        for (PartitionService partitionService : getPartitionServices()) {
            partitionService.unregister();
        }

        service.unregister(getObjectName());
    }
}
