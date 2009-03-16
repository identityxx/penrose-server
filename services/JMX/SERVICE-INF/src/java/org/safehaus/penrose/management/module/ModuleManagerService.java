package org.safehaus.penrose.management.module;

import org.safehaus.penrose.management.BaseService;
import org.safehaus.penrose.management.PenroseJMXService;
import org.safehaus.penrose.module.*;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.partition.Partition;

import java.util.Collection;
import java.util.ArrayList;
import java.util.Map;
import java.util.LinkedHashMap;

/**
 * @author Endi Sukma Dewata
 */
public class ModuleManagerService extends BaseService implements ModuleManagerServiceMBean {

    private PartitionManager partitionManager;
    private String partitionName;

    Map<String,ModuleService> moduleServices = new LinkedHashMap<String,ModuleService>();

    public ModuleManagerService(PenroseJMXService jmxService, PartitionManager partitionManager, String partitionName) throws Exception {

        this.jmxService = jmxService;
        this.partitionManager = partitionManager;
        this.partitionName = partitionName;
    }

    public String getObjectName() {
        return ModuleManagerClient.getStringObjectName(partitionName);
    }

    public Object getObject() {
        return getModuleManager();
    }

    public PartitionConfig getPartitionConfig() {
        return partitionManager.getPartitionConfig(partitionName);
    }

    public Partition getPartition() {
        return partitionManager.getPartition(partitionName);
    }

    public ModuleConfigManager getModuleConfigManager() {
        PartitionConfig partitionConfig = getPartitionConfig();
        if (partitionConfig == null) return null;
        return partitionConfig.getModuleConfigManager();
    }

    public ModuleManager getModuleManager() {
        Partition partition = getPartition();
        if (partition == null) return null;
        return partition.getModuleManager();
    }

    public void createModuleService(String moduleName) throws Exception {

        ModuleService moduleService = new ModuleService(jmxService, partitionManager, partitionName, moduleName);
        moduleService.init();

        moduleServices.put(moduleName, moduleService);
    }

    public ModuleService getModuleService(String moduleName) throws Exception {
        return moduleServices.get(moduleName);
    }

    public void removeModuleService(String moduleName) throws Exception {
        ModuleService moduleService = moduleServices.remove(moduleName);
        if (moduleService == null) return;

        moduleService.destroy();
    }

    public void init() throws Exception {

        super.init();

        ModuleConfigManager moduleConfigManager = getModuleConfigManager();
        for (String moduleName : moduleConfigManager.getModuleNames()) {
            createModuleService(moduleName);
        }
    }

    public void destroy() throws Exception {
        ModuleConfigManager moduleConfigManager = getModuleConfigManager();
        for (String moduleName : moduleConfigManager.getModuleNames()) {
            removeModuleService(moduleName);
        }

        super.destroy();
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Modules
    ////////////////////////////////////////////////////////////////////////////////

    public Collection<String> getModuleNames() throws Exception {

        PartitionConfig partitionConfig = getPartitionConfig();
        ModuleConfigManager moduleConfigManager = partitionConfig.getModuleConfigManager();

        Collection<String> list = new ArrayList<String>();
        list.addAll(moduleConfigManager.getModuleNames());

        return list;
    }

    public void createModule(ModuleConfig moduleConfig) throws Exception {

        boolean debug = log.isDebugEnabled();
        String moduleName = moduleConfig.getName();
        if (debug) log.debug("Creating module "+moduleName+".");

        PartitionConfig partitionConfig = getPartitionConfig();
        ModuleConfigManager moduleConfigManager = partitionConfig.getModuleConfigManager();
        moduleConfigManager.addModuleConfig(moduleConfig);

        Partition partition = getPartition();
        if (partition != null) {
            ModuleManager moduleManager = partition.getModuleManager();
            moduleManager.startModule(moduleName);
        }

        createModuleService(moduleName);
    }

    public void updateModule(String moduleName, ModuleConfig moduleConfig) throws Exception {

        boolean debug = log.isDebugEnabled();
        if (debug) log.debug("Updating module "+moduleName+".");

        Partition partition = getPartition();

        if (partition != null) {
            ModuleManager moduleManager = partition.getModuleManager();
            moduleManager.stopModule(moduleName);
        }

        removeModuleService(moduleName);

        PartitionConfig partitionConfig = getPartitionConfig();
        ModuleConfigManager moduleConfigManager = partitionConfig.getModuleConfigManager();
        moduleConfigManager.updateModuleConfig(moduleName, moduleConfig);

        if (partition != null) {
            ModuleManager moduleManager = partition.getModuleManager();
            moduleManager.startModule(moduleName);
        }

        createModuleService(moduleConfig.getName());
    }

    public void removeModule(String moduleName) throws Exception {

        boolean debug = log.isDebugEnabled();
        if (debug) log.debug("Removing module "+moduleName+".");

        Partition partition = getPartition();

        if (partition != null) {
            ModuleManager moduleManager = partition.getModuleManager();
            moduleManager.stopModule(moduleName);
        }

        PartitionConfig partitionConfig = getPartitionConfig();
        ModuleConfigManager moduleConfigManager = partitionConfig.getModuleConfigManager();
        moduleConfigManager.removeModuleConfig(moduleName);

        removeModuleService(moduleName);
    }

}
