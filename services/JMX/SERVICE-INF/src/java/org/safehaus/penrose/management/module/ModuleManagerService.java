package org.safehaus.penrose.management.module;

import org.safehaus.penrose.management.BaseService;
import org.safehaus.penrose.management.PenroseJMXService;
import org.safehaus.penrose.module.*;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.partition.Partition;

import java.util.Collection;
import java.util.ArrayList;

/**
 * @author Endi Sukma Dewata
 */
public class ModuleManagerService extends BaseService implements ModuleManagerServiceMBean {

    private PartitionManager partitionManager;
    private String partitionName;

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

    public ModuleService getModuleService(String moduleName) throws Exception {

        ModuleService moduleService = new ModuleService(jmxService, partitionManager, partitionName, moduleName);
        moduleService.init();

        return moduleService;
    }

    public void register() throws Exception {

        super.register();

        ModuleConfigManager moduleConfigManager = getModuleConfigManager();
        for (String moduleName : moduleConfigManager.getModuleNames()) {
            ModuleService moduleService = getModuleService(moduleName);
            moduleService.register();
        }
    }

    public void unregister() throws Exception {
        ModuleConfigManager moduleConfigManager = getModuleConfigManager();
        for (String moduleName : moduleConfigManager.getModuleNames()) {
            ModuleService moduleService = getModuleService(moduleName);
            moduleService.unregister();
        }

        super.unregister();
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

        Partition partition = getPartition();
        if (partition != null) {
            ModuleManager moduleManager = partition.getModuleManager();
            moduleManager.createModule(moduleConfig);
        }

        String moduleName = moduleConfig.getName();

        PartitionConfig partitionConfig = getPartitionConfig();
        ModuleConfigManager moduleConfigManager = partitionConfig.getModuleConfigManager();
        moduleConfigManager.addModuleConfig(moduleConfig);

        ModuleService moduleService = getModuleService(moduleName);
        moduleService.register();
    }

    public void createModule(ModuleConfig moduleConfig, Collection<ModuleMapping> moduleMappings) throws Exception {

        Partition partition = getPartition();
        if (partition != null) {
            ModuleManager moduleManager = partition.getModuleManager();
            moduleManager.createModule(moduleConfig);
        }

        String moduleName = moduleConfig.getName();

        PartitionConfig partitionConfig = getPartitionConfig();
        ModuleConfigManager moduleConfigManager = partitionConfig.getModuleConfigManager();
        moduleConfigManager.addModuleConfig(moduleConfig);
        moduleConfigManager.addModuleMappings(moduleMappings);

        ModuleService moduleService = getModuleService(moduleName);
        moduleService.register();
    }

    public void updateModule(String name, ModuleConfig moduleConfig) throws Exception {

        Partition partition = getPartition();

        if (partition != null) {
            ModuleManager moduleManager = partition.getModuleManager();
            moduleManager.removeModule(name);
        }

        ModuleService oldModuleService = getModuleService(name);
        oldModuleService.unregister();

        PartitionConfig partitionConfig = getPartitionConfig();
        ModuleConfigManager moduleConfigManager = partitionConfig.getModuleConfigManager();
        moduleConfigManager.updateModuleConfig(name, moduleConfig);

        if (partition != null) {
            ModuleManager moduleManager = partition.getModuleManager();
            moduleManager.createModule(moduleConfig);
        }

        ModuleService newModuleService = getModuleService(moduleConfig.getName());
        newModuleService.register();
    }

    public void removeModule(String name) throws Exception {

        Partition partition = getPartition();

        if (partition != null) {
            ModuleManager moduleManager = partition.getModuleManager();
            moduleManager.removeModule(name);
        }

        PartitionConfig partitionConfig = getPartitionConfig();
        ModuleConfigManager moduleConfigManager = partitionConfig.getModuleConfigManager();
        moduleConfigManager.removeModuleConfig(name);

        ModuleService moduleService = getModuleService(name);
        moduleService.unregister();
    }

}
