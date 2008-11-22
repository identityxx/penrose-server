package org.safehaus.penrose.management.module;

import org.safehaus.penrose.module.*;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.management.BaseService;
import org.safehaus.penrose.management.PenroseJMXService;

import java.util.Collection;
import java.util.ArrayList;

/**
 * @author Endi Sukma Dewata
 */
public class ModuleService extends BaseService implements ModuleServiceMBean {

    private PartitionManager partitionManager;
    private String partitionName;
    private String moduleName;

    public ModuleService(
            PenroseJMXService jmxService,
            PartitionManager partitionManager,
            String partitionName,
            String moduleName
    ) throws Exception {

        this.jmxService = jmxService;
        this.partitionManager = partitionManager;
        this.partitionName = partitionName;
        this.moduleName = moduleName;
    }

    public String getObjectName() {
        return ModuleClient.getStringObjectName(partitionName, moduleName);
    }

    public Object getObject() {
        return getModule();
    }

    public ModuleConfig getModuleConfig() throws Exception {
        return getPartitionConfig().getModuleConfigManager().getModuleConfig(moduleName);
    }

    public Collection<String> getParameterNames() throws Exception {
        ModuleConfig moduleConfig = getModuleConfig();
        Collection<String> list = new ArrayList<String>();
        list.addAll(moduleConfig.getParameterNames());
        return list;
    }

    public String getParameter(String name) throws Exception {
        ModuleConfig moduleConfig = getModuleConfig();
        return moduleConfig.getParameter(name);
    }

    public Module getModule() {
        Partition partition = getPartition();
        if (partition == null) return null;

        ModuleManager moduleManager = partition.getModuleManager();
        return moduleManager.getModule(moduleName);
    }

    public PartitionConfig getPartitionConfig() {
        return partitionManager.getPartitionConfig(partitionName);
    }

    public Partition getPartition() {
        return partitionManager.getPartition(partitionName);
    }

    public void addModuleMapping(ModuleMapping moduleMapping) throws Exception {
        PartitionConfig partitionConfig = getPartitionConfig();
        ModuleConfigManager moduleConfigManager = partitionConfig.getModuleConfigManager();
        moduleConfigManager.addModuleMapping(moduleMapping);
    }

    public void removeModuleMapping(ModuleMapping moduleMapping) throws Exception {
        PartitionConfig partitionConfig = getPartitionConfig();
        ModuleConfigManager moduleConfigManager = partitionConfig.getModuleConfigManager();
        moduleConfigManager.removeModuleMapping(moduleMapping);
    }

    public Collection<ModuleMapping> getModuleMappings() throws Exception {
        PartitionConfig partitionConfig = getPartitionConfig();
        ModuleConfigManager moduleConfigManager = partitionConfig.getModuleConfigManager();

        Collection<ModuleMapping> results = new ArrayList<ModuleMapping>();
        Collection<ModuleMapping> list = moduleConfigManager.getModuleMappings(moduleName);
        if (list != null) results.addAll(list);

        return results;
    }

    public String getStatus() throws Exception {
        Module module = getModule();
        return module == null ? ModuleServiceMBean.STOPPED : ModuleServiceMBean.STARTED;
    }

    public void start() throws Exception {

        log.debug("Starting module "+partitionName+"/"+moduleName+"...");

        Module module = getModule();
        module.init();

        log.debug("Module started.");
    }

    public void stop() throws Exception {

        log.debug("Stopping module "+partitionName+"/"+moduleName+"...");

        Module module = getModule();
        module.destroy();

        log.debug("Module stopped.");
    }

    public void restart() throws Exception {

        log.debug("Restarting module "+partitionName+"/"+moduleName+"...");

        Module module = getModule();
        module.destroy();
        module.init();

        log.debug("Module restarted.");
    }
}
