package org.safehaus.penrose.module;

import org.safehaus.penrose.ldap.DN;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author Endi Sukma Dewata
 */
public class ModuleManager {

    public Logger log = LoggerFactory.getLogger(getClass());
    public boolean debug = log.isDebugEnabled();

    protected Partition partition;
    protected ModuleConfigManager moduleConfigManager;

    protected Map<String,Module> modules = new LinkedHashMap<String,Module>();

    public ModuleManager(Partition partition) {
        this.partition = partition;

        PartitionConfig partitionConfig = partition.getPartitionConfig();
        this.moduleConfigManager = partitionConfig.getModuleConfigManager();
    }

    public void init() throws Exception {
        
        Collection<String> moduleNames = new ArrayList<String>();
        moduleNames.addAll(getModuleNames());

        for (String moduleName : moduleNames) {

            ModuleConfig moduleConfig = getModuleConfig(moduleName);
            if (!moduleConfig.isEnabled()) continue;

            startModule(moduleName);
        }
    }

    public void destroy() throws Exception {

        Collection<String> moduleNames = new ArrayList<String>();
        moduleNames.addAll(modules.keySet());

        for (String name : moduleNames) {
            stopModule(name);
        }
    }

    public Collection<String> getModuleNames() {
        return moduleConfigManager.getModuleNames();
    }

    public ModuleConfig getModuleConfig(String moduleName) {
        return moduleConfigManager.getModuleConfig(moduleName);
    }

    public void startModule(String moduleName) throws Exception {

        if (debug) log.debug("Starting module "+moduleName+".");

        ModuleConfig moduleConfig = getModuleConfig(moduleName);

        String className = moduleConfig.getModuleClass();

        ClassLoader cl = partition.getPartitionContext().getClassLoader();
        Class clazz = cl.loadClass(className);
        Module module = (Module)clazz.newInstance();

        ModuleContext moduleContext = new ModuleContext();
        moduleContext.setPartition(partition);

        module.init(moduleConfig, moduleContext);

        modules.put(module.getName(), module);
    }

    public void stopModule(String moduleName) throws Exception {

        if (debug) log.debug("Stopping module "+moduleName+".");

        Module module = modules.remove(moduleName);
        module.destroy();
    }

    public Collection<Module> getModules() {
        return modules.values();
    }

    public Module getModule(String moduleName) {
        Module module = modules.get(moduleName);
        if (module != null) return module;

        if (partition.getName().equals("DEFAULT")) return null;
        Partition defaultPartition = partition.getPartitionContext().getPartition("DEFAULT");

        ModuleManager moduleManager = defaultPartition.getModuleManager();
        return moduleManager.getModule(moduleName);
    }

    public Collection<Module> findModules(DN dn) throws Exception {

        boolean debug = log.isDebugEnabled();

        if (debug) log.debug("Modules:");

        Collection<Module> list = new ArrayList<Module>();

        for (ModuleMapping moduleMapping : moduleConfigManager.getModuleMappings()) {
            String moduleName = moduleMapping.getModuleName();

            boolean b = moduleMapping.match(dn);
            if (!b) continue;

            Module module = getModule(moduleName);
            if (module == null) continue;
            if (!module.isEnabled()) continue;

            if (debug) log.debug(" - "+moduleName);
            list.add(module);
        }

        return list;
    }

}
