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
        
        Collection<String> names = new ArrayList<String>();
        names.addAll(getModuleNames());

        for (String name : names) {

            ModuleConfig moduleConfig = getModuleConfig(name);
            if (!moduleConfig.isEnabled()) continue;

            createModule(moduleConfig);
        }
    }

    public void destroy() throws Exception {

        Collection<String> names = new ArrayList<String>();
        names.addAll(modules.keySet());

        for (String name : names) {
            stopModule(name);
        }
    }

    public Collection<String> getModuleNames() {
        return moduleConfigManager.getModuleNames();
    }

    public ModuleConfig getModuleConfig(String name) {
        return moduleConfigManager.getModuleConfig(name);
    }

    public void startModule(String name) throws Exception {
        if (debug) log.debug("Starting module "+name+".");
        ModuleConfig moduleConfig = getModuleConfig(name);
        createModule(moduleConfig);
    }

    public void stopModule(String name) throws Exception {
        if (debug) log.debug("Stopping module "+name+".");
        Module module = removeModule(name);
        module.destroy();
    }

    public Module createModule(ModuleConfig moduleConfig) throws Exception {

        String className = moduleConfig.getModuleClass();

        ClassLoader cl = partition.getPartitionContext().getClassLoader();
        Class clazz = cl.loadClass(className);
        Module module = (Module)clazz.newInstance();

        ModuleContext moduleContext = new ModuleContext();
        moduleContext.setPartition(partition);

        module.init(moduleConfig, moduleContext);

        addModule(module);

        return module;
    }

    public void addModule(Module module) {
        modules.put(module.getName(), module);
    }

    public Module removeModule(String name) {
        return modules.remove(name);
    }

    public Collection<Module> getModules() {
        return modules.values();
    }

    public Module getModule(String name) {
        Module module = modules.get(name);
        if (module != null) return module;

        if (partition.getName().equals("DEFAULT")) return null;
        Partition defaultPartition = partition.getPartitionContext().getPartition("DEFAULT");

        ModuleManager moduleManager = defaultPartition.getModuleManager();
        return moduleManager.getModule(name);
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
