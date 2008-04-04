package org.safehaus.penrose.module;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author Endi Sukma Dewata
 */
public class ModuleConfigManager implements Serializable, Cloneable {

    static {
        log = LoggerFactory.getLogger(ModuleConfigManager.class);
    }

    public static transient Logger log;
    public static boolean debug = log.isDebugEnabled();

    private Map<String,ModuleConfig> moduleConfigs = new LinkedHashMap<String,ModuleConfig>();
    private Map<String,Collection<ModuleMapping>> moduleMappings = new LinkedHashMap<String,Collection<ModuleMapping>>();

    public void addModuleConfig(ModuleConfig moduleConfig) {
        String name = moduleConfig.getName();
        //if (debug) log.debug("Adding module "+name+".");
        moduleConfigs.put(name, moduleConfig);
    }

    public ModuleConfig getModuleConfig(String name) {
        return moduleConfigs.get(name);
    }

    public Collection<String> getModuleNames() {
        return moduleConfigs.keySet();
    }
    
    public Collection<ModuleConfig> getModuleConfigs() {
        return moduleConfigs.values();
    }

    public void updateModuleConfig(String name, ModuleConfig moduleConfig) throws Exception {

        ModuleConfig origModuleConfig = moduleConfigs.get(name);
        origModuleConfig.copy(moduleConfig);

        if (!name.equals(moduleConfig.getName())) {
            moduleConfigs.remove(name);
            moduleConfigs.put(moduleConfig.getName(), moduleConfig);

            Collection<ModuleMapping> list = moduleMappings.remove(name);
            if (list != null) {
                for (ModuleMapping moduleMapping : list) {
                    moduleMapping.setModuleName(moduleConfig.getName());
                }
                moduleMappings.put(name, list);
            }
        }
    }

    public ModuleConfig removeModuleConfig(String moduleName) {
        moduleMappings.remove(moduleName);
        return moduleConfigs.remove(moduleName);
    }

    public void addModuleMappings(Collection<ModuleMapping> moduleMappings) {
        for (ModuleMapping moduleMapping : moduleMappings) {
            addModuleMapping(moduleMapping);
        }
    }
    
    public void addModuleMapping(ModuleMapping moduleMapping) {

        String moduleName = moduleMapping.getModuleName();

        if (debug) log.debug("Adding module mapping "+moduleName+" => "+ moduleMapping.getBaseDn());

        Collection<ModuleMapping> c = moduleMappings.get(moduleMapping.getModuleName());
        if (c == null) {
            c = new ArrayList<ModuleMapping>();
            moduleMappings.put(moduleMapping.getModuleName(), c);
        }
        c.add(moduleMapping);
    }

    public Collection<ModuleMapping> getModuleMappings() {
        Collection<ModuleMapping> results = new ArrayList<ModuleMapping>();
        for (Collection<ModuleMapping> list : moduleMappings.values()) {
            results.addAll(list);
        }
        return results;
    }

    public Collection<ModuleMapping> getModuleMappings(String name) {
        return moduleMappings.get(name);
    }

    public void removeModuleMapping(ModuleMapping mapping) {
        if (mapping == null) return;
        if (mapping.getModuleName() == null) return;

        Collection<ModuleMapping> c = moduleMappings.get(mapping.getModuleName());
        if (c != null) c.remove(mapping);
    }

    public Collection<ModuleMapping> removeModuleMapping(String moduleName) {
        return moduleMappings.remove(moduleName);
    }

    public Object clone() throws CloneNotSupportedException {
        ModuleConfigManager modules = (ModuleConfigManager)super.clone();

        modules.moduleConfigs = new LinkedHashMap<String,ModuleConfig>();
        modules.moduleMappings = new LinkedHashMap<String,Collection<ModuleMapping>>();

        for (ModuleConfig moduleConfig : moduleConfigs.values()) {
            modules.addModuleConfig((ModuleConfig)moduleConfig.clone());

            Collection<ModuleMapping> list = moduleMappings.get(moduleConfig.getName());
            if (list == null) continue;
            
            for (ModuleMapping moduleMapping : list) {
                modules.addModuleMapping((ModuleMapping)moduleMapping.clone());
            }
        }

        return modules;
    }
}

