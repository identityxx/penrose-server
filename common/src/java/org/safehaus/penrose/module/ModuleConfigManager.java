package org.safehaus.penrose.module;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private Map<String,ModuleConfig> moduleConfigs = new LinkedHashMap<String,ModuleConfig>();
    //private Map<String,Collection<ModuleMapping>> moduleMappings = new LinkedHashMap<String,Collection<ModuleMapping>>();

    public void addModuleConfig(ModuleConfig moduleConfig) throws Exception {

        String moduleName = moduleConfig.getName();

        if (moduleConfigs.containsKey(moduleName)) {
            throw new Exception("Module "+moduleName+" already exists.");
        }

        moduleConfigs.put(moduleName, moduleConfig);
    }

    public ModuleConfig getModuleConfig(String moduleName) {
        return moduleConfigs.get(moduleName);
    }

    public Collection<String> getModuleNames() {
        return moduleConfigs.keySet();
    }
    
    public Collection<ModuleConfig> getModuleConfigs() {
        return moduleConfigs.values();
    }

    public void updateModuleConfig(String moduleName, ModuleConfig newModuleConfig) throws Exception {

        ModuleConfig moduleConfig = moduleConfigs.get(moduleName);

        if (moduleConfig == null) {
            throw new Exception("Module "+moduleName+" not found.");
        }

        moduleConfig.copy(newModuleConfig);
/*
        if (!moduleName.equals(newModuleConfig.getName())) {
            moduleConfigs.remove(moduleName);
            moduleConfigs.put(newModuleConfig.getName(), newModuleConfig);

            Collection<ModuleMapping> list = moduleMappings.remove(moduleName);
            if (list != null) {
                for (ModuleMapping moduleMapping : list) {
                    moduleMapping.setModuleName(newModuleConfig.getName());
                }
                moduleMappings.put(moduleName, list);
            }
        }
*/
    }

    public ModuleConfig removeModuleConfig(String moduleName) {
        //moduleMappings.remove(moduleName);
        return moduleConfigs.remove(moduleName);
    }

    public void addModuleMappings(Collection<ModuleMapping> moduleMappings) throws Exception {
        for (ModuleMapping moduleMapping : moduleMappings) {
            addModuleMapping(moduleMapping);
        }
    }
    
    public void addModuleMapping(ModuleMapping moduleMapping) throws Exception {

        boolean debug = log.isDebugEnabled();
        String moduleName = moduleMapping.getModuleName();

        if (debug) log.debug("Adding module mapping "+moduleName+" => "+ moduleMapping.getBaseDn());

        ModuleConfig moduleConfig = moduleConfigs.get(moduleName);
        if (moduleConfig == null) {
            throw new Exception("Module "+moduleName+" not found.");
        }

        moduleConfig.addModuleMapping(moduleMapping);
/*
        Collection<ModuleMapping> c = moduleMappings.get(moduleMapping.getModuleName());
        if (c == null) {
            c = new ArrayList<ModuleMapping>();
            moduleMappings.put(moduleMapping.getModuleName(), c);
        }
        c.add(moduleMapping);
*/
    }

    public Collection<ModuleMapping> getModuleMappings() {
        Collection<ModuleMapping> results = new ArrayList<ModuleMapping>();
        for (ModuleConfig moduleConfig : moduleConfigs.values()) {
            results.addAll(moduleConfig.getModuleMappings());
        }
        return results;
    }

    public Collection<ModuleMapping> getModuleMappings(String moduleName) throws Exception {
        ModuleConfig moduleConfig = moduleConfigs.get(moduleName);
        if (moduleConfig == null) {
            throw new Exception("Module "+moduleName+" not found.");
        }

        return moduleConfig.getModuleMappings();
        //return moduleMappings.get(moduleName);
    }

    public void removeModuleMapping(ModuleMapping moduleMapping) throws Exception {
        if (moduleMapping == null) return;

        String moduleName = moduleMapping.getModuleName();
        if (moduleName == null) return;

        ModuleConfig moduleConfig = moduleConfigs.get(moduleName);
        if (moduleConfig == null) {
            throw new Exception("Module "+moduleName+" not found.");
        }

        moduleConfig.removeModuleMapping(moduleMapping);
        //Collection<ModuleMapping> c = moduleMappings.get(moduleName);
        //if (c != null) c.remove(moduleMapping);
    }

    public void removeModuleMapping(String moduleName) throws Exception {

        ModuleConfig moduleConfig = moduleConfigs.get(moduleName);
        if (moduleConfig == null) {
            throw new Exception("Module "+moduleName+" not found.");
        }

        moduleConfig.removeModuleMappings();
        // moduleMappings.remove(moduleName);
    }

    public Object clone() throws CloneNotSupportedException {
        ModuleConfigManager modules = (ModuleConfigManager)super.clone();

        modules.moduleConfigs = new LinkedHashMap<String,ModuleConfig>();
/*
        modules.moduleMappings = new LinkedHashMap<String,Collection<ModuleMapping>>();

        for (ModuleConfig moduleConfig : moduleConfigs.values()) {
            modules.moduleConfigs.put(moduleConfig.getName(), (ModuleConfig)moduleConfig.clone());

            Collection<ModuleMapping> list = moduleMappings.get(moduleConfig.getName());
            if (list == null) continue;
            
            for (ModuleMapping moduleMapping : list) {
                modules.addModuleMapping((ModuleMapping)moduleMapping.clone());
            }
        }
*/
        return modules;
    }
}

