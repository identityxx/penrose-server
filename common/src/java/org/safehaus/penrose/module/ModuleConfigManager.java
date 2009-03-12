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

    protected Map<String,ModuleConfig> moduleConfigs = new LinkedHashMap<String,ModuleConfig>();

    public void addModuleConfig(ModuleConfig moduleConfig) throws Exception {

        Logger log = LoggerFactory.getLogger(getClass());
        boolean debug = log.isDebugEnabled();

        String moduleName = moduleConfig.getName();

        if (debug) log.debug("Adding module \""+moduleName+"\".");

        validate(moduleConfig);

        moduleConfigs.put(moduleName, moduleConfig);
    }

    public void validate(ModuleConfig moduleConfig) throws Exception {

        String moduleName = moduleConfig.getName();

        if (moduleName == null || "".equals(moduleName)) {
            throw new Exception("Missing module name.");
        }

        char startingChar = moduleName.charAt(0);
        if (!Character.isLetter(startingChar)) {
            throw new Exception("Invalid module name: "+moduleName);
        }

        for (int i = 1; i<moduleName.length(); i++) {
            char c = moduleName.charAt(i);
            if (Character.isLetterOrDigit(c) || c == '_') continue;
            throw new Exception("Invalid module name: "+moduleName);
        }

        if (moduleConfigs.containsKey(moduleName)) {
            throw new Exception("Module "+moduleName+" already exists.");
        }
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
    }

    public ModuleConfig removeModuleConfig(String moduleName) {
        return moduleConfigs.remove(moduleName);
    }

    public void addModuleMappings(Collection<ModuleMapping> moduleMappings) throws Exception {
        for (ModuleMapping moduleMapping : moduleMappings) {
            addModuleMapping(moduleMapping);
        }
    }
    
    public void addModuleMapping(ModuleMapping moduleMapping) throws Exception {

        Logger log = LoggerFactory.getLogger(getClass());
        boolean debug = log.isDebugEnabled();

        String moduleName = moduleMapping.getModuleName();

        if (debug) log.debug("Adding module mapping "+moduleName+" => "+ moduleMapping.getBaseDn());

        ModuleConfig moduleConfig = moduleConfigs.get(moduleName);
        if (moduleConfig == null) {
            throw new Exception("Module "+moduleName+" not found.");
        }

        moduleConfig.addModuleMapping(moduleMapping);
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
    }

    public void removeModuleMapping(String moduleName) throws Exception {

        ModuleConfig moduleConfig = moduleConfigs.get(moduleName);
        if (moduleConfig == null) {
            throw new Exception("Module "+moduleName+" not found.");
        }

        moduleConfig.removeModuleMappings();
    }

    public Object clone() throws CloneNotSupportedException {
        ModuleConfigManager modules = (ModuleConfigManager)super.clone();

        modules.moduleConfigs = new LinkedHashMap<String,ModuleConfig>();

        for (ModuleConfig moduleConfig : moduleConfigs.values()) {
            modules.moduleConfigs.put(moduleConfig.getName(), (ModuleConfig)moduleConfig.clone());
        }

        return modules;
    }
}

