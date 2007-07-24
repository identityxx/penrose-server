package org.safehaus.penrose.module;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Collection;
import java.util.ArrayList;

/**
 * @author Endi Sukma Dewata
 */
public class Modules {

    public Logger log = LoggerFactory.getLogger(getClass());
    public boolean debug = log.isDebugEnabled();

    private Map<String,ModuleConfig> moduleConfigs = new LinkedHashMap<String,ModuleConfig>();
    private Map<String, Collection<ModuleMapping>> moduleMappings = new LinkedHashMap<String,Collection<ModuleMapping>>();

    public void addModuleConfig(ModuleConfig moduleConfig) throws Exception {
        moduleConfigs.put(moduleConfig.getName(), moduleConfig);
    }

    public ModuleConfig getModuleConfig(String name) {
        return moduleConfigs.get(name);
    }

    public Collection<ModuleConfig> getModuleConfigs() {
        return moduleConfigs.values();
    }

    public ModuleConfig removeModuleConfig(String moduleName) {
        return moduleConfigs.remove(moduleName);
    }

    public void addModuleMapping(ModuleMapping mapping) throws Exception {

        String moduleName = mapping.getModuleName();

        if (debug) log.debug("Adding module mapping "+moduleName+" => "+mapping.getBaseDn());
        if (moduleName == null) throw new Exception("Missing module name");

        Collection<ModuleMapping> c = moduleMappings.get(mapping.getModuleName());
        if (c == null) {
            c = new ArrayList<ModuleMapping>();
            moduleMappings.put(mapping.getModuleName(), c);
        }
        c.add(mapping);

        ModuleConfig moduleConfig = getModuleConfig(moduleName);
        if (moduleConfig == null) throw new Exception("Undefined module "+moduleName);

        mapping.setModuleConfig(moduleConfig);
    }

    public Collection<Collection<ModuleMapping>> getModuleMappings() {
        return moduleMappings.values();
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
}

