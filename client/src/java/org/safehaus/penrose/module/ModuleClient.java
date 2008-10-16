package org.safehaus.penrose.module;

import org.safehaus.penrose.management.BaseClient;
import org.safehaus.penrose.management.PenroseClient;
import org.safehaus.penrose.management.module.ModuleServiceMBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class ModuleClient extends BaseClient implements ModuleServiceMBean {

    public static Logger log = LoggerFactory.getLogger(ModuleClient.class);

    protected String partitionName;

    public ModuleClient(PenroseClient client, String partitionName, String name) throws Exception {
        super(client, name, getStringObjectName(partitionName, name));

        this.partitionName = partitionName;
    }

    public ModuleConfig getModuleConfig() throws Exception {
        return (ModuleConfig)getAttribute("ModuleConfig");
    }

    public Collection<String> getParameterNames() throws Exception {
        return (Collection<String>)getAttribute("ParameterNames");
    }

    public String getParameter(String name) throws Exception {
        return (String)invoke(
                "getParameter",
                new Object[] { name },
                new String[] { String.class.getName() }
        );
    }

    public void addModuleMapping(ModuleMapping moduleMapping) throws Exception {
        invoke(
                "addModuleMapping",
                new Object[] { moduleMapping },
                new String[] { ModuleMapping.class.getName() }
        );
    }

    public void removeModuleMapping(ModuleMapping moduleMapping) throws Exception {
        invoke(
                "removeModuleMapping",
                new Object[] { moduleMapping },
                new String[] { ModuleMapping.class.getName() }
        );
    }

    public Collection<ModuleMapping> getModuleMappings() throws Exception {
        return (Collection<ModuleMapping>)getAttribute("ModuleMappings");
    }

    public static String getStringObjectName(String partitionName, String name) {
        return "Penrose:type=module,partition="+partitionName+",name="+name;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public void setPartitionName(String partitionName) {
        this.partitionName = partitionName;
    }

    public void start() throws Exception {
        invoke("start", new Object[] {}, new String[] {});
    }

    public void stop() throws Exception {
        invoke("stop", new Object[] {}, new String[] {});
    }

    public void restart() throws Exception {
        invoke("restart", new Object[] {}, new String[] {});
    }
}
