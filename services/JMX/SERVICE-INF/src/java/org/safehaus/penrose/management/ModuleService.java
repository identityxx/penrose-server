package org.safehaus.penrose.management;

import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.module.Module;
import org.safehaus.penrose.module.ModuleConfig;

/**
 * @author Endi Sukma Dewata
 */
public class ModuleService extends JMXService implements ModuleServiceMBean {

    private Module module;

    public ModuleService(Module module) throws Exception {
        super(module, module.getDescription());

        this.module = module;
    }

    public Module getModule() {
        return module;
    }

    public void setModule(Module module) {
        this.module = module;
    }

    public ModuleConfig getModuleConfig() throws Exception {
        return module.getModuleConfig();
    }

    public String getObjectName() {
        Partition partition = module.getPartition();
        return ModuleClient.getObjectName(partition.getName(), module.getName());
    }

    public void start() throws Exception {
        Partition partition = module.getPartition();
        log.debug("Starting module "+partition.getName()+"/"+module.getName()+"...");
        module.init();
        log.debug("Module started.");
    }

    public void stop() throws Exception {
        Partition partition = module.getPartition();
        log.debug("Stopping module "+partition.getName()+"/"+module.getName()+"...");
        module.destroy();
        log.debug("Module stopped.");
    }

    public void restart() throws Exception {
        Partition partition = module.getPartition();
        log.debug("Restarting module "+partition.getName()+"/"+module.getName()+"...");
        module.destroy();
        module.init();
        log.debug("Module restarted.");
    }
}
