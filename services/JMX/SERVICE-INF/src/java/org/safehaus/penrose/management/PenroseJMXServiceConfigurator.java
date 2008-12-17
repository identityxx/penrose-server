package org.safehaus.penrose.management;

import org.safehaus.penrose.service.ServiceConfigurator;
import org.safehaus.penrose.config.Parameter;

/**
 * @author Endi Sukma Dewata
 */
public class PenroseJMXServiceConfigurator extends ServiceConfigurator {

    public void init() throws Exception {
        String rmiPort = serviceConfig.getParameter("rmiPort");
        if (rmiPort == null || "".equals(rmiPort)) rmiPort = "1099";
        addParameter(new Parameter("rmiPort", "RMI Port", rmiPort));

        String rmiTransportPort = serviceConfig.getParameter("rmiTransportPort");
        if (rmiTransportPort == null || "".equals(rmiTransportPort)) rmiTransportPort = "40888";
        addParameter(new Parameter("rmiTransportPort", "RMI Transport Port", rmiTransportPort));
    }

    public void setParameterValue(Parameter parameter, String value) throws Exception {
        serviceConfig.setParameter(parameter.getName(), value);
    }

    public void close() throws Exception {
    }
}
