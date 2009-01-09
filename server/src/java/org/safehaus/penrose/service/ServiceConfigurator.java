package org.safehaus.penrose.service;

import org.safehaus.penrose.config.Parameter;

import java.io.PrintWriter;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class ServiceConfigurator {

    protected File serviceDir;
    protected ServiceConfig serviceConfig;

    protected PrintWriter out = new PrintWriter(System.out, true);
    protected BufferedReader in = new BufferedReader(new InputStreamReader(System.in));

    protected Map<String,Parameter> parameters = new LinkedHashMap<String,Parameter>();

    public ServiceConfigurator() {
    }

    public void init() throws Exception {
    }

    public void configure() throws Exception {
        for (Parameter parameter : parameters.values()) {

            boolean done = false;
            while (!done) {
                try {
                    configure(parameter);
                    done = true;

                } catch (Exception e) {
                    out.println("Error: "+e.getMessage());
                }
            }
        }
    }

    public void configure(Parameter parameter) throws Exception {
        String description = parameter.getDisplayName();
        String value = parameter.getDefaultValue();

        out.print(description+" ["+value+"]: ");
        out.flush();

        String s = in.readLine();
        if (s != null) {
            s = s.trim();
            if (!"".equals(s)) value = s;
        }

        Collection<String> options = parameter.getOptions();
        if (!options.isEmpty() && !options.contains(value)) {
            throw new Exception("Valid options are "+options+".");
        }

        setParameterValue(parameter, value);
    }

    public void close() throws Exception {
    }

    public ServiceConfig getServiceConfig() {
        return serviceConfig;
    }

    public void setServiceConfig(ServiceConfig serviceConfig) {
        this.serviceConfig = serviceConfig;
    }

    public File getServiceDir() {
        return serviceDir;
    }

    public void setServiceDir(File serviceDir) {
        this.serviceDir = serviceDir;
    }

    public Collection<Parameter> getParameters() {
        return parameters.values();
    }

    public void setParameters(Map<String, Parameter> parameters) {
        this.parameters = parameters;
    }

    public void addParameter(Parameter parameter) {
        parameters.put(parameter.getName(), parameter);
    }
    
    public Collection<String> getParameterNames() {
        return parameters.keySet();
    }

    public void setParameterValue(Parameter parameter, String value) throws Exception {
    }
}
