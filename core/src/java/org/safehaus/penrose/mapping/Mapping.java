package org.safehaus.penrose.mapping;

import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.ldap.Attributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;

/**
 * @author Endi Sukma Dewata
 */
public class Mapping {

    public Logger log = LoggerFactory.getLogger(getClass());
    public boolean debug = log.isDebugEnabled();

    private MappingConfig mappingConfig;
    private MappingContext mappingContext;

    public Mapping() {
    }

    public String getName() {
        return mappingConfig.getName();
    }

    public void init(
            MappingConfig mappingConfig,
            MappingContext mappingContext
    ) throws Exception {

        log.debug("Starting "+mappingConfig.getName()+" mapping.");

        this.mappingConfig = mappingConfig;
        this.mappingContext = mappingContext;

        init();
    }

    public void init() throws Exception {
    }

    public void destroy() throws Exception {
    }

    public Collection<String> getFieldNames() {
        return mappingConfig.getFieldNames();
    }
    
    public Collection<MappingFieldConfig> getFieldConfigs() {
        return mappingConfig.getFieldConfigs();
    }

    public Attributes map(String prefix, Attributes input) throws Exception {

        Attributes output = new Attributes();

        Interpreter interpreter = mappingContext.getPartition().newInterpreter();
        interpreter.set(prefix, input);

        map(interpreter, output);

        return output;
    }

    public void map(Interpreter interpreter, Attributes output) throws Exception {

        if (debug) log.debug("Executing "+mappingConfig.getName()+" mapping:");

        String preMapping = mappingConfig.getPreScript();
        if (preMapping != null) {
            if (debug) log.debug(" - Executing pre-script");
            interpreter.eval(preMapping);
        }

        for (MappingFieldConfig fieldConfig : mappingConfig.getFieldConfigs()) {
            String name = fieldConfig.getName();

            boolean required = fieldConfig.isRequired();
            if (!required) {
                Object value = interpreter.get(name);
                if (value != null) {
                    if (debug) log.debug(" - Skipping "+name+": value has been set");
                    continue;
                }
            }

            String variable = fieldConfig.getVariable();
            if (variable != null) {
                Object variableValue = interpreter.get(variable);
                if (variableValue == null) {
                    if (debug) log.debug(" - Skipping "+name+": "+variable+" is undefined");
                    continue;
                }
            }
            String condition = fieldConfig.getCondition();
            if (condition != null) {
                Object conditionValue = interpreter.eval(condition);
                if (!(conditionValue instanceof Boolean && (Boolean) conditionValue)) {
                    if (debug) {
                        //String className = conditionValue == null ? "" : " ("+conditionValue.getClass().getName()+")";
                        log.debug(" - Skipping "+name+": condition is "+ conditionValue);
                    }
                    continue;
                }
            }

            Object newValue = interpreter.eval(fieldConfig);
            if (newValue == null) {
                if (debug) log.debug(" - Skipping "+name+": value is null");
                continue;
            }

            if (debug) log.debug(" - Adding "+name+": "+newValue+" ("+newValue.getClass().getName()+")");

            Object oldValue = interpreter.get(name);
            //if (debug) {
            //    String className = oldValue == null ? "" : " ("+oldValue.getClass().getName()+")";
            //    log.debug("   Old value: "+oldValue+className);
            //}

            if (oldValue == newValue) {
                // skip
                
            } else if (oldValue == null) {
                interpreter.set(name, newValue);

            } else if (oldValue instanceof Collection) {
                Collection<Object> list = (Collection<Object>)oldValue;
                if (newValue instanceof Collection) {
                    list.addAll((Collection<Object>)newValue);
                } else {
                    list.add(newValue);
                }

            } else if (oldValue.equals(newValue)) {
                // skip

            } else {
                Collection<Object> list = new LinkedHashSet<Object>();
                list.add(oldValue);
                if (newValue instanceof Collection) {
                    list.addAll((Collection<Object>)newValue);
                } else {
                    list.add(newValue);
                }
                interpreter.set(name, list);
            }
        }

        String postMapping = mappingConfig.getPostScript();
        if (postMapping != null) {
            if (debug) log.debug(" - Executing post-script");
            interpreter.eval(postMapping);
        }

        //if (debug) log.debug(" - Storing mapping results");

        for (String name : mappingConfig.getFieldNames()) {
            Object value = interpreter.get(name);
            //if (debug) log.debug("   - "+name+": "+value);
            if (value == null) continue;

            if (value instanceof Collection) {
                Collection<Object> list = (Collection<Object>)value;
                output.setValues(name, list);

            } else {
                output.setValue(name, value);
            }
        }
    }

    public MappingConfig getMappingConfig() {
        return mappingConfig;
    }

    public void setMappingConfig(MappingConfig mappingConfig) {
        this.mappingConfig = mappingConfig;
    }

    public MappingContext getMappingContext() {
        return mappingContext;
    }

    public void setMappingContext(MappingContext mappingContext) {
        this.mappingContext = mappingContext;
    }

    public String getDescription() {
        return mappingConfig.getDescription();
    }

    public String getParameter(String name) {
        return mappingConfig.getParameter(name);
    }

    public Map<String,String> getParameters() {
        return mappingConfig.getParameters();
    }

    public Collection<String> getParameterNames() {
        return mappingConfig.getParameterNames();
    }

    public String removeParameter(String name) {
        return mappingConfig.removeParameter(name);
    }

}
