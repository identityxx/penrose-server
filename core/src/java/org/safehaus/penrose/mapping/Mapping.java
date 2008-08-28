package org.safehaus.penrose.mapping;

import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.ldap.Attributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author Endi Sukma Dewata
 */
public class Mapping {

    public Logger log = LoggerFactory.getLogger(getClass());
    public boolean debug = log.isDebugEnabled();

    public final static List<MappingRule> EMPTY = new LinkedList<MappingRule>();

    private MappingConfig mappingConfig;
    private MappingContext mappingContext;

    protected List<MappingRule> rules = new LinkedList<MappingRule>();
    protected Map<String,List<MappingRule>> rulesByName = new TreeMap<String,List<MappingRule>>();

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

        for (MappingRuleConfig ruleConfig : mappingConfig.getRuleConfigs()) {
            MappingRule rule = new MappingRule(ruleConfig);
            addRule(rule);
        }

        init();
    }

    public void init() throws Exception {
    }

    public void destroy() throws Exception {
    }

    public Collection<String> getRuleNames() {
        Collection<String> list = new ArrayList<String>();
        for (MappingRule rule : rules) {
            list.add(rule.getName());
        }
        return list;
    }

    public List<MappingRule> getRules() {
        return rules;
    }

    public void removeRules() {
        rules.clear();
        rulesByName.clear();
    }

    public void addRule(MappingRule rule) {

        String name = rule.getName().toLowerCase();

        rules.add(rule);

        List<MappingRule> list = rulesByName.get(name);
        if (list == null) {
            list = new LinkedList<MappingRule>();
            rulesByName.put(name, list);
        }
        list.add(rule);
    }

    public void addRule(int index, MappingRule rule) {

        String name = rule.getName().toLowerCase();

        rules.add(index, rule);

        List<MappingRule> list = rulesByName.get(name);
        if (list == null) {
            list = new LinkedList<MappingRule>();
            rulesByName.put(name, list);
        }

        if (list.isEmpty()) {
            list.add(rule);
        } else {
            for (MappingRule fc : list) {
                int i = rules.indexOf(fc);
                if (i < index) continue;
                list.add(i, rule);
            }
        }
    }

    public int getRuleIndex(MappingRule rule) {
        return rules.indexOf(rule);
    }

    public List<MappingRule> getRules(String name) {
        List<MappingRule> list = rulesByName.get(name.toLowerCase());
        if (list == null) return EMPTY;
        return list;
    }

    public List<MappingRule> removeRules(String name) {
         return rulesByName.remove(name.toLowerCase());
    }

    public void removeRule(MappingRule rule) {

        String name = rule.getName().toLowerCase();

        rules.remove(rule);

        Collection<MappingRule> list = rulesByName.get(name);
        if (list == null) return;

        list.remove(rule);
        if (list.isEmpty()) rulesByName.remove(name);
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

        for (MappingRule rule : getRules()) {
            String name = rule.getName();

            boolean required = rule.isRequired();
            if (!required) {
                Object value = interpreter.get(name);
                if (value != null) {
                    if (debug) log.debug(" - Skipping "+name+": value has been set");
                    continue;
                }
            }

            String variable = rule.getVariable();
            if (variable != null) {
                Object variableValue = interpreter.get(variable);
                if (variableValue == null) {
                    if (debug) log.debug(" - Skipping "+name+": "+variable+" is undefined");
                    continue;
                }
            }
            String condition = rule.getCondition();
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

            Object newValue = interpreter.eval(rule);
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

        for (String name : getRuleNames()) {
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
