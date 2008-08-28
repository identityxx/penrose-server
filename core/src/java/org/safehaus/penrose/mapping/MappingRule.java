package org.safehaus.penrose.mapping;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Endi Sukma Dewata
 */
public class MappingRule {

    public Logger log = LoggerFactory.getLogger(getClass());
    public boolean debug = log.isDebugEnabled();

    private MappingRuleConfig ruleConfig;

    public MappingRule(MappingRuleConfig ruleConfig) {
        this.ruleConfig = ruleConfig;
    }

    public MappingRuleConfig getRuleConfig() {
        return ruleConfig;
    }

    public void setRuleConfig(MappingRuleConfig ruleConfig) {
        this.ruleConfig = ruleConfig;
    }

    public String getName() {
        return ruleConfig.getName();
    }

    public boolean isRequired() {
        return ruleConfig.isRequired();
    }

    public String getCondition() {
        return ruleConfig.getCondition();
    }
    
    public Object getConstant() {
        return ruleConfig.getConstant();
    }

    public byte[] getBinary() {
        return ruleConfig.getBinary();
    }

    public String getVariable() {
        return ruleConfig.getVariable();
    }

    public Expression getExpression() {
        return ruleConfig.getExpression();
    }
}
