package org.safehaus.penrose.mapping;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;
import org.dom4j.tree.DefaultElement;
import org.dom4j.tree.DefaultAttribute;
import org.dom4j.tree.DefaultText;
import org.safehaus.penrose.util.BinaryUtil;

import java.io.File;
import java.io.FileWriter;

/**
 * @author Endi Sukma Dewata
 */
public class MappingWriter {

    public Logger log = LoggerFactory.getLogger(getClass());

    public void write(File file, MappingConfigManager mappingConfigManager) throws Exception {

        log.debug("Writing "+file+".");

        FileWriter fw = new FileWriter(file);
        OutputFormat format = OutputFormat.createPrettyPrint();
        format.setTrimText(false);

        XMLWriter writer = new XMLWriter(fw, format);
        writer.startDocument();

        writer.startDTD(
                "mappings",
                "-//Penrose/DTD Mappings "+getClass().getPackage().getSpecificationVersion()+"//EN",
                "http://penrose.safehaus.org/dtd/mappings.dtd"
        );

        writer.write(createElement(mappingConfigManager));
        writer.close();
    }

    public Element createElement(MappingConfigManager mappingConfigManager) throws Exception {
        Element element = new DefaultElement("mappings");

        for (MappingConfig mappingConfig : mappingConfigManager.getMappingConfigs()) {
            element.add(createElement(mappingConfig));
        }

        return element;
    }

    public Element createElement(MappingConfig mappingConfig) throws Exception {

        Element element = new DefaultElement("mapping");
        element.add(new DefaultAttribute("name", mappingConfig.getName()));

        if (mappingConfig.getMappingClass() != null) {
            Element mappingClassElement = new DefaultElement("mapping-class");
            mappingClassElement.add(new DefaultText(mappingConfig.getMappingClass()));
            element.add(mappingClassElement);
        }

        if (mappingConfig.getDescription() != null) {
            Element descriptionElement = new DefaultElement("description");
            descriptionElement.add(new DefaultText(mappingConfig.getDescription()));
            element.add(descriptionElement);
        }

        if (mappingConfig.getPreScript() != null) {
            Element preScriptElement = new DefaultElement("pre");
            preScriptElement.add(new DefaultText(mappingConfig.getPreScript()));
            element.add(preScriptElement);
        }

        if (mappingConfig.getPostScript() != null) {
            Element postScriptElement = new DefaultElement("post");
            postScriptElement.add(new DefaultText(mappingConfig.getPostScript()));
            element.add(postScriptElement);
        }

        for (MappingRuleConfig fieldMapping : mappingConfig.getRuleConfigs()) {
            Element fieldElement = createElement(fieldMapping);
            element.add(fieldElement);
        }

        for (String name : mappingConfig.getParameterNames()) {
            String value = mappingConfig.getParameter(name);

            Element parameter = new DefaultElement("parameter");

            Element paramName = new DefaultElement("param-name");
            paramName.add(new DefaultText(name));
            parameter.add(paramName);

            Element paramValue = new DefaultElement("param-value");
            paramValue.add(new DefaultText(value));
            parameter.add(paramValue);

            element.add(parameter);
        }

        return element;
    }

    public Element createElement(MappingRuleConfig ruleConfig) throws Exception {
        
        Element element = new DefaultElement("field");
        element.add(new DefaultAttribute("name", ruleConfig.getName()));

        if (!ruleConfig.isRequired()) {
            element.add(new DefaultAttribute("required", "false"));
        }

        if (ruleConfig.getCondition() != null) {
            Element e = element.addElement("condition");
            e.addText(ruleConfig.getCondition());
        }

        if (ruleConfig.getConstant() != null) {
            Object value = ruleConfig.getConstant();
            if (value instanceof byte[]) {
                Element e = element.addElement("binary");
                e.addText(BinaryUtil.encode(BinaryUtil.BASE64, (byte[])value));
            } else {
                Element e = element.addElement("constant");
                e.addText((String)value);
            }

        } else if (ruleConfig.getVariable() != null) {
            Element e = element.addElement("variable");
            e.addText(ruleConfig.getVariable());

        } else if (ruleConfig.getExpression() != null) {
            element.add(createElement(ruleConfig.getExpression()));

        } else {
            return null;
        }

        return element;
    }

    public Element createElement(Expression expression) {

        Element element = new DefaultElement("expression");
        if (expression.getForeach() != null) element.add(new DefaultAttribute("foreach", expression.getForeach()));
        if (expression.getVar() != null) element.add(new DefaultAttribute("var", expression.getVar()));

        element.setText(expression.getScript());

        return element;
    }
}