package org.safehaus.penrose.source;

import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;
import org.dom4j.Element;
import org.dom4j.tree.DefaultElement;
import org.dom4j.tree.DefaultText;
import org.dom4j.tree.DefaultAttribute;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.mapping.Expression;
import org.safehaus.penrose.util.BinaryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;

/**
 * @author Endi Sukma Dewata
 */
public class SourceWriter {

    public Logger log = LoggerFactory.getLogger(getClass());

    public void write(File file, SourceConfigs sourceConfigs) throws Exception {

        log.debug("Writing "+file+".");

        FileWriter fw = new FileWriter(file);
        OutputFormat format = OutputFormat.createPrettyPrint();
        format.setTrimText(false);

        XMLWriter writer = new XMLWriter(fw, format);
        writer.startDocument();

        writer.startDTD(
                "sources",
                "-//Penrose/DTD Sources "+Penrose.SPECIFICATION_VERSION+"//EN",
                "http://penrose.safehaus.org/dtd/sources.dtd"
        );

        writer.write(createElement(sourceConfigs));
        writer.close();
    }

    public Element createElement(SourceConfigs sources) throws Exception {
        Element element = new DefaultElement("sources");

        for (SourceConfig sourceConfig : sources.getSourceConfigs()) {
            element.add(createElement(sourceConfig));
        }

        return element;
    }

    public Element createElement(SourceConfig sourceConfig) throws Exception {
        Element element = new DefaultElement("source");
        element.addAttribute("name", sourceConfig.getName());

        Element adapterName = new DefaultElement("connection-name");
        adapterName.add(new DefaultText(sourceConfig.getConnectionName()));
        element.add(adapterName);

        for (FieldConfig fieldConfig : sourceConfig.getFieldConfigs()) {
            Element fieldElement = createElement(fieldConfig);
            element.add(fieldElement);
        }

        for (IndexConfig indexConfig : sourceConfig.getIndexConfigs()) {
            Element indexElement = createElement(indexConfig);
            element.add(indexElement);
        }

        for (String name : sourceConfig.getParameterNames()) {
            String value = sourceConfig.getParameter(name);

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

    public Element createElement(IndexConfig indexConfig) throws Exception {
        log.debug("Index "+indexConfig.getName()+":");

        Element indexElement = new DefaultElement("index");
        if (indexConfig.getName() != null) indexElement.addAttribute("name", indexConfig.getName());

        for (String fieldName : indexConfig.getFieldNames()) {
            Element fieldNameElement = new DefaultElement("field-name");
            fieldNameElement.add(new DefaultText(fieldName));
            indexElement.add(fieldNameElement);
        }

        return indexElement;
    }

    public Element createElement(FieldConfig fieldConfig) throws Exception {
        log.debug("Field "+fieldConfig.getName()+":");
        Element element = new DefaultElement("field");
        element.addAttribute("name", fieldConfig.getName());

        if (!fieldConfig.getName().equals(fieldConfig.getOriginalName())) {
            log.debug(" - originalName: "+fieldConfig.getOriginalName());
            element.addAttribute("originalName", fieldConfig.getOriginalName());
        }

        if (fieldConfig.isPrimaryKey()) {
            log.debug(" - primaryKey: "+fieldConfig.isPrimaryKey());
            element.addAttribute("primaryKey", "true");
        }

        if (!fieldConfig.isSearchable()) {
            log.debug(" - searchable: "+fieldConfig.isSearchable());
            element.addAttribute("searchable", "false");
        }

        if (fieldConfig.isUnique()) {
            log.debug(" - unique: "+fieldConfig.isUnique());
            element.addAttribute("unique", "true");
        }

        if (fieldConfig.isIndex()) {
            log.debug(" - index: "+fieldConfig.isIndex());
            element.addAttribute("index", "true");
        }

        if (fieldConfig.isCaseSensitive()) {
            log.debug(" - caseSensitive: "+fieldConfig.isCaseSensitive());
            element.addAttribute("caseSensitive", "true");
        }

        if (fieldConfig.isAutoIncrement()) {
            log.debug(" - autoIncrement: "+fieldConfig.isAutoIncrement());
            element.addAttribute("autoIncrement", "true");
        }

        if (!FieldConfig.DEFAULT_TYPE.equals(fieldConfig.getType())) {
            log.debug(" - type: "+fieldConfig.getType());
            element.addAttribute("type", fieldConfig.getType());
        }

        if (fieldConfig.getOriginalType() != null) {
            log.debug(" - originalType: "+fieldConfig.getOriginalType());
            element.addAttribute("originalType", fieldConfig.getOriginalType());
        }

        if (fieldConfig.getCastType() != null) {
            log.debug(" - castType: "+fieldConfig.getCastType());
            element.addAttribute("castType", fieldConfig.getCastType());
        }

        if (fieldConfig.getLength() != fieldConfig.getDefaultLength()) {
            log.debug(" - length: "+fieldConfig.getLength());
            element.addAttribute("length", ""+ fieldConfig.getLength());
        }

        if (fieldConfig.getPrecision() != FieldConfig.DEFAULT_PRECISION) {
            log.debug(" - precision: "+fieldConfig.getPrecision());
            element.addAttribute("precision", ""+ fieldConfig.getPrecision());
        }

        if (fieldConfig.getConstant() != null) {
            Object value = fieldConfig.getConstant();
            if (value instanceof byte[]) {
                String s = BinaryUtil.encode(BinaryUtil.BASE64, (byte[])value);
                log.debug(" - binary: "+s);
                Element e = element.addElement("binary");
                e.addText(s);
            } else {
                log.debug(" - constant: "+value);
                Element e = element.addElement("constant");
                e.addText((String)value);
            }

        } else if (fieldConfig.getVariable() != null) {
            log.debug(" - variable: "+fieldConfig.getVariable());
            Element scriptElement = new DefaultElement("variable");
            scriptElement.setText(fieldConfig.getVariable());
            element.add(scriptElement);

        }

        Expression expression = fieldConfig.getExpression();
        if (expression != null) {
            log.debug(" - expression foreach: "+expression.getForeach());
            log.debug(" - expression var: "+expression.getVar());
            log.debug(" - expression script: "+expression.getScript());
            element.add(createElement(fieldConfig.getExpression()));
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
