package org.safehaus.penrose.directory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;
import org.dom4j.tree.DefaultElement;
import org.dom4j.tree.DefaultAttribute;
import org.dom4j.tree.DefaultText;
import org.safehaus.penrose.acl.ACI;
import org.safehaus.penrose.util.BinaryUtil;
import org.safehaus.penrose.schema.ObjectClass;
import org.safehaus.penrose.schema.AttributeType;
import org.safehaus.penrose.mapping.Expression;

import java.util.Collection;
import java.io.File;
import java.io.FileWriter;

/**
 * @author Endi Sukma Dewata
 */
public class DirectoryWriter {

    public Logger log = LoggerFactory.getLogger(getClass());

    public void write(File file, DirectoryConfig directoryConfig) throws Exception {

        log.debug("Writing "+file+".");

        Element element = createElement(directoryConfig);

        FileWriter fw = new FileWriter(file);
        OutputFormat format = OutputFormat.createPrettyPrint();
        format.setTrimText(false);

        XMLWriter writer = new XMLWriter(fw, format);
        writer.startDocument();

        writer.startDTD(
                "directory",
                "-//Penrose/DTD Directory "+getClass().getPackage().getSpecificationVersion()+"//EN",
                "http://penrose.safehaus.org/dtd/directory.dtd"
        );

        writer.write(element);
        writer.close();
    }

    public Element createElement(DirectoryConfig directoryConfig) throws Exception {
        Element directoryElement = new DefaultElement("directory");

        for (EntryConfig entryConfig : directoryConfig.getRootEntryConfigs()) {
            createElement(directoryConfig, entryConfig, directoryElement);
        }

        return directoryElement;
    }

    public Element createElement(DirectoryConfig directoryConfig, EntryConfig entryConfig, Element configElement) throws Exception {

        Element element = new DefaultElement("entry");

        if (!entryConfig.getDn().isEmpty()) {
            element.add(new DefaultAttribute("dn", entryConfig.getDn().toString()));
        }

        if (!entryConfig.isEnabled()) element.addAttribute("enabled", "false");
        if (!entryConfig.isAttached()) element.addAttribute("attached", "false");
        configElement.add(element);

        if (entryConfig.getEntryClass() != null) {
            Element entryClassElement = new DefaultElement("entry-class");
            entryClassElement.setText(entryConfig.getEntryClass());
            element.add(entryClassElement);
        }

        if (entryConfig.getDescription() != null) {
            Element descriptionElement = new DefaultElement("description");
            descriptionElement.setText(entryConfig.getDescription());
            element.add(descriptionElement);
        }

        String mappingName = entryConfig.getMappingName();
        if (mappingName != null) {
            Element mappingNameElement = new DefaultElement("mapping-name");
            mappingNameElement.setText(mappingName);
            element.add(mappingNameElement);
        }

        for (String objectClass : entryConfig.getObjectClasses()) {
            Element objectClassElement = new DefaultElement("oc");
            objectClassElement.setText(objectClass);
            element.add(objectClassElement);
        }

        Collection<EntryAttributeConfig> attributes = entryConfig.getAttributeConfigs();
        for (EntryAttributeConfig attributeConfig : attributes) {

            Element child = createElement(attributeConfig);
            if (child == null) continue;

            element.add(child);
        }

        for (EntrySourceConfig sourceConfig : entryConfig.getSourceConfigs()) {
            element.add(createElement(sourceConfig));
        }

        for (ACI aci : entryConfig.getACL()) {
            element.add(createElement(aci));
        }

        for (String name : entryConfig.getParameterNames()) {
            String value = entryConfig.getParameter(name);

            Element parameter = new DefaultElement("parameter");

            Element paramName = new DefaultElement("param-name");
            paramName.add(new DefaultText(name));
            parameter.add(paramName);

            Element paramValue = new DefaultElement("param-value");
            paramValue.add(new DefaultText(value));
            parameter.add(paramValue);

            element.add(parameter);
        }

        Collection<EntryConfig> children = directoryConfig.getChildren(entryConfig);
        for (EntryConfig child : children) {
            createElement(directoryConfig, child, configElement);
        }

        return element;
    }

    public Element createElement(EntryAttributeConfig attributeConfig) throws Exception {
        Element element = new DefaultElement("at");
        element.add(new DefaultAttribute("name", attributeConfig.getName()));
        if (attributeConfig.isRdn()) element.add(new DefaultAttribute("rdn", "true"));
        if (attributeConfig.getEncryption() != null) element.add(new DefaultAttribute("encryption", attributeConfig.getEncryption()));

        if (attributeConfig.getConstant() != null) {
            Object value = attributeConfig.getConstant();
            if (value instanceof byte[]) {
                Element e = element.addElement("binary");
                e.addText(BinaryUtil.encode(BinaryUtil.BASE64, (byte[])value));
            } else {
                Element e = element.addElement("constant");
                e.addText((String)value);
            }

        } else if (attributeConfig.getVariable() != null) {
            Element scriptElement = new DefaultElement("variable");
            scriptElement.setText(attributeConfig.getVariable());
            element.add(scriptElement);

        } else if (attributeConfig.getExpression() != null) {
            element.add(createElement(attributeConfig.getExpression()));

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

    public Element createElement(EntrySourceConfig sourceConfig) throws Exception {
        Element element = new DefaultElement("source");

        String sourceName = sourceConfig.getSourceName();
        String alias = sourceConfig.getAlias();

        if (alias != null && !alias.equals(sourceName)) {
            element.add(new DefaultAttribute("alias", alias));
        }

        if (sourceConfig.isReadOnly()) element.add(new DefaultAttribute("readOnly", "true"));

        if (sourceConfig.getSearch() != null) {
            element.add(new DefaultAttribute("search", sourceConfig.getSearch()));
        }

        if (sourceConfig.getBind() != null) {
            element.add(new DefaultAttribute("bind", sourceConfig.getBind()));
        }

        if (sourceConfig.getAdd() != null) {
            element.add(new DefaultAttribute("add", sourceConfig.getAdd()));
        }

        if (sourceConfig.getDelete() != null) {
            element.add(new DefaultAttribute("delete", sourceConfig.getDelete()));
        }

        if (sourceConfig.getModify() != null) {
            element.add(new DefaultAttribute("modify", sourceConfig.getModify()));
        }

        if (sourceConfig.getModrdn() != null) {
            element.add(new DefaultAttribute("modrdn", sourceConfig.getModrdn()));
        }

        String partitionName = sourceConfig.getPartitionName();
        if (partitionName != null) {
            Element partitionNameElement = new DefaultElement("partition-name");
            partitionNameElement.add(new DefaultText(partitionName));
            element.add(partitionNameElement);
        }

        Element sourceNameElement = new DefaultElement("source-name");
        sourceNameElement.add(new DefaultText(sourceName));
        element.add(sourceNameElement);

        if (sourceConfig.getMappingName() != null) {
            Element mappingNameElement = new DefaultElement("mapping-name");
            mappingNameElement.setText(sourceConfig.getMappingName());
            element.add(mappingNameElement);
        }

        // fields
        for (EntryFieldConfig fieldConfig : sourceConfig.getFieldConfigs()) {

            Element child = createElement(fieldConfig);
            if (child == null) continue;

            element.add(child);
        }

        // parameters
        for (String name : sourceConfig.getParameterNames()) {
            String value = sourceConfig.getParameter(name);
            if ("".equals(value)) continue;

            Element parameterElement = new DefaultElement("parameter");

            Element nameElement = new DefaultElement("param-name");
            nameElement.add(new DefaultText(name));
            parameterElement.add(nameElement);

            Element valueElement = new DefaultElement("param-value");
            valueElement.add(new DefaultText(value));
            parameterElement.add(valueElement);

            element.add(parameterElement);
        }

        return element;
    }

    public Element createElement(ACI aci) {
        Element element = new DefaultElement("aci");

        if (!ACI.SUBJECT_ANYBODY.equals(aci.getSubject())) {
            element.add(new DefaultAttribute("subject", aci.getSubject()));
        }

        if (aci.getDn() != null && !aci.getDn().isEmpty()) {
            Element dnElement = new DefaultElement("dn");
            dnElement.add(new DefaultText(aci.getDn().toString()));
            element.add(dnElement);
        }

        if (!ACI.TARGET_OBJECT.equals(aci.getTarget())) {
            Element targetElement = new DefaultElement("target");
            targetElement.add(new DefaultText(aci.getTarget()));
            element.add(targetElement);
        }

        if (aci.getAttributes() != null && !"".equals(aci.getAttributes())) {
            Element attributesElement = new DefaultElement("attributes");
            attributesElement.add(new DefaultText(aci.getAttributes()));
            element.add(attributesElement);
        }

        if (!ACI.SCOPE_SUBTREE.equals(aci.getScope())) {
            Element scopeElement = new DefaultElement("scope");
            scopeElement.add(new DefaultText(aci.getScope()));
            element.add(scopeElement);
        }

        if (!ACI.ACTION_GRANT.equals(aci.getAction())) {
            Element actionElement = new DefaultElement("action");
            actionElement.add(new DefaultText(aci.getAction()));
            element.add(actionElement);
        }

        Element permissionElement = new DefaultElement("permission");
        permissionElement.add(new DefaultText(aci.getPermission()));
        element.add(permissionElement);

        return element;
    }

    public Element createElement(ObjectClass oc) {
        Element element = new DefaultElement("objectclass");

        Element oidElement = new DefaultElement("oid");
        oidElement.add(new DefaultText(oc.getOid()));
        element.add(oidElement);

        Element namesElement = new DefaultElement("names");
        for (String name : oc.getNames()) {
            Element nameElement = new DefaultElement("name");
            nameElement.add(new DefaultText(name));
            namesElement.add(nameElement);
        }
        element.add(namesElement);

        Element descElement = new DefaultElement("description");
        if (oc.getDescription() != null) descElement.add(new DefaultText(oc.getDescription()));
        element.add(descElement);

        Element obsoleteElement = new DefaultElement("obsolete");
        obsoleteElement.add(new DefaultText(Boolean.toString(oc.isObsolete())));
        element.add(obsoleteElement);

        Element superclassesElement = new DefaultElement("superclasses");
        for (String superClass : oc.getSuperClasses()) {
            Element ocElement = new DefaultElement("oc");
            ocElement.add(new DefaultText(superClass));
            superclassesElement.add(ocElement);
        }
        element.add(superclassesElement);

        Element typeElement = new DefaultElement("type");
        typeElement.add(new DefaultText(oc.getType()));
        element.add(typeElement);

        Element requiredAttributesElement = new DefaultElement("required-attributes");
        for (String name : oc.getRequiredAttributes()) {
            Element atElement = new DefaultElement("at");
            atElement.add(new DefaultText(name));
            requiredAttributesElement.add(atElement);
        }
        element.add(requiredAttributesElement);

        Element optionalAttributesElement = new DefaultElement("optional-attributes");
        for (String name : oc.getOptionalAttributes()) {
            Element atElement = new DefaultElement("at");
            atElement.add(new DefaultText(name));
            optionalAttributesElement.add(atElement);
        }
        element.add(optionalAttributesElement);

        return element;
    }

    public Element createElement(AttributeType at) {
        Element element = new DefaultElement("attributetype");

        Element oidElement = new DefaultElement("oid");
        oidElement.add(new DefaultText(at.getOid()));
        element.add(oidElement);

        Element namesElement = new DefaultElement("names");
        for (String name : at.getNames()) {
            Element nameElement = new DefaultElement("name");
            nameElement.add(new DefaultText(name));
            namesElement.add(nameElement);
        }
        element.add(namesElement);

        Element descElement = new DefaultElement("description");
        if (at.getDescription() != null) descElement.add(new DefaultText(at.getDescription()));
        element.add(descElement);

        Element obsoleteElement = new DefaultElement("obsolete");
        obsoleteElement.add(new DefaultText(Boolean.toString(at.isObsolete())));
        element.add(obsoleteElement);

        Element superclassElement = new DefaultElement("superclass");
        superclassElement.add(new DefaultText(at.getSuperClass()));
        element.add(superclassElement);

        Element equalityElement = new DefaultElement("equality");
        equalityElement.add(new DefaultText(at.getEquality()));
        element.add(equalityElement);

        Element orderingElement = new DefaultElement("ordering");
        orderingElement.add(new DefaultText(at.getOrdering()));
        element.add(orderingElement);

        Element substringElement = new DefaultElement("substring");
        substringElement.add(new DefaultText(at.getSubstring()));
        element.add(substringElement);

        Element syntaxElement = new DefaultElement("syntax");
        syntaxElement.add(new DefaultText(at.getSyntax()));
        element.add(syntaxElement);

        Element singleValuedElement = new DefaultElement("single-valued");
        singleValuedElement.add(new DefaultText(Boolean.toString(at.isSingleValued())));
        element.add(singleValuedElement);

        Element collectiveElement = new DefaultElement("collective");
        collectiveElement.add(new DefaultText(Boolean.toString(at.isCollective())));
        element.add(collectiveElement);

        Element modifiableElement = new DefaultElement("modifiable");
        modifiableElement.add(new DefaultText(Boolean.toString(at.isModifiable())));
        element.add(modifiableElement);

        Element usageElement = new DefaultElement("usage");
        usageElement.add(new DefaultText(at.getUsage()));
        element.add(usageElement);

        return element;
    }

    public Element createElement(EntryFieldConfig fieldConfig) throws Exception {
        Element element = new DefaultElement("field");
        element.add(new DefaultAttribute("name", fieldConfig.getName()));
        if (fieldConfig.getEncryption() != null) element.add(new DefaultAttribute("encryption", fieldConfig.getEncryption()));

        if (!fieldConfig.getOperations().isEmpty()) {
            StringBuilder sb = new StringBuilder();
            for (String operation : fieldConfig.getOperations()) {
                if (sb.length() > 0) sb.append(",");
                sb.append(operation);
            }
        }

        if (fieldConfig.getConstant() != null) {
            Object value = fieldConfig.getConstant();
            if (value instanceof byte[]) {
                Element e = element.addElement("binary");
                e.addText(BinaryUtil.encode(BinaryUtil.BASE64, (byte[])value));
            } else {
                Element e = element.addElement("constant");
                e.addText((String)value);
            }

        } else if (fieldConfig.getVariable() != null) {
            Element scriptElement = new DefaultElement("variable");
            scriptElement.setText(fieldConfig.getVariable());
            element.add(scriptElement);

        } else if (fieldConfig.getExpression() != null) {
            element.add(createElement(fieldConfig.getExpression()));

        } else {
            return null;
        }

        return element;
    }
}
