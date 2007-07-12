/**
 * Copyright (c) 2000-2006, Identyx Corporation.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.safehaus.penrose.partition;

import java.util.Collection;
import java.io.File;
import java.io.FileWriter;

import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;
import org.dom4j.tree.DefaultAttribute;
import org.dom4j.tree.DefaultElement;
import org.dom4j.tree.DefaultText;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.schema.AttributeType;
import org.safehaus.penrose.schema.ObjectClass;
import org.safehaus.penrose.module.ModuleConfig;
import org.safehaus.penrose.module.ModuleMapping;
import org.safehaus.penrose.acl.ACI;
import org.safehaus.penrose.util.BinaryUtil;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.connection.ConnectionConfig;
import org.safehaus.penrose.source.SourceConfig;
import org.safehaus.penrose.source.FieldConfig;
import org.safehaus.penrose.ldap.DN;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/**
 * @author Endi S. Dewata
 */
public class PartitionWriter {

    Logger log = LoggerFactory.getLogger(getClass());

    File directory;

    public PartitionWriter(String directory) {
        this.directory = new File(directory);
    }

    public void write(Partition partition) throws Exception {
        directory.mkdirs();

        storeMappingConfig(partition);
        storeConnectionsConfig(partition);
        storeSourcesConfig(partition);
        storeModulesConfig(partition);
    }

    public void storeMappingConfig(Partition partition) throws Exception {
        File file = new File(directory, "mapping.xml");

        FileWriter fw = new FileWriter(file);
        OutputFormat format = OutputFormat.createPrettyPrint();
        format.setTrimText(false);

        XMLWriter writer = new XMLWriter(fw, format);
        writer.startDocument();

        writer.startDTD(
                "mapping",
                "-//Penrose/DTD Mapping "+Penrose.SPECIFICATION_VERSION+"//EN",
                "http://penrose.safehaus.org/dtd/mapping.dtd"
        );

        writer.write(toMappingXmlElement(partition));
        writer.close();
    }

    public void storeConnectionsConfig(Partition partition) throws Exception {
        File file = new File(directory, "connections.xml");

        FileWriter fw = new FileWriter(file);
        OutputFormat format = OutputFormat.createPrettyPrint();
        format.setTrimText(false);

        XMLWriter writer = new XMLWriter(fw, format);
        writer.startDocument();

        writer.startDTD(
                "connections",
                "-//Penrose/DTD Connections "+Penrose.SPECIFICATION_VERSION+"//EN",
                "http://penrose.safehaus.org/dtd/connections.dtd"
        );

        writer.write(toConnectionsXmlElement(partition));
        writer.close();
    }

    public void storeSourcesConfig(Partition partition) throws Exception {
        File file = new File(directory, "sources.xml");

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

        writer.write(toSourcesXmlElement(partition));
        writer.close();
    }

    public void storeModulesConfig(Partition partition) throws Exception {
        File file = new File(directory, "modules.xml");

        FileWriter fw = new FileWriter(file);
        OutputFormat format = OutputFormat.createPrettyPrint();
        format.setTrimText(false);

        XMLWriter writer = new XMLWriter(fw, format);
        writer.startDocument();

        writer.startDTD(
                "modules",
                "-//Penrose/DTD Modules "+Penrose.SPECIFICATION_VERSION+"//EN",
                "http://penrose.safehaus.org/dtd/modules.dtd"
        );

        writer.write(toModulesXmlElement(partition));
        writer.close();
    }

    public Element toMappingXmlElement(Partition partition) throws Exception {
        Element mappingElement = new DefaultElement("mapping");

        for (EntryMapping entryMapping : partition.getMappings().getRootEntryMappings()) {
            toElement(partition, entryMapping, mappingElement);
        }

        return mappingElement;
    }

    public Element toConnectionsXmlElement(Partition partition) {
        Element element = new DefaultElement("connections");

        for (ConnectionConfig connectionConfig : partition.getConnectionConfigs()) {
            element.add(toElement(connectionConfig));
        }

        return element;
    }

    public Element toSourcesXmlElement(Partition partition) throws Exception {
        Element element = new DefaultElement("sources");

        for (SourceConfig sourceConfig : partition.getSources().getSourceConfigs()) {
            element.add(toElement(sourceConfig));
        }

        return element;
    }

    public Element toModulesXmlElement(Partition partition) {
        Element modulesElement = new DefaultElement("modules");

        // module
        for (ModuleConfig moduleConfig : partition.getModules().getModuleConfigs()) {
            Element moduleElement = toElement(moduleConfig);
            modulesElement.add(moduleElement);
        }

        // module-mapping
        for (Collection<ModuleMapping> moduleMappings : partition.getModules().getModuleMappings()) {

            for (ModuleMapping moduleMapping : moduleMappings) {
                Element mappingElement = toElement(moduleMapping);
                modulesElement.add(mappingElement);
            }
        }

        return modulesElement;
    }

    public Element toElement(FieldMapping fieldMapping) throws Exception {
        Element element = new DefaultElement("field");
        element.add(new DefaultAttribute("name", fieldMapping.getName()));

        if (!fieldMapping.getOperations().isEmpty()) {
            StringBuilder sb = new StringBuilder();
            for (String operation : fieldMapping.getOperations()) {
                if (sb.length() > 0) sb.append(",");
                sb.append(operation);
            }
        }

        if (fieldMapping.getConstant() != null) {
            Object value = fieldMapping.getConstant();
            if (value instanceof byte[]) {
                Element e = element.addElement("binary");
                e.addText(BinaryUtil.encode(BinaryUtil.BASE64, (byte[])value));
            } else {
                Element e = element.addElement("constant");
                e.addText((String)value);
            }

        } else if (fieldMapping.getVariable() != null) {
            Element scriptElement = new DefaultElement("variable");
            scriptElement.setText(fieldMapping.getVariable());
            element.add(scriptElement);

        } else if (fieldMapping.getExpression() != null) {
            element.add(toElement(fieldMapping.getExpression()));

        } else {
            return null;
        }

        return element;
    }

    public Element toElement(ConnectionConfig connection) {
        Element element = new DefaultElement("connection");
        element.add(new DefaultAttribute("name", connection.getName()));

        Element adapterName = new DefaultElement("adapter-name");
        adapterName.add(new DefaultText(connection.getAdapterName()));
        element.add(adapterName);

        // parameters
        for (String name : connection.getParameterNames()) {
            String value = connection.getParameter(name);

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

    public Element toElement(SourceConfig sourceConfig) throws Exception {
        Element element = new DefaultElement("source");
        element.addAttribute("name", sourceConfig.getName());

        Element adapterName = new DefaultElement("connection-name");
        adapterName.add(new DefaultText(sourceConfig.getConnectionName()));
        element.add(adapterName);

        for (FieldConfig fieldConfig : sourceConfig.getFieldConfigs()) {
            Element fieldElement = toElement(fieldConfig);
            element.add(fieldElement);
        }

        for (String name : sourceConfig.getParameterNames()) {
            String value = sourceConfig.getParameter(name);
            if ("".equals(value)) continue;

            Element parameterElement = createParameterElement(name, value);
            element.add(parameterElement);
        }

        return element;
    }

    public Element toElement(Partition partition, EntryMapping entryMapping, Element configElement) throws Exception {

        Element entryElement = new DefaultElement("entry");
        entryElement.add(new DefaultAttribute("dn", entryMapping.getDn().toString()));
        if (!entryMapping.isEnabled()) entryElement.add(new DefaultAttribute("enabled", "false"));
        configElement.add(entryElement);

        for (String objectClass : entryMapping.getObjectClasses()) {
            Element objectClassElement = new DefaultElement("oc");
            objectClassElement.setText(objectClass);
            entryElement.add(objectClassElement);
        }

        Collection<AttributeMapping> attributes = entryMapping.getAttributeMappings();
        for (AttributeMapping attribute : attributes) {

            Element child = toElement(attribute);
            if (child == null) continue;

            entryElement.add(child);
        }

        for (SourceMapping sourceMapping : entryMapping.getSourceMappings()) {
            entryElement.add(toElement(sourceMapping));
        }
/*
        for (Iterator i = entryMapping.getRelationships().iterator(); i.hasNext(); ) {
            Relationship relationship = (Relationship)i.next();
            entryElement.add(toElement(relationship));
        }
*/
        Link link = entryMapping.getLink();
        if (link != null) {
            Element linkElement = new DefaultElement("link");

            DN dn = link.getDn();
            if (dn != null) {
                Element partitionElement = new DefaultElement("dn");
                partitionElement.add(new DefaultText(dn.toString()));
                linkElement.add(partitionElement);
            }

            String partitionName = link.getPartitionName();
            if (partitionName != null) {
                Element partitionElement = new DefaultElement("partition");
                partitionElement.add(new DefaultText(partitionName));
                linkElement.add(partitionElement);
            }

            entryElement.add(linkElement);
        }

        String handlerName = entryMapping.getHandlerName();
        if (handlerName != null) {
            Element element = new DefaultElement("handler");
            element.add(new DefaultText(handlerName));
            entryElement.add(element);
        }

        String engineName = entryMapping.getEngineName();
        if (engineName != null) {
            Element element = new DefaultElement("engine");
            element.add(new DefaultText(engineName));
            entryElement.add(element);
        }

        for (ACI aci : entryMapping.getACL()) {
            entryElement.add(toElement(aci));
        }

        for (String name : entryMapping.getParameterNames()) {
            String value = entryMapping.getParameter(name);
            if ("".equals(value)) continue;

            Element parameterElement = createParameterElement(name, value);
            entryElement.add(parameterElement);
        }

        Collection<EntryMapping> children = partition.getMappings().getChildren(entryMapping);
        for (EntryMapping child : children) {
            toElement(partition, child, configElement);
        }

        return entryElement;
    }

    public Element toElement(AttributeMapping attributeMapping) throws Exception {
        Element element = new DefaultElement("at");
        element.add(new DefaultAttribute("name", attributeMapping.getName()));
        if (attributeMapping.isRdn()) element.add(new DefaultAttribute("rdn", "true"));
        //if (!AttributeMapping.DEFAULT_TYPE.equals(attributeMapping.getType())) element.addAttribute("type", attributeMapping.getType());
        if (attributeMapping.getLength() != AttributeMapping.DEFAULT_LENGTH) element.addAttribute("length", ""+attributeMapping.getLength());
        if (attributeMapping.getPrecision() != AttributeMapping.DEFAULT_PRECISION) element.addAttribute("precision", ""+attributeMapping.getPrecision());

        if (attributeMapping.getConstant() != null) {
            Object value = attributeMapping.getConstant();
            if (value instanceof byte[]) {
                Element e = element.addElement("binary");
                e.addText(BinaryUtil.encode(BinaryUtil.BASE64, (byte[])value));
            } else {
                Element e = element.addElement("constant");
                e.addText((String)value);
            }

        } else if (attributeMapping.getVariable() != null) {
            Element scriptElement = new DefaultElement("variable");
            scriptElement.setText(attributeMapping.getVariable());
            element.add(scriptElement);

        } else if (attributeMapping.getExpression() != null) {
            element.add(toElement(attributeMapping.getExpression()));

        } else {
            return null;
        }

        return element;
    }

    public Element toElement(Expression expression) {
        Element element = new DefaultElement("expression");
        if (expression.getForeach() != null) element.add(new DefaultAttribute("foreach", expression.getForeach()));
        if (expression.getVar() != null) element.add(new DefaultAttribute("var", expression.getVar()));

        element.setText(expression.getScript());
        return element;
    }

    public Element toElement(SourceMapping sourceMapping) throws Exception {
        Element element = new DefaultElement("source");

        element.add(new DefaultAttribute("name", sourceMapping.getName()));
        if (sourceMapping.isReadOnly()) element.add(new DefaultAttribute("readOnly", "true"));

        if (sourceMapping.getSearch() != null) {
            element.add(new DefaultAttribute("search", sourceMapping.getSearch()));
        }

        if (sourceMapping.getBind() != null) {
            element.add(new DefaultAttribute("bind", sourceMapping.getBind()));
        }

        if (sourceMapping.getAdd() != null) {
            element.add(new DefaultAttribute("add", sourceMapping.getAdd()));
        }

        if (sourceMapping.getDelete() != null) {
            element.add(new DefaultAttribute("delete", sourceMapping.getDelete()));
        }

        if (sourceMapping.getModify() != null) {
            element.add(new DefaultAttribute("modify", sourceMapping.getModify()));
        }

        if (sourceMapping.getModrdn() != null) {
            element.add(new DefaultAttribute("modrdn", sourceMapping.getModrdn()));
        }

        Element sourceName = new DefaultElement("source-name");
        sourceName.add(new DefaultText(sourceMapping.getSourceName()));
        element.add(sourceName);

        // fields
        for (FieldMapping fieldMapping : sourceMapping.getFieldMappings()) {

            Element child = toElement(fieldMapping);
            if (child == null) continue;

            element.add(child);
        }

        // parameters
        for (String name : sourceMapping.getParameterNames()) {
            String value = sourceMapping.getParameter(name);
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

    public Element toElement(ACI aci) {
        Element element = new DefaultElement("aci");

        if (!ACI.SUBJECT_ANYBODY.equals(aci.getSubject())) {
            element.add(new DefaultAttribute("subject", aci.getSubject()));
        }

        if (aci.getDn() != null && aci.getDn() != null) {
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

    public Element toElement(ObjectClass oc) {
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

    public Element toElement(AttributeType at) {
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

    public Element toElement(ModuleConfig moduleConfig) {

        Element element = new DefaultElement("module");
        element.addAttribute("name", moduleConfig.getName());
        if (!moduleConfig.isEnabled()) element.addAttribute("enabled", "false");

        Element moduleClass = new DefaultElement("module-class");
        moduleClass.add(new DefaultText(moduleConfig.getModuleClass()));
        element.add(moduleClass);

        if (moduleConfig.getDescription() != null && !"".equals(moduleConfig.getDescription())) {
            Element description = new DefaultElement("description");
            description.add(new DefaultText(moduleConfig.getDescription()));
            element.add(description);
        }

        for (String name : moduleConfig.getParameterNames()) {
            String value = moduleConfig.getParameter(name);

            Element parameterElement = createParameterElement(name, value);
            element.add(parameterElement);
        }

        return element;
    }

    public Element toElement(ModuleMapping mapping) {
        Element element = new DefaultElement("module-mapping");

        Element name = new DefaultElement("module-name");
        name.add(new DefaultText(mapping.getModuleName()));
        element.add(name);

        Element dn = new DefaultElement("base-dn");
        dn.add(new DefaultText(mapping.getBaseDn().toString()));
        element.add(dn);

        Element filter = new DefaultElement("filter");
        filter.add(new DefaultText(mapping.getFilter()));
        element.add(filter);

        Element scope = new DefaultElement("scope");
        scope.add(new DefaultText(mapping.getScope()));
        element.add(scope);

        return element;
    }

    public Element toElement(FieldConfig fieldConfig) throws Exception {
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

        if (!FieldConfig.DEFAULT_TYPE.equals(fieldConfig.getType())) {
            log.debug(" - type: "+fieldConfig.getType());
            element.addAttribute("type", fieldConfig.getType());
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
            element.add(toElement(fieldConfig.getExpression()));
        }

        return element;
    }

    public Element createParameterElement(String paramName, String paramValue) {

        Element element = new DefaultElement("parameter");

        Element name = new DefaultElement("param-name");
        name.add(new DefaultText(paramName));
        element.add(name);

        Element value = new DefaultElement("param-value");
        value.add(new DefaultText(paramValue));
        element.add(value);

        return element;
    }
}
