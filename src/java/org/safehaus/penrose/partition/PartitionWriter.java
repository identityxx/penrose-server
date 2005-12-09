/**
 * Copyright (c) 2000-2005, Identyx Corporation.
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

import java.util.Iterator;
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
import org.safehaus.penrose.partition.Partition;
import org.apache.log4j.Logger;

/**
 * @author Endi S. Dewata
 */
public class PartitionWriter {

    Logger log = Logger.getLogger(getClass());

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
		/*
		writer.startDTD("mapping",
				"-//Penrose/Penrose Server Configuration DTD 1.0//EN",
				"http://penrose.safehaus.org/dtd/penrose-mapping-config-1.0.dtd");
				*/
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
		/*
		writer.startDTD("mapping",
				"-//Penrose/Penrose Server Configuration DTD 1.0//EN",
				"http://penrose.safehaus.org/dtd/penrose-mapping-config-1.0.dtd");
				*/
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
		/*
		writer.startDTD("mapping",
				"-//Penrose/Penrose Server Configuration DTD 1.0//EN",
				"http://penrose.safehaus.org/dtd/penrose-mapping-config-1.0.dtd");
				*/
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
		/*
		writer.startDTD("mapping",
				"-//Penrose/Penrose Server Configuration DTD 1.0//EN",
				"http://penrose.safehaus.org/dtd/penrose-mapping-config-1.0.dtd");
				*/
		writer.write(toModulesXmlElement(partition));
		writer.close();
    }

	public Element toMappingXmlElement(Partition partition) {
		Element mappingElement = new DefaultElement("mapping");

		for (Iterator iter = partition.getRootEntryMappings().iterator(); iter.hasNext();) {
			EntryMapping entry = (EntryMapping)iter.next();
            toElement(partition, entry, mappingElement);
		}

		return mappingElement;
	}

    public Element toConnectionsXmlElement(Partition partition) {
        Element element = new DefaultElement("connections");

        for (Iterator i = partition.getConnectionConfigs().iterator(); i.hasNext();) {
            ConnectionConfig connection = (ConnectionConfig)i.next();
            element.add(toElement(connection));
        }

        return element;
    }

	public Element toSourcesXmlElement(Partition partition) {
		Element element = new DefaultElement("sources");

        for (Iterator i = partition.getSourceConfigs().iterator(); i.hasNext(); ) {
            SourceConfig sourceConfig = (SourceConfig)i.next();
            element.add(toElement(sourceConfig));
        }

		return element;
	}

	public Element toModulesXmlElement(Partition partition) {
		Element modulesElement = new DefaultElement("modules");
		// module
		for (Iterator iter = partition.getModuleConfigs().iterator(); iter.hasNext();) {
			ModuleConfig module = (ModuleConfig)iter.next();
            Element moduleElement = toElement(module);
			modulesElement.add(moduleElement);
		}
		// module-mapping
		for (Iterator i = partition.getModuleMappings().iterator(); i.hasNext();) {
            Collection c = (Collection)i.next();

            for (Iterator j = c.iterator(); j.hasNext(); ) {
                ModuleMapping mapping = (ModuleMapping)j.next();
                Element mappingElement = toElement(mapping);
                modulesElement.add(mappingElement);
            }
		}
		return modulesElement;
	}

	public Element toElement(FieldMapping fieldMapping) {
		Element element = new DefaultElement("field");
		element.add(new DefaultAttribute("name", fieldMapping.getName()));

        if (fieldMapping.getConstant() != null) {
            Element scriptElement = new DefaultElement("constant");
            scriptElement.setText(fieldMapping.getConstant());
            element.add(scriptElement);

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
		for (Iterator iter = connection.parameters.keySet().iterator(); iter.hasNext();) {
			String name = (String) iter.next();
			String value = (String) connection.parameters.get(name);

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

    public Element toElement(SourceConfig source) {
    	Element element = new DefaultElement("source");
    	element.addAttribute("name", source.getName());

        Element adapterName = new DefaultElement("connection-name");
        adapterName.add(new DefaultText(source.getConnectionName()));
        element.add(adapterName);

    	for (Iterator i = source.getFieldConfigs().iterator(); i.hasNext(); ) {
    		FieldConfig field = (FieldConfig)i.next();
    		Element fieldElement = toElement(field);
    		element.add(fieldElement);
    	}

        for (Iterator i = source.getParameterNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            String value = source.getParameter(name);
            if ("".equals(value)) continue;

            Element parameterElement = createParameterElement(name, value);
            element.add(parameterElement);
        }

    	return element;
    }

	public Element toElement(Partition partition, EntryMapping entry, Element configElement) {

        Element entryElement = new DefaultElement("entry");
        entryElement.add(new DefaultAttribute("dn", entry.getDn()));
        configElement.add(entryElement);

		for (Iterator i=entry.getObjectClasses().iterator(); i.hasNext(); ) {
			String objectClass = (String)i.next();
			Element objectClassElement = new DefaultElement("oc");
			objectClassElement.setText(objectClass);
			entryElement.add(objectClassElement);
		}

		Collection attributes = entry.getAttributeMappings();
		for (Iterator i = attributes.iterator(); i.hasNext(); ) {
			AttributeMapping attribute = (AttributeMapping)i.next();

            Element child = toElement(attribute);
            if (child == null) continue;

            entryElement.add(child);
		}

		for (Iterator i = entry.getSourceMappings().iterator(); i.hasNext(); ) {
			SourceMapping sourceMapping = (SourceMapping)i.next();
            entryElement.add(toElement(sourceMapping));
		}

		for (Iterator i = entry.getRelationships().iterator(); i.hasNext(); ) {
			Relationship relationship = (Relationship)i.next();
			entryElement.add(toElement(relationship));
		}

        for (Iterator i = entry.getACL().iterator(); i.hasNext(); ) {
            ACI aci = (ACI)i.next();
            entryElement.add(toElement(aci));
        }

        for (Iterator i = entry.getParameterNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            String value = entry.getParameter(name);
            if ("".equals(value)) continue;

            Element parameterElement = createParameterElement(name, value);
            entryElement.add(parameterElement);
        }

        Collection children = partition.getChildren(entry);
        for (Iterator i = children.iterator(); i.hasNext(); ) {
            EntryMapping child = (EntryMapping)i.next();
            toElement(partition, child, configElement);
        }

		return entryElement;
	}

    public Element toElement(AttributeMapping attributeMapping) {
    	Element element = new DefaultElement("at");
    	element.add(new DefaultAttribute("name", attributeMapping.getName()));
    	if (attributeMapping.isRdn()) element.add(new DefaultAttribute("rdn", "true"));
        if (!AttributeMapping.DEFAULT_TYPE.equals(attributeMapping.getType())) element.addAttribute("type", attributeMapping.getType());
        if (attributeMapping.getLength() != AttributeMapping.DEFAULT_LENGTH) element.addAttribute("length", ""+attributeMapping.getLength());
        if (attributeMapping.getPrecision() != AttributeMapping.DEFAULT_PRECISION) element.addAttribute("precision", ""+attributeMapping.getPrecision());

        if (attributeMapping.getScript() != null) {
            Element scriptElement = new DefaultElement("script");
            scriptElement.setText(attributeMapping.getScript());
            element.add(scriptElement);
        }

        if (attributeMapping.getConstant() != null) {
            Element scriptElement = new DefaultElement("constant");
            scriptElement.setText(attributeMapping.getConstant());
            element.add(scriptElement);

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

    public Element toElement(SourceMapping sourceMapping) {
		Element element = new DefaultElement("source");

        element.add(new DefaultAttribute("name", sourceMapping.getName()));
        if (!sourceMapping.isRequired()) element.add(new DefaultAttribute("required", "false"));
        if (sourceMapping.isReadOnly()) element.add(new DefaultAttribute("readOnly", "true"));
        if (!sourceMapping.isIncludeOnAdd()) element.add(new DefaultAttribute("includeOnAdd", "false"));
        if (!sourceMapping.isIncludeOnModify()) element.add(new DefaultAttribute("includeOnModify", "false"));
        if (!sourceMapping.isIncludeOnModRdn()) element.add(new DefaultAttribute("includeOnModRdn", "false"));
        if (!sourceMapping.isIncludeOnDelete()) element.add(new DefaultAttribute("includeOnDelete", "false"));

        Element sourceName = new DefaultElement("source-name");
        sourceName.add(new DefaultText(sourceMapping.getSourceName()));
        element.add(sourceName);

		// fields
		for (Iterator i=sourceMapping.getFieldMappings().iterator(); i.hasNext(); ) {
			FieldMapping fieldMapping = (FieldMapping)i.next();

            Element child = toElement(fieldMapping);
            if (child == null) continue;

            element.add(child);
		}

    	// parameters
        for (Iterator i = sourceMapping.getParameterNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
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

    public Element toElement(Relationship relationship) {
    	Element element = new DefaultElement("relationship");

        Element expressionElement = new DefaultElement("expression");
        expressionElement.add(new DefaultText(relationship.getExpression()));
        element.add(expressionElement);

    	return element;
    }

    public Element toElement(ACI aci) {
    	Element element = new DefaultElement("aci");

        if (!ACI.SUBJECT_ANYBODY.equals(aci.getSubject())) {
            element.add(new DefaultAttribute("subject", aci.getSubject()));
        }

        if (aci.getDn() != null && !"".equals(aci.getDn())) {
            Element dnElement = new DefaultElement("dn");
            dnElement.add(new DefaultText(aci.getDn()));
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
    	for (Iterator i=oc.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
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
    	for (Iterator i=oc.getSuperClasses().iterator(); i.hasNext(); ) {
            String superClass = (String)i.next();
    		Element ocElement = new DefaultElement("oc");
    		ocElement.add(new DefaultText(superClass));
    		superclassesElement.add(ocElement);
    	}
    	element.add(superclassesElement);
    	
    	Element typeElement = new DefaultElement("type");
    	typeElement.add(new DefaultText(oc.getType()));
    	element.add(typeElement);
    	
    	Element requiredAttributesElement = new DefaultElement("required-attributes");
    	for (Iterator i=oc.getRequiredAttributes().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
    		Element atElement = new DefaultElement("at");
    		atElement.add(new DefaultText(name));
    		requiredAttributesElement.add(atElement);
    	}
    	element.add(requiredAttributesElement);
    	
    	Element optionalAttributesElement = new DefaultElement("optional-attributes");
    	for (Iterator i=oc.getOptionalAttributes().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
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
    	for (Iterator i=at.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
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

    public Element toElement(ModuleConfig module) {
        Element element = new DefaultElement("module");
        Element moduleName = new DefaultElement("module-name");
        moduleName.add(new DefaultText(module.getModuleName()));
        element.add(moduleName);
        Element moduleClass = new DefaultElement("module-class");
        moduleClass.add(new DefaultText(module.getModuleClass()));
        element.add(moduleClass);
        
        if (!module.getParameterNames().isEmpty()) {
        	Object[] paramNames = module.getParameterNames().toArray();
        	for (int i=0; i<paramNames.length; i++) {
        		String paramValue = module.getParameter(paramNames[i].toString());
        		Element parameterElement = createParameterElement(paramNames[i].toString(), paramValue);
        		element.add(parameterElement);
        	}
        }
        return element;
    }

    public Element toElement(ModuleMapping mapping) {
        Element element = new DefaultElement("module-mapping");

        Element name = new DefaultElement("module-name");
        name.add(new DefaultText(mapping.getModuleName()));
        element.add(name);

        Element dn = new DefaultElement("base-dn");
        dn.add(new DefaultText(mapping.getBaseDn()));
        element.add(dn);

        Element filter = new DefaultElement("filter");
        filter.add(new DefaultText(mapping.getFilter()));
        element.add(filter);

        Element scope = new DefaultElement("scope");
        scope.add(new DefaultText(mapping.getScope()));
        element.add(scope);

        return element;
    }
    
    public Element toElement(FieldConfig field) {
    	Element element = new DefaultElement("field");
    	element.addAttribute("name", field.getName());
        if (!field.getName().equals(field.getOriginalName())) element.addAttribute("originalName", field.getOriginalName());
        if (field.isPrimaryKey()) element.addAttribute("primaryKey", "true");
        if (!field.isSearchable()) element.addAttribute("searchable", "false");
        if (field.isUnique()) element.addAttribute("unique", "true");
        if (field.isIndex()) element.addAttribute("index", "true");
        if (!FieldConfig.DEFAULT_TYPE.equals(field.getType())) element.addAttribute("type", field.getType());
        if (field.getLength() != FieldConfig.DEFAULT_LENGTH) element.addAttribute("length", ""+field.getLength());
        if (field.getPrecision() != FieldConfig.DEFAULT_PRECISION) element.addAttribute("precision", ""+field.getPrecision());
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
