/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.config;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
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
import org.safehaus.penrose.mapping.ConnectionConfig;
import org.safehaus.penrose.schema.AttributeType;
import org.safehaus.penrose.schema.ObjectClass;
import org.safehaus.penrose.module.ModuleConfig;
import org.safehaus.penrose.module.ModuleMapping;

/**
 * @author Endi S. Dewata
 */
public class ConfigWriter {

    private Config config;

	public ConfigWriter(Config config) {
        this.config = config;
	}

    public void storeMappingConfig(String filename) throws Exception {
        File file = new File(filename);
        storeMappingConfig(file);
    }

	public void storeMappingConfig(File file) throws Exception {
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
		writer.write(toMappingXmlElement());
		writer.close();
	}

    public void storeSourcesConfig(File file) throws Exception {
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
		writer.write(toSourcesXmlElement());
		writer.close();
    }

    public void storeModulesConfig(File file) throws Exception {
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
		writer.write(toModulesXmlElement());
		writer.close();
    }

	public Element toMappingXmlElement() {
		Element mappingElement = new DefaultElement("mapping");
		// entries
		for (Iterator iter = config.getEntryDefinitions().iterator(); iter.hasNext();) {
			EntryDefinition entry = (EntryDefinition)iter.next();
			if (entry.getParent() == null) {
                Element entryElement = new DefaultElement("entry");
                entryElement.add(new DefaultAttribute("dn", entry.getDn()));
				mappingElement.add(entryElement);
                toElement(entry, entryElement, mappingElement);
			}
		}
		return mappingElement;
	}
	
	public Element toSourcesXmlElement() {
		Element sourcesElement = new DefaultElement("sources");

        // connections
		for (Iterator i = config.getConnectionConfigs().iterator(); i.hasNext();) {
			ConnectionConfig connection = (ConnectionConfig)i.next();
			sourcesElement.add(toElement(connection));
		}

		return sourcesElement;
	}

	public Element toModulesXmlElement() {
		Element modulesElement = new DefaultElement("modules");
		// module
		for (Iterator iter = config.getModuleConfigs().iterator(); iter.hasNext();) {
			ModuleConfig module = (ModuleConfig)iter.next();
            Element moduleElement = toElement(module);
			modulesElement.add(moduleElement);
		}
		// module-mapping
		for (Iterator i = config.getModuleMappings().iterator(); i.hasNext();) {
            Collection c = (Collection)i.next();

            for (Iterator j = c.iterator(); j.hasNext(); ) {
                ModuleMapping mapping = (ModuleMapping)j.next();
                Element mappingElement = toElement(mapping);
                modulesElement.add(mappingElement);
            }
		}
		return modulesElement;
	}

	public Element toElement(Field field) {
		Element element = new DefaultElement("field");
		element.add(new DefaultAttribute("name", field.getName()));

		// write expression
        if (field.getExpression() != null) {
            Element expression = new DefaultElement("expression");
            expression.add(new DefaultText(field.getExpression()));
            element.add(expression);
        }

		return element;
	}

    public Element toElement(ConnectionConfig connection) {
		Element element = new DefaultElement("connection");
        element.add(new DefaultAttribute("name", connection.getConnectionName()));

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

        Collection sourceDefinitions = connection.getSourceDefinitions();
        for (Iterator i = sourceDefinitions.iterator(); i.hasNext(); ) {
            SourceDefinition sourceDefinition = (SourceDefinition)i.next();
            Element sourceElement = toElement(sourceDefinition);
            element.add(sourceElement);
        }

		return element;
	}

    public Element toElement(SourceDefinition source) {
    	Element element = new DefaultElement("source");
    	element.addAttribute("name", source.getName());

    	// field
    	Object[] fields = source.getFields().toArray();
    	for (int i=0; i<fields.length; i++) {
    		FieldDefinition field = (FieldDefinition) fields[i];
    		Element fieldElement = toElement(field);
    		element.add(fieldElement);
    	}

    	// parameter
        if (!source.getParameterNames().isEmpty()) {
        	Object[] paramNames = source.getParameterNames().toArray();
        	for (int i=0; i<paramNames.length; i++) {
        		String paramValue = source.getParameter(paramNames[i].toString());
        		Element parameterElement = createParameterElement(paramNames[i].toString(), paramValue);
        		element.add(parameterElement);
        	}
        }

    	return element;
    }

	public Element toElement(EntryDefinition entry, Element entryElement, Element configElement) {
		// object classes
		for (int i = 0; i < entry.getObjectClasses().size(); i++) {
			String objectClass = (String) entry.getObjectClasses().get(i);
			Element objectClassElement = new DefaultElement("oc");
			objectClassElement.setText(objectClass);
			entryElement.add(objectClassElement);
		}
		// attributes
		Map attributes = entry.getAttributes();
		for (Iterator i = attributes.values().iterator(); i.hasNext(); ) {
			AttributeDefinition attribute = (AttributeDefinition)i.next();
            if (attribute.getExpression() == null) continue;
            entryElement.add(toElement(attribute));
		}
		// sources
		for (Iterator i = entry.getSources().iterator(); i.hasNext(); ) {
			Source source = (Source)i.next();
            entryElement.add(toElement(source));
		}
		// relationships
		for (Iterator i = entry.getRelationships().iterator(); i.hasNext(); ) {
			Relationship relationship = (Relationship)i.next();
			entryElement.add(toElement(relationship));
		}
		// children
		for (Iterator i = entry.getChildren().iterator(); i.hasNext(); ) {
			EntryDefinition child = (EntryDefinition)i.next();
            Element childElement = new DefaultElement("entry");
            childElement.add(new DefaultAttribute("dn", child.getDn()));
			configElement.add(childElement);
            toElement(child, childElement, configElement);
		}
		return entryElement;
	}

    public Element toElement(AttributeDefinition attribute) {
    	Element element = new DefaultElement("at");
    	element.add(new DefaultAttribute("name", attribute.getName()));
    	if (attribute.isRdn()) element.add(new DefaultAttribute("rdn", "true"));
        // expression (actually the value)
        if (attribute.getExpression() != null) {
            Element expressionElement = new DefaultElement("expression");
            expressionElement.setText(attribute.getExpression());
            element.add(expressionElement);
        }

    	return element;
    }

    public Element toElement(Source source) {
		Element element = new DefaultElement("source");

        // name
        element.add(new DefaultAttribute("name", source.getName()));

        Element sourceName = new DefaultElement("source-name");
        sourceName.add(new DefaultText(source.getSourceName()));
        element.add(sourceName);

        Element connectionName = new DefaultElement("connection-name");
        connectionName.add(new DefaultText(source.getConnectionName()));
        element.add(connectionName);

        SourceDefinition sourceDefinition = source.getSourceDefinition();

		// fields
		for (Iterator i=source.getFields().iterator(); i.hasNext(); ) {
			Field field = (Field)i.next();

            if (sourceDefinition.getFieldDefinition(field.getName()) == null) continue;
            if (field.getExpression() == null) continue;

            element.add(toElement(field));
		}
    	// parameters
        Map parameters = new HashMap(); // source.getParameters();

        for (Iterator i = parameters.keySet().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            String value = (String)parameters.get(name);

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

    public Element toElement(ObjectClass oc) {
    	Element element = new DefaultElement("objectclass");
    	
    	Element oidElement = new DefaultElement("oid");
    	oidElement.add(new DefaultText(oc.getOid()));
    	element.add(oidElement);
    	
    	Element namesElement = new DefaultElement("names");
    	for (int i=0; oc.getNames() != null && i<oc.getNames().size(); i++) {
    		Element nameElement = new DefaultElement("name");
    		nameElement.add(new DefaultText(oc.getNames().get(i).toString()));
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
    	for (int i=0; oc.getSuperClasses() != null && i<oc.getSuperClasses().size(); i++) {
    		Element ocElement = new DefaultElement("oc");
    		ocElement.add(new DefaultText(oc.getSuperClasses().get(i).toString()));
    		superclassesElement.add(ocElement);
    	}
    	element.add(superclassesElement);
    	
    	Element typeElement = new DefaultElement("type");
    	typeElement.add(new DefaultText(oc.getType()));
    	element.add(typeElement);
    	
    	Element requiredAttributesElement = new DefaultElement("required-attributes");
    	for (int i=0; oc.getRequiredAttributes() != null && i<oc.getRequiredAttributes().size(); i++) {
    		Element atElement = new DefaultElement("at");
    		atElement.add(new DefaultText(oc.getRequiredAttributes().get(i).toString()));
    		requiredAttributesElement.add(atElement);
    	}
    	element.add(requiredAttributesElement);
    	
    	Element optionalAttributesElement = new DefaultElement("optional-attributes");
    	for (int i=0; oc.getOptionalAttributes() != null && i<oc.getOptionalAttributes().size(); i++) {
    		Element atElement = new DefaultElement("at");
    		atElement.add(new DefaultText(oc.getOptionalAttributes().get(i).toString()));
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
    	for (int i=0; at.getNames() != null && i<at.getNames().size(); i++) {
    		Element nameElement = new DefaultElement("name");
    		nameElement.add(new DefaultText(at.getNames().get(i).toString()));
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
    
    public Element toElement(FieldDefinition field) {
    	Element element = new DefaultElement("field");
    	element.addAttribute("name", field.getName());
        if (!"VARCHAR".equals(field.getType())) element.addAttribute("type", field.getType());
    	if (field.isPrimaryKey()) element.addAttribute("primaryKey", "true");
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

    public Config getConfig() {
        return config;
    }

    public void setConfig(Config config) {
        this.config = config;
    }
}
