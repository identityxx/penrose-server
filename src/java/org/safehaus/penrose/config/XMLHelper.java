/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.config;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.dom4j.Element;
import org.dom4j.tree.DefaultAttribute;
import org.dom4j.tree.DefaultElement;
import org.dom4j.tree.DefaultText;
import org.safehaus.penrose.module.ModuleConfig;
import org.safehaus.penrose.module.ModuleMapping;
import org.safehaus.penrose.connection.AdapterConfig;
import org.safehaus.penrose.cache.CacheConfig;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.connection.ConnectionConfig;
import org.safehaus.penrose.schema.AttributeType;
import org.safehaus.penrose.schema.ObjectClass;
import org.safehaus.penrose.interpreter.InterpreterConfig;
import org.safehaus.penrose.engine.EngineConfig;

/**
 * @author Adison Wongkar
 * @author Endi S. Dewata
 */
public class XMLHelper {

	/**
	 * Constructor 
	 */
	public XMLHelper() {
		super();
	}

	/**
	 * Convert to Server XML element
	 * 
	 * @param config the configuration
	 * @return XML Element
	 */
	public static Element toServerXmlElement(Config config) {
		Element element = new DefaultElement("server");

        // interpreters
        for (Iterator iter = config.getInterpreterConfigs().iterator(); iter.hasNext();) {
            InterpreterConfig interpreterConfig = (InterpreterConfig)iter.next();
            element.add(toElement(interpreterConfig));
        }

        // engines
        for (Iterator iter = config.getEngineConfigs().iterator(); iter.hasNext();) {
            EngineConfig engineConfig = (EngineConfig)iter.next();
            element.add(toElement(engineConfig));
        }

		// caches
        for (Iterator iter = config.getCacheConfigs().iterator(); iter.hasNext();) {
            CacheConfig cacheConfig = (CacheConfig)iter.next();
            element.add(toElement(cacheConfig));
        }

        // adapters
        for (Iterator iter = config.getAdapterConfigs().iterator(); iter.hasNext();) {
            AdapterConfig adapter = (AdapterConfig)iter.next();
            element.add(toElement(adapter));
        }

		// root
		Element rootElement = new DefaultElement("root");

        Element rootDn = new DefaultElement("root-dn");
        rootDn.add(new DefaultText(config.getRootDn()));
        rootElement.add(rootDn);

        Element rootPassword = new DefaultElement("root-password");
        rootPassword.add(new DefaultText(config.getRootPassword()));
        rootElement.add(rootPassword);

		element.add(rootElement);

		return element;
	}

	/**
	 * Convert to Mapping XML element
	 * @param config
	 * @return XML Element
	 */
	public static Element toMappingXmlElement(Config config) {
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
	
	public static Element toSourcesXmlElement(Config config) {
		Element sourcesElement = new DefaultElement("sources");

        // connections
		for (Iterator iter = config.getConnectionConfigs().iterator(); iter.hasNext();) {
			ConnectionConfig connection = (ConnectionConfig)iter.next();
			sourcesElement.add(toElement(connection));
		}

		// sources
		for (Iterator iter = config.getSourceDefinitions().iterator(); iter.hasNext();) {
			SourceDefinition source = (SourceDefinition)iter.next();
			sourcesElement.add(toElement(source));
		}
		return sourcesElement;
	}

	public static Element toModulesXmlElement(Config config) {
		Element modulesElement = new DefaultElement("modules");
		// module
		for (Iterator iter = config.getModuleConfigs().iterator(); iter.hasNext();) {
			ModuleConfig module = (ModuleConfig)iter.next();
            Element moduleElement = toElement(module);
			modulesElement.add(moduleElement);
		}
		// module-mapping
		for (Iterator iter = config.getModuleMappings().iterator(); iter.hasNext();) {
			ModuleMapping mapping = (ModuleMapping)iter.next();
			Element mappingElement = toElement(mapping);
			modulesElement.add(mappingElement);
		}
		return modulesElement;
	}

	public static Element toElement(Field field) {
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

    public static Element toElement(AdapterConfig adapter) {
        Element element = new DefaultElement("adapter");

        Element adapterName = new DefaultElement("adapter-name");
        adapterName.add(new DefaultText(adapter.getAdapterName()));
        element.add(adapterName);

        Element adapterClass = new DefaultElement("adapter-class");
        adapterClass.add(new DefaultText(adapter.getAdapterClass()));
        element.add(adapterClass);

        if (adapter.getDescription() != null) { 
            Element description = new DefaultElement("description");
            description.add(new DefaultText(adapter.getDescription()));
            element.add(description);
        }

        return element;
    }

    public static Element toElement(ConnectionConfig connection) {
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
		return element;
	}

	public static Element toElement(EntryDefinition entry, Element entryElement, Element configElement) {
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

    public static Element toElement(AttributeDefinition attribute) {
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

    public static Element toElement(Source source) {
		Element element = new DefaultElement("source");

        // name
        element.add(new DefaultAttribute("name", source.getName()));

        Element sourceName = new DefaultElement("source-name");
        sourceName.add(new DefaultText(source.getSourceName()));
        element.add(sourceName);

        SourceDefinition sourceDefinition = source.getSourceDefinition();

		// fields
		for (Iterator i=source.getFields().iterator(); i.hasNext(); ) {
			Field field = (Field)i.next();

            if (sourceDefinition.getFieldDefinition(field.getName()) == null) continue;
            
			Element fieldElement = toElement(field);
            element.add(fieldElement);
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

    public static Element toElement(Relationship relationship) {
    	Element element = new DefaultElement("relationship");

        Element expressionElement = new DefaultElement("expression");
        expressionElement.add(new DefaultText(relationship.getExpression()));
        element.add(expressionElement);

    	return element;
    }

    public static Element toElement(ObjectClass oc) {
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
    
    public static Element toElement(AttributeType at) {
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

    public static Element toElement(ModuleConfig module) {
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

    public static Element toElement(ModuleMapping mapping) {
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
    
    public static Element toElement(SourceDefinition source) {
    	Element element = new DefaultElement("source");
    	element.addAttribute("name", source.getName());

        Element connectionName = new DefaultElement("connection-name");
        connectionName.add(new DefaultText(source.getConnectionName()));
        element.add(connectionName);

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
    
    public static Element toElement(InterpreterConfig interpreterConfig) {
    	Element element = new DefaultElement("interpreter");

        Element interpreterName = new DefaultElement("interpreter-name");
        interpreterName.add(new DefaultText(interpreterConfig.getInterpreterName()));
        element.add(interpreterName);

        Element interpreterClass = new DefaultElement("interpreter-class");
        interpreterClass.add(new DefaultText(interpreterConfig.getInterpreterClass()));
        element.add(interpreterClass);

        // parameters
        for (Iterator iter = interpreterConfig.getParameterNames().iterator(); iter.hasNext();) {
            String name = (String) iter.next();
            String value = (String) interpreterConfig.getParameter(name);

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

    public static Element toElement(EngineConfig engineConfig) {
    	Element element = new DefaultElement("engine");

        Element interpreterName = new DefaultElement("engine-name");
        interpreterName.add(new DefaultText(engineConfig.getEngineName()));
        element.add(interpreterName);

        Element interpreterClass = new DefaultElement("engine-class");
        interpreterClass.add(new DefaultText(engineConfig.getEngineClass()));
        element.add(interpreterClass);

        return element;
    }

    public static Element toElement(CacheConfig cache) {
    	Element element = new DefaultElement("cache");

        Element cacheName = new DefaultElement("cache-name");
        cacheName.add(new DefaultText(cache.getCacheName()));
        element.add(cacheName);

        Element cacheClass = new DefaultElement("cache-class");
        cacheClass.add(new DefaultText(cache.getCacheClass()));
        element.add(cacheClass);

        if (cache.getDescription() != null && !cache.getDescription().equals("")) {
            Element description = new DefaultElement("description");
            description.add(new DefaultText(cache.getDescription()));
            element.add(description);
        }

        // parameters
        for (Iterator iter = cache.getParameterNames().iterator(); iter.hasNext();) {
            String name = (String) iter.next();
            String value = (String) cache.getParameter(name);

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
    
    public static Element toElement(FieldDefinition field) {
    	Element element = new DefaultElement("field");
    	element.addAttribute("name", field.getName());
    	if (field.isPrimaryKey()) element.addAttribute("primaryKey", "true");
    	return element;
    }
    
    public static Element createParameterElement(String paramName, String paramValue) {

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
