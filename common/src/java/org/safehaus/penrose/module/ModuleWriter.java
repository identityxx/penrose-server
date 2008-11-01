package org.safehaus.penrose.module;

import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;
import org.dom4j.tree.DefaultElement;
import org.dom4j.tree.DefaultText;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;

/**
 * @author Endi Sukma Dewata
 */
public class ModuleWriter {

    public Logger log = LoggerFactory.getLogger(getClass());

    public void write(File file, ModuleConfigManager moduleConfigManager) throws Exception {

        log.debug("Writing "+file+".");

        Element element = createElement(moduleConfigManager);
        
        FileWriter fw = new FileWriter(file);
        OutputFormat format = OutputFormat.createPrettyPrint();
        format.setTrimText(false);

        XMLWriter writer = new XMLWriter(fw, format);
        writer.startDocument();

        writer.startDTD(
                "modules",
                "-//Penrose/DTD Modules "+getClass().getPackage().getSpecificationVersion()+"//EN",
                "http://penrose.safehaus.org/dtd/modules.dtd"
        );

        writer.write(element);
        writer.close();
    }

    public Element createElement(ModuleConfigManager moduleConfigManager) {
        Element modulesElement = new DefaultElement("modules");

        // module
        for (ModuleConfig moduleConfig : moduleConfigManager.getModuleConfigs()) {
            Element moduleElement = createElement(moduleConfig);
            modulesElement.add(moduleElement);
        }

        // module-mapping
        for (ModuleMapping moduleMapping : moduleConfigManager.getModuleMappings()) {
            Element mappingElement = createElement(moduleMapping);
            modulesElement.add(mappingElement);
        }

        return modulesElement;
    }

    public Element createElement(ModuleConfig moduleConfig) {

        Element element = new DefaultElement("module");
        element.addAttribute("name", moduleConfig.getName());
        if (!moduleConfig.isEnabled()) element.addAttribute("enabled", "false");

        if (moduleConfig.getDescription() != null) {
            Element description = new DefaultElement("description");
            description.add(new DefaultText(moduleConfig.getDescription()));
            element.add(description);
        }

        Element moduleClass = new DefaultElement("module-class");
        moduleClass.add(new DefaultText(moduleConfig.getModuleClass()));
        element.add(moduleClass);

        // parameters
        for (String name : moduleConfig.getParameterNames()) {
            String value = moduleConfig.getParameter(name);

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

    public Element createElement(ModuleMapping mapping) {
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
}
