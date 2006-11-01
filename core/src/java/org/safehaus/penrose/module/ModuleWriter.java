package org.safehaus.penrose.module;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.partition.Partition;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;
import org.dom4j.Element;
import org.dom4j.tree.DefaultElement;
import org.dom4j.tree.DefaultText;

import java.io.File;
import java.io.FileWriter;
import java.util.Iterator;
import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class ModuleWriter {

    Logger log = LoggerFactory.getLogger(getClass());

    public ModuleWriter() {
    }

    public void write(String directory, Partition partition) throws Exception {
        File dir = new File(directory);
        dir.mkdirs();

        File file = new File(dir, "modules.xml");

        FileWriter fw = new FileWriter(file);
        OutputFormat format = OutputFormat.createPrettyPrint();
        format.setTrimText(false);

        XMLWriter writer = new XMLWriter(fw, format);
        writer.startDocument();

        writer.startDTD(
                "modules",
                "-//Penrose/DTD Modules 1.2//EN",
                "http://penrose.safehaus.org/dtd/modules.dtd"
        );

        writer.write(toElement(partition));
        writer.close();
    }

    public Element toElement(Partition partition) {
        Element modulesElement = new DefaultElement("modules");

        for (Iterator iter = partition.getModuleConfigs().iterator(); iter.hasNext();) {
            ModuleConfig module = (ModuleConfig)iter.next();
            Element moduleElement = toElement(module);
            modulesElement.add(moduleElement);
        }

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

        for (Iterator i = moduleConfig.getParameterNames().iterator(); i.hasNext();) {
            String name = (String)i.next();
            String value = (String)moduleConfig.getParameter(name);

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
}
