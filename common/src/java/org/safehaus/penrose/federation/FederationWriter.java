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
package org.safehaus.penrose.federation;

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
 * @author Endi S. Dewata
 */
public class FederationWriter {

    public Logger log = LoggerFactory.getLogger(getClass());

    public FederationWriter() {
    }

    public void write(File file, FederationConfig federationConfig) throws Exception {

        log.debug("Writing "+file+".");

        FileWriter fw = new FileWriter(file);
        OutputFormat format = OutputFormat.createPrettyPrint();
        format.setTrimText(false);

        XMLWriter writer = new XMLWriter(fw, format);
        writer.startDocument();

        writer.startDTD(
                "federation",
                "-//Penrose/DTD Federation "+getClass().getPackage().getSpecificationVersion()+"//EN",
                "http://penrose.safehaus.org/dtd/federation.dtd"
        );

        writer.write(createElement(federationConfig));
        writer.close();
    }

    public Element createElement(FederationConfig federationConfig)  {
        Element element = new DefaultElement("federation");

        for (Repository repository : federationConfig.getRepositories()) {
            element.add(createElement(repository));
        }

        return element;
    }

    public Element createElement(Repository repository) {
        Element element = new DefaultElement("repository");
        element.addAttribute("name", repository.getName());
        element.addAttribute("type", repository.getType());

        for (String name : repository.getParameterNames()) {
            String value = repository.getParameter(name);

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
}