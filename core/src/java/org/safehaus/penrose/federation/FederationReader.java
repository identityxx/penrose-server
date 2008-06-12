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

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.apache.commons.digester.Digester;
import org.apache.commons.digester.xmlrules.DigesterLoader;
import org.xml.sax.EntityResolver;
import org.xml.sax.InputSource;

import java.io.File;
import java.io.IOException;
import java.net.URL;

/**
 * @author Endi S. Dewata
 */
public class FederationReader implements EntityResolver {

    public Logger log = LoggerFactory.getLogger(getClass());

    URL dtdUrl;
    URL digesterUrl;

    Digester digester;

    public FederationReader() {

        ClassLoader cl = getClass().getClassLoader();

        dtdUrl = cl.getResource("org/safehaus/penrose/federation/federation.dtd");
        digesterUrl = cl.getResource("org/safehaus/penrose/federation/federation-digester-rules.xml");

        digester = DigesterLoader.createDigester(digesterUrl);
        digester.setEntityResolver(this);
        digester.setValidating(true);
        digester.setClassLoader(cl);
    }

    public FederationConfig read(File file) throws Exception {
        FederationConfig federationConfig = new FederationConfig();
        read(file, federationConfig);
        return federationConfig;
    }

    public void read(File file, FederationConfig federationConfig) throws Exception {
        digester.push(federationConfig);
        digester.parse(file);
        digester.pop();
    }

    public InputSource resolveEntity(String publicId, String systemId) throws IOException {
        return new InputSource(dtdUrl.openStream());
    }
}