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
package org.safehaus.penrose;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.net.URL;
import java.util.Properties;
import java.io.File;

/**
 * @author Endi S. Dewata
 */
public class PenroseFactory {

    public static Logger log = LoggerFactory.getLogger(PenroseFactory.class);

    public static PenroseFactory instance;

    public static PenroseFactory getInstance() throws Exception {
        if (instance == null) {

            String factoryClassName = PenroseFactory.class.getName();

            try {
                ClassLoader cl = PenroseFactory.class.getClassLoader();
                URL url = cl.getResource("org/safehaus/penrose/penrose.properties");
                if (url != null) {
	                Properties properties = new Properties();
	                properties.load(url.openStream());
	
	                String s = properties.getProperty("org.safehaus.penrose.factory");
	                if (s != null) factoryClassName = s;
                }

            } catch (Exception e) {
                // ignore
            }

            //log.debug("Initializing "+factoryClassName);
            Class clazz = Class.forName(factoryClassName);
            instance = (PenroseFactory)clazz.newInstance();
        }
        return instance;
    }

    public Penrose createPenrose() throws Exception {
        return new Penrose();
    }

    public Penrose createPenrose(String home) throws Exception {
        return new Penrose(home);
    }

    public Penrose createPenrose(File home) throws Exception {
        return new Penrose(home);
    }

    public Penrose createPenrose(String home, PenroseConfig penroseConfig) throws Exception {
        return new Penrose(home, penroseConfig);
    }

    public Penrose createPenrose(File home, PenroseConfig penroseConfig) throws Exception {
        return new Penrose(home, penroseConfig);
    }

    public Penrose createPenrose(PenroseConfig penroseConfig) throws Exception {
        return new Penrose(penroseConfig);
    }
}
