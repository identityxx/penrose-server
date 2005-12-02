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
package org.safehaus.penrose.management;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.safehaus.penrose.Penrose;

import java.util.Collection;
import java.util.ArrayList;
import java.util.TreeSet;
import java.util.Enumeration;
import java.io.File;
import java.io.IOException;
import java.io.FileInputStream;
import java.io.FileOutputStream;


/**
 * @author Endi S. Dewata
 */
public class PenroseAdmin implements PenroseAdminMBean {

    Logger log = Logger.getLogger(PenroseAdmin.class);

    private Penrose penrose;

    public PenroseAdmin() {
    }

    public String getProductName() {
        return Penrose.PRODUCT_NAME;
    }

    public void setProductName(String name) {
    }

    public String getProductVersion() {
        return Penrose.PRODUCT_VERSION;
    }

    public void setProductVersion(String version) {
    }

    public Collection listFiles(String directory) throws Exception {
        Collection results = new ArrayList();

        String homeDirectory = penrose.getPenroseConfig().getHome();
        File file = new File((homeDirectory == null ? "" : homeDirectory+File.separator)+directory);
        if (!file.exists()) return results;

        File children[] = file.listFiles();
        for (int i=0; i<children.length; i++) {
            if (children[i].isDirectory()) {
                results.addAll(listFiles(directory+File.separator+children[i].getName()));
            } else {
                results.add(directory+File.separator+children[i].getName());
            }
        }
        return results;
    }

    public Collection getLoggerNames(String path) throws Exception {
        //log.debug("Loggers under "+path);
        Collection loggerNames = new TreeSet();

        Enumeration e = LogManager.getCurrentLoggers();
        while (e.hasMoreElements()) {
    		Logger logger = (Logger)e.nextElement();
    		//log.debug(" - "+logger.getName()+": "+logger.getEffectiveLevel());
            loggerNames.add(logger.getName());
    	}

        return loggerNames;
    }

    public byte[] download(String filename) throws Exception {
        String homeDirectory = penrose.getPenroseConfig().getHome();
        File file = new File((homeDirectory == null ? "" : homeDirectory+File.separator)+filename);
        if (!file.exists()) return null;

        log.debug("Downloading "+file.getAbsolutePath());

        FileInputStream in = new FileInputStream(file);
        byte content[] = new byte[(int)file.length()];
        in.read(content);
        in.close();

        return content;
    }

    public void upload(String filename, byte content[]) throws Exception {
        String homeDirectory = penrose.getPenroseConfig().getHome();
        File file = new File((homeDirectory == null ? "" : homeDirectory+File.separator)+filename);
        file.getParentFile().mkdirs();

        log.debug("Uploading "+file.getAbsolutePath());

        FileOutputStream out = new FileOutputStream(file);
        out.write(content);
        out.close();
    }

    public Penrose getPenrose() {
        return penrose;
    }

    public void setPenrose(Penrose penrose) {
        this.penrose = penrose;
    }
}
