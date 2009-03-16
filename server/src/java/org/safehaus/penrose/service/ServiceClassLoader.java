package org.safehaus.penrose.service;

import org.safehaus.penrose.util.PenroseClassLoader;

import java.io.File;
import java.io.FilenameFilter;

/**
 * @author Endi Sukma Dewata
 */
public class ServiceClassLoader extends PenroseClassLoader {

    public ServiceClassLoader(File serviceDir, ClassLoader parent) throws Exception {
        super(parent);

        boolean debug = log.isDebugEnabled();
        if (debug) log.debug("Creating service class loader:");

        File baseDir = new File(serviceDir, "SERVICE-INF");

        File classesDir = new File(baseDir, "classes");
        if (classesDir.isDirectory()) {
            if (debug) log.debug(" - "+classesDir);
            classPaths.add(classesDir);
        }

        File libDir = new File(baseDir, "lib");
        if (libDir.isDirectory()) {
            File files[] = libDir.listFiles(new FilenameFilter() {
                public boolean accept(File dir, String name) {
                    return name.toLowerCase().endsWith(".jar");
                }
            });

            for (File f : files) {
                if (debug) log.debug(" - "+f);
                classPaths.add(f);
            }
        }
    }
}