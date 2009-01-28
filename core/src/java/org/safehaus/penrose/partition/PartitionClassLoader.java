package org.safehaus.penrose.partition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.FileInputStream;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Collection;
import java.util.ArrayList;
import java.util.jar.JarFile;
import java.util.jar.JarEntry;
import java.net.URL;
import java.net.URI;

/**
 * Based on example at http://forums.sun.com/thread.jspa?threadID=360060&forumID=31.
 */
public class PartitionClassLoader extends ClassLoader {

    Logger log = LoggerFactory.getLogger(getClass());
    boolean debug = log.isDebugEnabled();

    Map<String,Class> classes = new LinkedHashMap<String,Class>();

    Collection<File> files = new ArrayList<File>();

    public PartitionClassLoader(Collection<File> files, ClassLoader parent) {
        super(parent);
        this.files.addAll(files);
    }

    public byte[] getClassData(File file, String classFileName) throws Exception {
        if (file.isDirectory()) {
            return getClassDataFromDirectory(file, classFileName);

        } else {
            return getClassDataFromJarFile(file, classFileName);
        }
    }

    public byte[] getClassDataFromDirectory(File dir, String classFileName) throws Exception {

        BufferedInputStream is = null;

        try {
            File file = new File(dir, classFileName);
            if (!file.exists()) return null;

            is = new BufferedInputStream(new FileInputStream(file));
            byte[] buffer = new byte[(int)file.length()];

            int c = is.read(buffer, 0, buffer.length);
            if (c < 0) throw new Exception("Error reading "+file.getName()+".");

            return buffer;

        } finally {
            if (is != null) try { is.close(); } catch (IOException e) { log.error(e.getMessage(), e); }
        }
    }

    public byte[] getClassDataFromJarFile(File file, String classFileName) throws Exception {

        JarFile jarFile = null;
        BufferedInputStream is = null;

        try {
            jarFile = new JarFile(file);

            JarEntry jarEntry = (JarEntry)jarFile.getEntry(classFileName);
            if (jarEntry == null) return null;

            is = new BufferedInputStream(jarFile.getInputStream(jarEntry));
            byte[] buffer = new byte[(int)jarEntry.getSize()];

            int c = is.read(buffer, 0, buffer.length);
            if (c < 0) throw new Exception("Error reading "+ file.getName()+".");

            return buffer;

        } finally {
            if (is != null) try { is.close(); } catch (IOException e) { log.error(e.getMessage(), e); }
            if (jarFile != null) try { jarFile.close(); } catch (IOException e) { log.error(e.getMessage(), e); }
        }
    }

    public byte[] getClassData(String className) {

        //if (debug) log.debug("Searching for "+className+":");

        String classFileName = className.replace('.', '/')+".class";

        for (File file : files) {
            try {
                byte[] buffer = getClassData(file, classFileName);
                if (buffer == null) continue;

                //if (debug) log.debug("Class "+className+" found in "+file+".");

                return buffer;

            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }

        //if (debug) log.debug("Class "+className+" not found.");

        return null;
    }

	public synchronized Class loadClass(String className, boolean resolve) throws ClassNotFoundException {

        if (classes.containsKey(className)) return classes.get(className);

        byte[] buffer = getClassData(className);
        if (buffer == null) return super.loadClass(className, resolve);

		Class clazz = defineClass(className, buffer, 0, buffer.length);
		if (clazz == null) throw new ClassFormatError();

		if (resolve) resolveClass(clazz);
		classes.put(className, clazz);

		return(clazz);
	}
}
