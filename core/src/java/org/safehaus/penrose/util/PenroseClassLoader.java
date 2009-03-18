package org.safehaus.penrose.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.Penrose;

import java.io.File;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.FileInputStream;
import java.util.*;
import java.util.jar.JarFile;
import java.util.jar.JarEntry;
import java.net.URL;
import java.net.MalformedURLException;

/**
 * Based on example at http://forums.sun.com/thread.jspa?threadID=360060&forumID=31.
 */
public class PenroseClassLoader extends ClassLoader {

    protected Logger log = LoggerFactory.getLogger(getClass());

    protected Collection<File> classPaths = new ArrayList<File>();
    protected Map<String,Class> classes = new LinkedHashMap<String,Class>();

    public PenroseClassLoader(ClassLoader parent) {
        super(parent);
    }

    public void setClassPaths(Collection<File> classPaths) {
        if (this.classPaths == classPaths) return;
        this.classPaths.clear();
        if (classPaths == null) return;
        this.classPaths.addAll(classPaths);
    }

    public Collection<File> getClassPaths() {
        return classPaths;
    }

    public byte[] getClassData(File classPath, String classFileName) throws Exception {
        if (classPath.isDirectory()) {
            return getClassDataFromDirectory(classPath, classFileName);

        } else {
            return getClassDataFromFile(classPath, classFileName);
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
            if (is != null) try { is.close(); } catch (IOException e) { Penrose.errorLog.error(e.getMessage(), e); }
        }
    }

    public byte[] getClassDataFromFile(File file, String classFileName) throws Exception {

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
            if (is != null) try { is.close(); } catch (IOException e) { Penrose.errorLog.error(e.getMessage(), e); }
            if (jarFile != null) try { jarFile.close(); } catch (IOException e) { Penrose.errorLog.error(e.getMessage(), e); }
        }
    }

    public byte[] getClassData(String className) {

        //if (debug) log.debug("Searching for "+className+":");

        String classFileName = className.replace('.', '/')+".class";

        for (File classPath : classPaths) {
            try {
                byte[] buffer = getClassData(classPath, classFileName);
                if (buffer == null) continue;

                //if (debug) log.debug("Class "+className+" found in "+classPath+".");

                return buffer;

            } catch (Exception e) {
                Penrose.errorLog.error(e.getMessage(), e);
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

    public URL getResourceURL(File classPath, String name) throws Exception {
        if (classPath.isDirectory()) {
            return getResourceURLFromDirectory(classPath, name);

        } else {
            return getResourceURLFromFile(classPath, name);
        }
    }

    public URL getResourceURLFromDirectory(File dir, String name) throws MalformedURLException {
        File file = new File(dir, name);
        if (!file.exists()) return null;

        return file.toURL();
    }

    public URL getResourceURLFromFile(File file, String name) throws IOException {

        JarFile jarFile = null;

        try {
            jarFile = new JarFile(file);

            JarEntry jarEntry = (JarEntry)jarFile.getEntry(name);
            if (jarEntry == null) return null;

            return new URL("jar:file:"+file+"!/"+name);

        } finally {
            if (jarFile != null) try { jarFile.close(); } catch (IOException e) { Penrose.errorLog.error(e.getMessage(), e); }
        }
    }

    public URL findResource(String name) {

        //boolean debug = log.isDebugEnabled();
        //if (debug) log.debug("Searching resource "+name+":");

        for (File classPath : classPaths) {
            try {
                URL url = getResourceURL(classPath, name);
                if (url == null) continue;

                //if (debug) log.debug("URL: "+url);
                return url;

            } catch (Exception e) {
                Penrose.errorLog.error(e.getMessage(), e);
            }
        }

        //if (debug) log.debug("Resource "+name+" not found.");

        return null;
    }

    public Enumeration<URL> findResources(String name) throws IOException {

        //boolean debug = log.isDebugEnabled();
        //if (debug) log.debug("Searching resources "+name+":");

        final Collection<URL> urls = new ArrayList<URL>();

        for (File classPath : classPaths) {
            try {
                URL url = getResourceURL(classPath, name);
                if (url == null) continue;

                //if (debug) log.debug("URL: "+url);
                urls.add(url);

            } catch (Exception e) {
                Penrose.errorLog.error(e.getMessage(), e);
            }
        }

        return new Enumeration<URL>() {
            Iterator<URL> iterator = urls.iterator();
            public boolean hasMoreElements() {
                return iterator.hasNext();
            }
            public URL nextElement() {
                return iterator.next();
            }
        };
    }
}
