/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 * All rights reserved.
 */
package org.safehaus.penrose.logger;

import java.lang.reflect.Method;

/**
 * @author Endi S. Dewata
 */
public class Logger {

    Object logger;

    public Logger(Object logger) {
        this.logger = logger;
    }

    public void error(String message, Throwable t) {
        Method method = getMethod("error", new Class[] { String.class, Throwable.class });
        if (method == null) {
            method = getMethod("error", new Class[] { Object.class, Throwable.class });
        }
        invoke(method, new Object[] { message, t });
    }

    public void warn(String message) {
        Method method = getMethod("warn", new Class[] { String.class });
        if (method == null) {
            method = getMethod("warn", new Class[] { Object.class });
        }
        invoke(method, new Object[] { message });
    }

    public void info(String message) {
        Method method = getMethod("info", new Class[] { String.class });
        if (method == null) {
            method = getMethod("info", new Class[] { Object.class });
        }
        invoke(method, new Object[] { message });
    }

    public void debug(String message) {
        Method method = getMethod("debug", new Class[] { String.class });
        if (method == null) {
            method = getMethod("debug", new Class[] { Object.class });
        }
        invoke(method, new Object[] { message });
    }

    public Method getMethod(String name, Class paramTypes[]) {
        Class clazz = logger.getClass();
        try {
            return clazz.getMethod(name, paramTypes);
        } catch (Exception e) {
            return null;
        }
    }

    public void invoke(Method method, Object params[]) {
        try {
            method.invoke(logger, params);
        } catch (Exception e) {
            //ignore
        }
    }
}
