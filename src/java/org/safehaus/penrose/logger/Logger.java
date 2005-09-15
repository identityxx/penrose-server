/** * Copyright (c) 2000-2005, Identyx Corporation.
 * 
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA */package org.safehaus.penrose.logger;import java.lang.reflect.Method;/** * @author Endi S. Dewata */public class Logger {    Object logger;    public Logger(Object logger) {        this.logger = logger;    }    public void error(String message, Throwable t) {        Method method = getMethod("error", new Class[] { String.class, Throwable.class });        if (method == null) {            method = getMethod("error", new Class[] { Object.class, Throwable.class });        }        invoke(method, new Object[] { message, t });    }    public void warn(String message) {        Method method = getMethod("warn", new Class[] { String.class });        if (method == null) {            method = getMethod("warn", new Class[] { Object.class });        }        invoke(method, new Object[] { message });    }    public void info(String message) {        Method method = getMethod("info", new Class[] { String.class });        if (method == null) {            method = getMethod("info", new Class[] { Object.class });        }        invoke(method, new Object[] { message });    }    public void debug(String message) {        Method method = getMethod("debug", new Class[] { String.class });        if (method == null) {            method = getMethod("debug", new Class[] { Object.class });        }        invoke(method, new Object[] { message });    }    public Method getMethod(String name, Class paramTypes[]) {        Class clazz = logger.getClass();        try {            return clazz.getMethod(name, paramTypes);        } catch (Exception e) {            return null;        }    }    public void invoke(Method method, Object params[]) {        try {            method.invoke(logger, params);        } catch (Exception e) {            //ignore        }    }}