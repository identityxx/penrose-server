/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 * All rights reserved.
 */
package org.safehaus.penrose.logger;

/**
 * @author Endi S. Dewata
 */
public class LoggerFactory {

    public static Logger getLogger(Class clazz) {
        return new Logger(org.slf4j.LoggerFactory.getLogger(clazz));
    }
}
