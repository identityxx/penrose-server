package org.safehaus.penrose.util;

/**
 * @author Endi Sukma Dewata
 */
public class Util {

    public static Object coalesce(Object arg1, Object arg2) {
        if (arg1 != null) return arg1;
        return arg2;
    }
}
