package org.safehaus.penrose.util;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * @author Endi Sukma Dewata
 */
public class ClassUtil {

    public static String getSignature(Method method) {
        Collection<String> parameterTypes = new ArrayList<String>();
        for (Class<?> parameterType : method.getParameterTypes()) {
            parameterTypes.add(parameterType.getName());
        }
        return getSignature(method.getName(), parameterTypes);
    }

    public static String getSignature(String methodName, String[] parameterTypes) {
        return getSignature(methodName, parameterTypes == null ? null : Arrays.asList(parameterTypes));
    }

    public static String getSignature(String methodName, Collection<String> parameterTypes) {
        StringBuilder sb = new StringBuilder();
        sb.append(methodName);
        
        sb.append("(");
        if (parameterTypes != null) {
            boolean first = true;
            for (String parameterType : parameterTypes) {
                if (first) {
                    first = false;
                } else {
                    sb.append(",");
                }
                sb.append(parameterType);
            }
        }
        sb.append(")");

        return sb.toString();
    }
}
