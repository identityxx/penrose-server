package org.safehaus.penrose.log4j;

import java.util.Collection;
import java.util.Map;
import java.util.LinkedHashMap;

/**
 * @author Endi S. Dewata
 */
public class LayoutConfig {

    String layoutClass;
    Map parameters = new LinkedHashMap();

    public void setParameter(String name, String value) {
        parameters.put(name, value);
    }

    public String getParameter(String name) {
        return (String)parameters.get(name);
    }

    public Collection getParameterNames() {
        return parameters.keySet();
    }

    public String removeParameter(String name) {
        return (String)parameters.remove(name);
    }

    public void clearParameters() {
        parameters.clear();
    }

    public String getLayoutClass() {
        return layoutClass;
    }

    public void setLayoutClass(String layoutClass) {
        this.layoutClass = layoutClass;
    }
}
