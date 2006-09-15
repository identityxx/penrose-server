package org.safehaus.penrose.log4j;

import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class AppenderConfig {

    private String name;
    private String appenderClass;

    LayoutConfig layoutConfig;

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

    public LayoutConfig getLayoutConfig() {
        return layoutConfig;
    }

    public void setLayoutConfig(LayoutConfig layoutConfig) {
        this.layoutConfig = layoutConfig;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAppenderClass() {
        return appenderClass;
    }

    public void setAppenderClass(String appenderClass) {
        this.appenderClass = appenderClass;
    }
}
