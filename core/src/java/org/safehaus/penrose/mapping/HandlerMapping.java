package org.safehaus.penrose.mapping;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class HandlerMapping implements Cloneable {

    Logger log = LoggerFactory.getLogger(getClass());

	private String name = "DEFAULT";

    private String handlerName;

    private Properties parameters = new Properties();

	public HandlerMapping() {
	}

    public HandlerMapping(String name, String handlerName) {
        this.name = name;
        this.handlerName = handlerName;
    }

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

    public String getHandlerName() {
        return handlerName;
    }

    public void setHandlerName(String handlerName) {
        this.handlerName = handlerName;
    }

    public String getParameter(String name) {
        return parameters.getProperty(name);
    }

    public void removeParameters() {
        parameters.clear();
    }

    public void setParameter(String name, String value) {
        parameters.put(name, value);
    }

    public void removeParameter(String name) {
        parameters.remove(name);
    }

    public Collection getParameterNames() {
        return parameters.keySet();
    }

    public int hashCode() {
        return (name == null ? 0 : name.hashCode()) +
                (handlerName == null ? 0 : handlerName.hashCode()) +
                (parameters == null ? 0 : parameters.hashCode());
    }

    boolean equals(Object o1, Object o2) {
        if (o1 == null && o2 == null) return true;
        if (o1 != null) return o1.equals(o2);
        return o2.equals(o1);
    }

    public boolean equals(Object object) {
        if (this == object) return true;
        if((object == null) || (object.getClass() != this.getClass())) return false;

        HandlerMapping handlerMapping = (HandlerMapping)object;
        if (!equals(name, handlerMapping.name)) return false;
        if (!equals(handlerName, handlerMapping.handlerName)) return false;
        if (!equals(parameters, handlerMapping.parameters)) return false;

        return true;
    }

    public void copy(HandlerMapping handlerMapping) {
        name = handlerMapping.name;
        handlerName = handlerMapping.handlerName;

        removeParameters();
        parameters.putAll(handlerMapping.parameters);
    }

    public Object clone() {
        HandlerMapping handlerMapping = new HandlerMapping();
        handlerMapping.copy(this);
        return handlerMapping;
    }
}
