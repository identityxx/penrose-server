package org.safehaus.penrose.management;

import org.apache.log4j.Logger;
import org.safehaus.penrose.util.ClassUtil;

import javax.management.*;
import javax.security.auth.Subject;
import java.lang.reflect.Method;
import java.security.AccessController;
import java.security.Principal;
import java.util.*;

/**
 * @author Endi Sukma Dewata
 */
public abstract class BaseService implements DynamicMBean {

    public Logger log = Logger.getLogger(getClass());
    public boolean debug = log.isDebugEnabled();

    protected PenroseJMXService jmxService;

    protected Class objectClass;

    protected String description;

    protected Collection<String> attributes = new TreeSet<String>();
    protected Map<String,Method> getters    = new TreeMap<String,Method>();
    protected Map<String,Method> setters    = new TreeMap<String,Method>();
    protected Map<String,Method> operations = new TreeMap<String,Method>();

    protected Collection<MBeanAttributeInfo> attributeInfos = new LinkedHashSet<MBeanAttributeInfo>();
    protected Collection<MBeanOperationInfo> operationInfos = new ArrayList<MBeanOperationInfo>();

    public BaseService(Class objectClass) {
        this.objectClass = objectClass;
    }

    public BaseService(Class objectClass, String description) throws Exception {
        this.objectClass = objectClass;
        this.description = description;
    }

    public void init() throws Exception {

        register(objectClass);
        register(getClass());

        for (String attributeName : attributes) {
            Method getter = getters.get(attributeName);
            Method setter = setters.get(attributeName);
            attributeInfos.add(new MBeanAttributeInfo(attributeName, attributeName, getter, setter));
        }

        for (Method method : operations.values()) {
            operationInfos.add(new MBeanOperationInfo(method.getName(), method));
        }
    }

    public void register(Class clazz) throws Exception {

        Class superClass = clazz.getSuperclass();
        if (superClass != null) register(superClass);

        String className = clazz.getName();
        for (Class interfaceClass : clazz.getInterfaces()) {
            String interfaceName = interfaceClass.getName();
            if (!interfaceName.equals(className+"MBean")) continue;

            for (Method method : interfaceClass.getMethods()) {
                register(method);
            }
        }
    }

    public void register(Method method) throws Exception {

        String methodName = method.getName();

        if (methodName.length() > 3 &&
                methodName.startsWith("get") &&
                Character.isUpperCase(methodName.charAt(3)) &&
                method.getParameterTypes().length == 0) {
            String attributeName = methodName.substring(3);
            attributes.add(attributeName);
            getters.put(attributeName, method);

        } else if (methodName.length() > 2 &&
                methodName.startsWith("is") &&
                Character.isUpperCase(methodName.charAt(2)) &&
                method.getParameterTypes().length == 0) {
            String attributeName = methodName.substring(2);
            attributes.add(attributeName);
            getters.put(attributeName, method);

        } else if (methodName.length() > 3 &&
                methodName.startsWith("set") &&
                Character.isUpperCase(methodName.charAt(3)) &&
                method.getParameterTypes().length == 1) {
            String attributeName = methodName.substring(3);
            attributes.add(attributeName);
            setters.put(attributeName, method);

        } else {
            String signature = ClassUtil.getSignature(method);
            operations.put(signature, method);
        }
    }

    public PenroseJMXService getJmxService() {
        return jmxService;
    }

    public void setJmxService(PenroseJMXService jmxService) {
        this.jmxService = jmxService;
    }

    public void register() throws Exception {
        jmxService.register(getObjectName(), this);
    }

    public void unregister() throws Exception {
        jmxService.unregister(getObjectName());
    }

    public abstract String getObjectName();
    public abstract Object getObject();

    public Object getAttribute(String name) throws AttributeNotFoundException, MBeanException, ReflectionException {
        try {
            Class[] paramClass = new Class[0];

            Method method = getMethod(this, "get"+name, paramClass);
            if (method == null) method = getMethod(this, "is"+name, paramClass);

            if (method != null) {
                return method.invoke(this);
            }

            Object object = getObject();
            method = getMethod(object, "get"+name, paramClass);
            if (method == null) method = getMethod(object, "is"+name, paramClass);

            if (method != null) {
                return method.invoke(object);
            }

            throw new AttributeNotFoundException();

        } catch (Exception e) {
            throw new MBeanException(e);
        }
    }

    public void setAttribute(Attribute attribute) throws AttributeNotFoundException, InvalidAttributeValueException, MBeanException, ReflectionException {
        try {
            String name = attribute.getName();
            Object value = attribute.getValue();

            Class[] paramClass = new Class[] { value.getClass() };

            Method method = getMethod(this, "set"+name, paramClass);

            if (method != null) {
                method.invoke(this, value);
                return;
            }

            Object object = getObject();
            method = getMethod(object, "set"+name, paramClass);

            if (method != null) {
                method.invoke(object, value);
                return;
            }

            throw new AttributeNotFoundException();

        } catch (Exception e) {
            throw new MBeanException(e);
        }
    }

    public AttributeList getAttributes(String[] attributes) {
        AttributeList list = new AttributeList();
        for (String attributeName : attributes) {
            try {
                Object value = getAttribute(attributeName);
                list.add(new Attribute(attributeName, value));

            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
        return list;
    }

    public AttributeList setAttributes(AttributeList attributes) {
        AttributeList list = new AttributeList();
        for (Object object : attributes) {
            try {
                Attribute attribute = (Attribute) object;
                setAttribute(attribute);
                list.add(attribute);

            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
        return list;
    }

    public Object invoke(String operation, Object[] paramValues, String[] paramTypes) throws MBeanException, ReflectionException {
        try {
            String signature = ClassUtil.getSignature(operation, paramTypes);
            if (debug) log.debug("Invoking method "+signature);

            Class[] paramClass = new Class[paramTypes.length];
            for (int i = 0; i<paramTypes.length; i++) {
                String paramType = paramTypes[i];
                paramClass[i] = Class.forName(paramType);
            }

            Method method = getMethod(this, operation, paramClass);

            if (method != null) {
                return method.invoke(this, paramValues);
            }

            Object object = getObject();
            method = getMethod(object, operation, paramClass);

            if (method != null) {
                return method.invoke(object, paramValues);
            }

            throw new ReflectionException(new NullPointerException(), "Method "+signature+" not found.");

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new MBeanException(e);
        }
    }

    public Method getMethod(Object object, String operation, Class[] paramClass) throws Exception {
        try {
            Class clazz = object.getClass();
            return clazz.getMethod(operation, paramClass);
            
        } catch (NoSuchMethodException e) {
            log.error(e.getMessage());
            return null;
        }
    }

    public MBeanInfo getMBeanInfo() {
        return new MBeanInfo(
                objectClass.getName(),
                description,
                attributeInfos.toArray(new MBeanAttributeInfo[attributeInfos.size()]),
                null,
                operationInfos.toArray(new MBeanOperationInfo[operationInfos.size()]),
                null
        );
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getBindDn() {
        Subject subject = Subject.getSubject(AccessController.getContext());
        if (subject == null) return null;

        Collection<Principal> principals = subject.getPrincipals();
        if (principals.isEmpty()) return null;

        Principal principal = principals.iterator().next();
        return principal.getName();
    }
}
