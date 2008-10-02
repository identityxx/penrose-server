package org.safehaus.penrose.management;

import org.apache.log4j.Logger;
import org.safehaus.penrose.util.ClassUtil;

import javax.management.*;
import javax.security.auth.Subject;
import java.lang.reflect.InvocationTargetException;
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
    public boolean info = log.isInfoEnabled();

    protected PenroseJMXService jmxService;

    protected String description;

    public BaseService() {
    }

    public void init() throws Exception {
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
        if (info) log.info("Getting attribute "+name+" from "+getObjectName()+".");

        try {
            Class[] paramClass = new Class[0];

            Class clazz = getClass();

            try {
                Method method = clazz.getMethod("get"+name, paramClass);
                return method.invoke(this);

            } catch (NoSuchMethodException e) {
                // ignore
            }

            try {
                Method method = clazz.getMethod("is"+name, paramClass);
                return method.invoke(this);

            } catch (NoSuchMethodException e) {
                // ignore
            }

            Object object = getObject();
            Class objectClass = object.getClass();

            try {
                Method method = objectClass.getMethod("get"+name, paramClass);
                return method.invoke(object);

            } catch (NoSuchMethodException e) {
                // ignore
            }

            try {
                Method method = objectClass.getMethod("is"+name, paramClass);
                return method.invoke(object);

            } catch (NoSuchMethodException e) {
                // ignore
            }

            throw new AttributeNotFoundException();

        } catch (IllegalAccessException e) {
            throw new ReflectionException(e);

        } catch (InvocationTargetException e) {
            Exception cause = (Exception)e.getCause();
            log.error(cause.getMessage(), cause);
            throw new MBeanException(cause);

        } finally {
            if (info) log.info("Get attribute completed.");
        }
    }

    public void setAttribute(Attribute attribute) throws AttributeNotFoundException, InvalidAttributeValueException, MBeanException, ReflectionException {

        String name = attribute.getName();
        Object value = attribute.getValue();

        if (info) log.info("Setting attribute "+name+" in "+getObjectName()+".");

        try {
            Class[] paramClass = new Class[] { value.getClass() };

            Class clazz = getClass();

            try {
                Method method = clazz.getMethod("set"+name, paramClass);
                method.invoke(this, value);
                return;

            } catch (NoSuchMethodException e) {
                // ignore
            }

            Object object = getObject();
            Class objectClass = object.getClass();

            try {
                Method method = objectClass.getMethod("set"+name, paramClass);
                method.invoke(object, value);
                return;

            } catch (NoSuchMethodException e) {
                // ignore
            }

            throw new AttributeNotFoundException();


        } catch (IllegalAccessException e) {
            throw new ReflectionException(e);

        } catch (InvocationTargetException e) {
            Exception cause = (Exception)e.getCause();
            log.error(cause.getMessage(), cause);
            throw new MBeanException(cause);

        } finally {
            if (info) log.info("Set attribute "+name+" in "+getObjectName()+" completed.");
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

        String signature = ClassUtil.getSignature(operation, paramTypes);
        if (info) log.info("Invoking method "+signature+" in "+getObjectName()+".");

        try {
            Class[] paramClass = new Class[paramTypes.length];
            for (int i = 0; i<paramTypes.length; i++) {
                String paramType = paramTypes[i];
                paramClass[i] = Class.forName(paramType);
            }

            Class clazz = getClass();

            try {
                Method method = clazz.getMethod(operation, paramClass);
                return method.invoke(this, paramValues);

            } catch (NoSuchMethodException e) {
                if (debug) log.debug("No such method in "+clazz.getName()+".");
                // ignore
            }

            Object object = getObject();
            Class objectClass = object.getClass();

            try {
                Method method = objectClass.getMethod(operation, paramClass);
                return method.invoke(object, paramValues);

            } catch (NoSuchMethodException e) {
                if (debug) log.debug("No such method in "+objectClass.getName()+".");
                // ignore
            }

            throw new NoSuchMethodException("No such method: "+signature);

        } catch (ClassNotFoundException e) {
            throw new ReflectionException(e);

        } catch (NoSuchMethodException e) {
            throw new ReflectionException(e);

        } catch (IllegalAccessException e) {
            throw new ReflectionException(e);

        } catch (InvocationTargetException e) {
            Exception cause = (Exception)e.getCause();
            log.error(cause.getMessage(), cause);
            throw new MBeanException(cause);

        } finally {
            if (info) log.info("Invoke method completed.");
        }
    }

    public MBeanInfo getMBeanInfo() {

        Collection<String> attributes = new TreeSet<String>();
        Map<String,Method> getters    = new TreeMap<String,Method>();
        Map<String,Method> setters    = new TreeMap<String,Method>();
        Map<String,Method> operations = new TreeMap<String,Method>();

        Collection<MBeanAttributeInfo> attributeInfos = new LinkedHashSet<MBeanAttributeInfo>();
        Collection<MBeanOperationInfo> operationInfos = new ArrayList<MBeanOperationInfo>();

        Object object = getObject();
        Class objectClass = object == null ? getClass() : object.getClass();

        try {
            if (object != null) register(
                    object.getClass(),
                    attributes,
                    getters,
                    setters,
                    operations
            );

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

        try {
            register(
                    getClass(),
                    attributes,
                    getters,
                    setters,
                    operations
            );

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

        try {
            for (String attributeName : attributes) {
                Method getter = getters.get(attributeName);
                Method setter = setters.get(attributeName);
                attributeInfos.add(new MBeanAttributeInfo(attributeName, attributeName, getter, setter));
            }

            for (Method method : operations.values()) {
                operationInfos.add(new MBeanOperationInfo(method.getName(), method));
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

        return new MBeanInfo(
                objectClass.getName(),
                description,
                attributeInfos.toArray(new MBeanAttributeInfo[attributeInfos.size()]),
                null,
                operationInfos.toArray(new MBeanOperationInfo[operationInfos.size()]),
                null
        );
    }

    public void register(
            Class clazz,
            Collection<String> attributes,
            Map<String,Method> getters,
            Map<String,Method> setters,
            Map<String,Method> operations
    ) throws Exception {

        Class superClass = clazz.getSuperclass();
        if (superClass != null) register(
                superClass,
                attributes,
                getters,
                setters,
                operations
        );

        String className = clazz.getName();
        for (Class interfaceClass : clazz.getInterfaces()) {
            String interfaceName = interfaceClass.getName();
            if (!interfaceName.equals(className+"MBean")) continue;

            for (Method method : interfaceClass.getMethods()) {
                register(
                        method,
                        attributes,
                        getters,
                        setters,
                        operations
                );
            }
        }
    }

    public void register(
            Method method,
            Collection<String> attributes,
            Map<String,Method> getters,
            Map<String,Method> setters,
            Map<String,Method> operations
    ) throws Exception {

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
}
