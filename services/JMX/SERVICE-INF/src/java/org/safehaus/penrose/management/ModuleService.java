package org.safehaus.penrose.management;

import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.module.Module;
import org.safehaus.penrose.module.ModuleConfig;
import org.safehaus.penrose.util.ClassUtil;
import org.apache.log4j.Logger;

import javax.management.*;
import java.util.*;
import java.lang.reflect.Method;

/**
 * @author Endi Sukma Dewata
 */
public class ModuleService implements DynamicMBean, ModuleServiceMBean {

    public Logger log = Logger.getLogger(getClass());
    public boolean debug = log.isDebugEnabled();

    private PenroseJMXService jmxService;
    private Partition partition;
    private Module module;

    MBeanInfo mbeanInfo;

    public ModuleService(Partition partition, Module module) throws Exception {
        this.partition = partition;
        this.module = module;

        ModuleConfig moduleConfig = module.getModuleConfig();
        String moduleClass = moduleConfig.getModuleClass();
        String description = moduleConfig.getDescription();

        Class clazz = module.getClass();

        String mbeanClassName = moduleClass+"MBean";
        Class mbeanClass = null;

        //log.debug("Checking interfaces of "+clazz.getName()+":");
        for (Class interfaceClass : clazz.getInterfaces()) {
            String interfaceName = interfaceClass.getName();
            //log.debug(" - "+interfaceName);
            
            if (interfaceName.equals(mbeanClassName)) {
                mbeanClass = interfaceClass;
                break;
            }
        }

        Collection<String> attributes = new TreeSet<String>();
        Map<String,Method> getters    = new TreeMap<String,Method>();
        Map<String,Method> setters    = new TreeMap<String,Method>();
        Map<String,Method> operations = new TreeMap<String,Method>();

        if (mbeanClass == null) {
            //log.debug("MBean class "+mbeanClassName+" not found.");

        } else {
            for (Method method : mbeanClass.getMethods()) {
                String methodName = method.getName();

                if (methodName.length() > 3 && methodName.startsWith("get") && Character.isUpperCase(methodName.charAt(3))) {
                    String attributeName = methodName.substring(3);
                    attributes.add(attributeName);
                    getters.put(attributeName, method);

                } else if (methodName.length() > 2 && methodName.startsWith("is") && Character.isUpperCase(methodName.charAt(2))) {
                    String attributeName = methodName.substring(2);
                    attributes.add(attributeName);
                    getters.put(attributeName, method);

                } else if (methodName.length() > 3 && methodName.startsWith("set") && Character.isUpperCase(methodName.charAt(3))) {
                    String attributeName = methodName.substring(3);
                    attributes.add(attributeName);
                    setters.put(attributeName, method);

                } else {
                    String signature = ClassUtil.getSignature(method);
                    operations.put(signature, method);
                }
            }
        }

        Collection<MBeanAttributeInfo> attributeInfos = new LinkedHashSet<MBeanAttributeInfo>();
        for (String attributeName : attributes) {
            Method getter = getters.get(attributeName);
            Method setter = setters.get(attributeName);
            attributeInfos.add(new MBeanAttributeInfo(attributeName, "", getter, setter));
        }

        Method getter = clazz.getMethod("getModuleConfig");
        Method setter = clazz.getMethod("setModuleConfig", ModuleConfig.class);
        attributeInfos.add(new MBeanAttributeInfo("ModuleConfig", "", getter, setter));

        Collection<MBeanOperationInfo> operationInfos = new ArrayList<MBeanOperationInfo>();
        for (Method method : operations.values()) {
            operationInfos.add(new MBeanOperationInfo("", method));
        }

        mbeanInfo = new MBeanInfo(
                moduleClass,
                description,
                attributeInfos.toArray(new MBeanAttributeInfo[attributeInfos.size()]),
                null,
                operationInfos.toArray(new MBeanOperationInfo[operationInfos.size()]),
                null
        );
    }

    public PenroseJMXService getJmxService() {
        return jmxService;
    }

    public void setJmxService(PenroseJMXService jmxService) {
        this.jmxService = jmxService;
    }

    public Partition getPartition() {
        return partition;
    }

    public void setPartition(Partition partition) {
        this.partition = partition;
    }

    public Module getModule() {
        return module;
    }

    public void setModule(Module module) {
        this.module = module;
    }

    public void register() throws Exception {
        jmxService.register(getObjectName(), this);
    }

    public void unregister() throws Exception {
        jmxService.unregister(getObjectName());
    }

    public ModuleConfig getModuleConfig() throws Exception {
        return module.getModuleConfig();
    }

    public String getObjectName() {
        return ModuleClient.getObjectName(partition.getName(), module.getName());
    }

    public Object getAttribute(String attribute) throws AttributeNotFoundException, MBeanException, ReflectionException {

        Class clazz = module.getClass();

        Method getter = null;
        try {
            getter = clazz.getMethod("get"+attribute);
        } catch (Exception e) {
            //ignore
        }

        if (getter == null) {
            try {
                getter = clazz.getMethod("is"+attribute);
            } catch (Exception e) {
                //ignore
            }
        }

        if (getter == null) {
            throw new AttributeNotFoundException();
        }

        try {
            return getter.invoke(module);

        } catch (Exception e) {
            throw new MBeanException(e);
        }
    }

    public void setAttribute(Attribute attribute) throws AttributeNotFoundException, InvalidAttributeValueException, MBeanException, ReflectionException {
        String name = attribute.getName();
        Object value = attribute.getValue();

        Class clazz = module.getClass();

        Method setter;
        try {
            setter = clazz.getMethod("set"+name, value.getClass());
        } catch (Exception e) {
            throw new AttributeNotFoundException();
        }

        try {
            setter.invoke(module, value);

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
            if (debug) log.debug("Invoking "+ClassUtil.getSignature(operation, paramTypes));

            Class clazz = module.getClass();
            ClassLoader classLoader = clazz.getClassLoader();

            Class[] paramClass = new Class[paramTypes.length];
            for (int i = 0; i<paramTypes.length; i++) {
                String paramType = paramTypes[i];
                paramClass[i] = classLoader.loadClass(paramType);
            }

            Method method = clazz.getMethod(operation, paramClass);
            return method.invoke(module, paramValues);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new MBeanException(e);
        }
    }

    public MBeanInfo getMBeanInfo() {
        return mbeanInfo;
    }
}
