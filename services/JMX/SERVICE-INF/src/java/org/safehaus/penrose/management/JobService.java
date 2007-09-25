package org.safehaus.penrose.management;

import org.apache.log4j.Logger;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.util.ClassUtil;
import org.safehaus.penrose.scheduler.Job;
import org.safehaus.penrose.scheduler.JobConfig;

import javax.management.*;
import java.util.*;
import java.lang.reflect.Method;

/**
 * @author Endi Sukma Dewata
 */
public class JobService implements DynamicMBean, JobServiceMBean {

    public Logger log = Logger.getLogger(getClass());
    public boolean debug = log.isDebugEnabled();

    private PenroseJMXService jmxService;
    private Partition partition;
    private Job job;

    MBeanInfo mbeanInfo;

    public JobService(Partition partition, Job job) throws Exception {
        this.partition = partition;
        this.job = job;

        JobConfig jobConfig = job.getJobConfig();
        String jobClass = jobConfig.getJobClass();
        String description = jobConfig.getDescription();

        Class clazz = job.getClass();

        Collection<String> attributes = new TreeSet<String>();
        Map<String,Method> getters    = new TreeMap<String,Method>();
        Map<String,Method> setters    = new TreeMap<String,Method>();
        Map<String,Method> operations = new TreeMap<String,Method>();

        for (Method method : clazz.getMethods()) {
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

        Collection<MBeanAttributeInfo> attributeInfos = new LinkedHashSet<MBeanAttributeInfo>();
        for (String attributeName : attributes) {
            Method getter = getters.get(attributeName);
            Method setter = setters.get(attributeName);
            attributeInfos.add(new MBeanAttributeInfo(attributeName, "", getter, setter));
        }

        Method getter = clazz.getMethod("getJobConfig");
        Method setter = clazz.getMethod("setJobConfig", JobConfig.class);
        attributeInfos.add(new MBeanAttributeInfo("JobConfig", "", getter, setter));

        Collection<MBeanOperationInfo> operationInfos = new ArrayList<MBeanOperationInfo>();
        for (Method method : operations.values()) {
            operationInfos.add(new MBeanOperationInfo("", method));
        }

        mbeanInfo = new MBeanInfo(
                jobClass,
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

    public Job getJob() {
        return job;
    }

    public void setJob(Job job) {
        this.job = job;
    }

    public void register() throws Exception {
        jmxService.register(getObjectName(), this);
    }

    public void unregister() throws Exception {
        jmxService.unregister(getObjectName());
    }

    public JobConfig getJobConfig() throws Exception {
        return job.getJobConfig();
    }

    public String getObjectName() {
        return JobClient.getObjectName(partition.getName(), job.getName());
    }

    public Object getAttribute(String attribute) throws AttributeNotFoundException, MBeanException, ReflectionException {

        Class clazz = job.getClass();

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
            return getter.invoke(job);

        } catch (Exception e) {
            throw new MBeanException(e);
        }
    }

    public void setAttribute(Attribute attribute) throws AttributeNotFoundException, InvalidAttributeValueException, MBeanException, ReflectionException {
        String name = attribute.getName();
        Object value = attribute.getValue();

        Class clazz = job.getClass();

        Method setter;
        try {
            setter = clazz.getMethod("set"+name, value.getClass());
        } catch (Exception e) {
            throw new AttributeNotFoundException();
        }

        try {
            setter.invoke(job, value);

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

            Class clazz = job.getClass();
            ClassLoader classLoader = clazz.getClassLoader();

            Class[] paramClass = new Class[paramTypes.length];
            for (int i = 0; i<paramTypes.length; i++) {
                String paramType = paramTypes[i];
                paramClass[i] = classLoader.loadClass(paramType);
            }

            Method method = clazz.getMethod(operation, paramClass);
            return method.invoke(job, paramValues);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new MBeanException(e);
        }
    }

    public MBeanInfo getMBeanInfo() {
        return mbeanInfo;
    }
}
