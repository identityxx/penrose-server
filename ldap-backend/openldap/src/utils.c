/**
 * @author Endi S. Dewata
 */

#include "java-backend.h"

jstring newStringUTF(
    JNIEnv *env,
    char *str
)
{
    jstring res;

    DEBUG1("==> newStringUTF(%s)\n", str);
    res = (*env)->NewStringUTF(env, str);

    if (res == 0) {
        DEBUG("<== newStringUTF(): String not created.\n");
    } else {
        DEBUG("<== newStringUTF(): String created.\n");
    }

    return res;
}

jclass findClass(
    JNIEnv *env,
    char *className
)
{
    //DEBUG1("==> findClass(\"%s\")\n", className);

    jclass clazz = (*env)->FindClass(env, className);

    if (clazz == 0) {
        DEBUG1("Class \"%s\" not found.\n", className);
    }

    //DEBUG("<== findClass()\n");

    return clazz;
}

jmethodID getMethodID(
    JNIEnv *env,
    jclass clazz,
    char *methodName,
    char *methodSignature
)
{
    //DEBUG2("==> getMethodID(\"%s\", \"%s\")\n", methodName, methodSignature);

    jmethodID mid = (*env)->GetMethodID(env, clazz, methodName, methodSignature);

    if (mid == 0) {
        DEBUG2("Method \"%s%s\" not found.\n", methodName, methodSignature);
    }

    //DEBUG("<== getMethodID()\n");
    return mid;
}

/*
jobject newObject(
    JNIEnv *env,
    jclass clazz,
    jmethodID method,
    ...
)
{
    jobject object;
    va_list args;

    DEBUG("==> newObject()\n");
    va_start(args, method);
    object = (*env)->NewObject(env, clazz, method, args);
    va_end(args);

    if (object == 0) {
        DEBUG("<== newObject(): Object not created\n");
    } else {
        DEBUG("<== newObject(): Object created\n");
    }

    return object;
}

jboolean callBooleanMethod(
    JNIEnv *env,
    jobject object,
    jmethodID method,
    ...
)
{
    jboolean res;
    va_list args;

    va_start(args, method);
    res = (*env)->CallBooleanMethod(env, object, method, args);
    va_end(args);

    return res;
}

jint callIntMethod(
    JNIEnv *env,
    jobject object,
    jmethodID method,
    ...
)
{
    jint res;
    va_list args;

    va_start(args, method);
    res = (*env)->CallIntMethod(env, object, method, args);
    va_end(args);

    return res;
}

jobject callObjectMethod(
    JNIEnv *env,
    jobject object,
    jmethodID method,
    ...
)
{
    jobject res;
    va_list args;

    va_start(args, method);
    res = (*env)->CallObjectMethod(env, object, method, args);
    va_end(args);

    return res;
}

void callVoidMethod(
    JNIEnv *env,
    jobject object,
    jmethodID method,
    ...
)
{
    va_list args;

    va_start(args, method);
    (*env)->CallVoidMethod(env, object, method, args);
    va_end(args);
}

jthrowable exceptionOccurred(
    JNIEnv *env
)
{
    jthrowable exc = (*env)->ExceptionOccurred(env);

    if (exc) {
        (*env)->ExceptionDescribe(env);
        (*env)->ExceptionClear(env);
    }       

    return exc;
}
*/

int initClasses(JNIEnv* env) {

    //------------------------------------------------------------------------------------------
    // java.lang.String
    //------------------------------------------------------------------------------------------

    String.class = findClass(env, "java/lang/String");
    if (String.class == 0) return -1;

    //------------------------------------------------------------------------------------------
    // java.util.Iterator
    //------------------------------------------------------------------------------------------

    Iterator.class = findClass(env, "java/util/Iterator");
    if (Iterator.class == 0) return -1;

    Iterator.hasNext = getMethodID(env, Iterator.class, "hasNext", "()Z");
    if (Iterator.hasNext == 0) return -1;

    Iterator.next = getMethodID(env, Iterator.class, "next", "()Ljava/lang/Object;");
    if (Iterator.next == 0) return -1;

    //------------------------------------------------------------------------------------------
    // java.util.ArrayList
    //------------------------------------------------------------------------------------------

    ArrayList.class = findClass(env, "java/util/ArrayList");
    if (ArrayList.class == 0) return -1;

    ArrayList.constructor = getMethodID(env, ArrayList.class, "<init>", "()V");
    if (ArrayList.constructor == 0) return -1;

    ArrayList.add = getMethodID(env, ArrayList.class, "add", "(Ljava/lang/Object;)Z");
    if (ArrayList.add == 0) return -1;

    ArrayList.get = getMethodID(env, ArrayList.class, "get", "(I)Ljava/lang/Object;");
    if (ArrayList.get == 0) return -1;

    ArrayList.size = getMethodID(env, ArrayList.class, "size", "()I");
    if (ArrayList.size == 0) return -1;

    ArrayList.toArray = getMethodID(env, ArrayList.class, "toArray", "()[Ljava/lang/Object;");
    if (ArrayList.toArray == 0) return -1;

    //------------------------------------------------------------------------------------------
    // javax.naming.directory.SearchControls
    //------------------------------------------------------------------------------------------

    SearchControls.class = findClass(env,
        "javax/naming/directory/SearchControls");
    if (SearchControls.class == 0) return -1;

    SearchControls.constructor = getMethodID(env,
        SearchControls.class, "<init>", "()V");
    if (SearchControls.constructor == 0) return -1;

    SearchControls.setSearchScope = getMethodID(env,
        SearchControls.class, "setSearchScope", "(I)V");
    if (SearchControls.setSearchScope == 0) return -1;

    SearchControls.setCountLimit = getMethodID(env,
        SearchControls.class, "setCountLimit", "(J)V");
    if (SearchControls.setCountLimit == 0) return -1;

    SearchControls.setTimeLimit = getMethodID(env,
        SearchControls.class, "setTimeLimit", "(I)V");
    if (SearchControls.setTimeLimit == 0) return -1;

    SearchControls.setReturningAttributes = getMethodID(env,
        SearchControls.class, "setReturningAttributes", "([Ljava/lang/String;)V");
    if (SearchControls.setReturningAttributes == 0) return -1;

    //------------------------------------------------------------------------------------------
    // javax.naming.directory.SearchResult
    //------------------------------------------------------------------------------------------

    SearchResult.class = findClass(env, "javax/naming/directory/SearchResult");
    if (SearchResult.class == 0) return -1;

    SearchResult.getName = getMethodID(env,
        SearchResult.class, "getName", "()Ljava/lang/String;");
    if (SearchResult.getName == 0) return -1;

    SearchResult.getAttributes = getMethodID(env,
        SearchResult.class, "getAttributes", "()Ljavax/naming/directory/Attributes;");
    if (SearchResult.getAttributes == 0) return -1;

    //------------------------------------------------------------------------------------------
    // javax.naming.directory.BasicAttributes
    //------------------------------------------------------------------------------------------

    BasicAttributes.class = findClass(env, "javax/naming/directory/BasicAttributes");
    if (BasicAttributes.class == 0) return -1;

    BasicAttributes.constructor = getMethodID(env,
        BasicAttributes.class, "<init>", "()V");
    if (BasicAttributes.constructor == 0) return -1;

    //------------------------------------------------------------------------------------------
    // javax.naming.directory.Attributes
    //------------------------------------------------------------------------------------------

    Attributes.class = findClass(env, "javax/naming/directory/Attributes");
    if (Attributes.class == 0) return -1;

    Attributes.getAll = getMethodID(env, Attributes.class,
        "getAll", "()Ljavax/naming/NamingEnumeration;");
    if (Attributes.getAll == 0) return -1;

    Attributes.put = getMethodID(env, Attributes.class,
        "put", "(Ljavax/naming/directory/Attribute;)Ljavax/naming/directory/Attribute;");
    if (Attributes.put == 0) return -1;

    //------------------------------------------------------------------------------------------
    // javax.naming.directory.BasicAttribute
    //------------------------------------------------------------------------------------------

    BasicAttribute.class = findClass(env, "javax/naming/directory/BasicAttribute");
    if (BasicAttribute.class == 0) return -1;

    BasicAttribute.constructor = getMethodID(env, BasicAttribute.class, "<init>", "(Ljava/lang/String;)V");
    if (BasicAttribute.constructor == 0) return -1;

    //------------------------------------------------------------------------------------------
    // javax.naming.directory.Attribute
    //------------------------------------------------------------------------------------------

    java_backend_Attribute.class = findClass(env, "javax/naming/directory/Attribute");
    if (java_backend_Attribute.class == 0) return -1;

    java_backend_Attribute.getID = getMethodID(env, java_backend_Attribute.class,
        "getID", "()Ljava/lang/String;");
    if (java_backend_Attribute.getID == 0) return -1;

    java_backend_Attribute.getAll = getMethodID(env, java_backend_Attribute.class,
        "getAll", "()Ljavax/naming/NamingEnumeration;");
    if (java_backend_Attribute.getAll == 0) return -1;

    java_backend_Attribute.add = getMethodID(env, java_backend_Attribute.class,
        "add", "(Ljava/lang/Object;)Z");
    if (java_backend_Attribute.add == 0) return -1;

    //------------------------------------------------------------------------------------------
    // javax.naming.directory.ModificationItem
    //------------------------------------------------------------------------------------------

    ModificationItem.class = findClass(env, "javax/naming/directory/ModificationItem");
    if (ModificationItem.class == 0) return -1;

    ModificationItem.constructor = getMethodID(env, ModificationItem.class, "<init>",
        "(ILjavax/naming/directory/Attribute;)V");
    if (ModificationItem.constructor == 0) return -1;

    //------------------------------------------------------------------------------------------
    // javax.naming.NamingEnumeration
    //------------------------------------------------------------------------------------------

    NamingEnumeration.class = findClass(env, "javax/naming/NamingEnumeration");
    if (NamingEnumeration.class == 0) return -1;

    NamingEnumeration.hasMore = getMethodID(env, NamingEnumeration.class, "hasMore", "()Z");
    if (NamingEnumeration.hasMore == 0) return -1;

    NamingEnumeration.next = getMethodID(env, NamingEnumeration.class, "next", "()Ljava/lang/Object;");
    if (NamingEnumeration.next == 0) return -1;

    //------------------------------------------------------------------------------------------
    // org.safehaus.penrose.ldapbackend.Backend
    //------------------------------------------------------------------------------------------

    java_backend_Backend.class = findClass(env, config.className);
    if (java_backend_Backend.class == 0) return -1;

    java_backend_Backend.constructor = getMethodID(env, java_backend_Backend.class, "<init>", "()V");
    if (java_backend_Backend.constructor == 0) return -1;

    java_backend_Backend.init = getMethodID(env, java_backend_Backend.class, "init", "()I");
    if (java_backend_Backend.init == 0) return -1;

    java_backend_Backend.contains = getMethodID(env, java_backend_Backend.class, "contains", "(Ljava/lang/String;)Z");
    if (java_backend_Backend.contains == 0) return -1;

    java_backend_Backend.createSession = getMethodID(env, java_backend_Backend.class, "createSession", "(I)Lorg/safehaus/penrose/ldapbackend/Session;");
    if (java_backend_Backend.createSession == 0) return -1;

    java_backend_Backend.getSession = getMethodID(env, java_backend_Backend.class, "getSession", "(I)Lorg/safehaus/penrose/ldapbackend/Session;");
    if (java_backend_Backend.getSession == 0) return -1;

    java_backend_Backend.closeSession = getMethodID(env, java_backend_Backend.class, "closeSession", "(I)V");
    if (java_backend_Backend.closeSession == 0) return -1;

    //------------------------------------------------------------------------------------------
    // org.safehaus.penrose.ldapbackend.SearchResponse
    //------------------------------------------------------------------------------------------

    Results.class = findClass(env, "org/safehaus/penrose/ldapbackend/Results");
    if (Results.class == 0) return -1;

    Results.hasNext = getMethodID(env, Results.class, "hasNext", "()Z");
    if (Results.hasNext == 0) return -1;

    Results.next = getMethodID(env, Results.class, "next", "()Ljava/lang/Object;");
    if (Results.next == 0) return -1;

    Results.getReturnCode = getMethodID(env, Results.class, "getReturnCode", "()I");
    if (Results.getReturnCode == 0) return -1;

    //------------------------------------------------------------------------------------------
    // org.safehaus.penrose.ldapbackend.Session
    //------------------------------------------------------------------------------------------

    Session.class = findClass(env, "org/safehaus/penrose/ldapbackend/Session");
    if (Session.class == 0) return -1;

    Session.bind = getMethodID(env, Session.class, "bind",
        "(Ljava/lang/String;Ljava/lang/String;)I");
    if (Session.bind == 0) return -1;

    Session.unbind = getMethodID(env, Session.class, "unbind",
        "()I");
    if (Session.unbind == 0) return -1;

    Session.search = getMethodID(env, Session.class, "search",
        "(Ljava/lang/String;Ljava/lang/String;Ljavax/naming/directory/SearchControls;)Lorg/safehaus/penrose/ldapbackend/Results;");
    if (Session.search == 0) return -1;

    Session.add = getMethodID(env, Session.class, "add",
        "(Ljava/lang/String;Ljavax/naming/directory/Attributes;)I");
    if (Session.add == 0) return -1;

    Session.delete = getMethodID(env, Session.class, "delete",
        "(Ljava/lang/String;)I");
    if (Session.delete == 0) return -1;

    Session.modify = getMethodID(env, Session.class, "modify",
        "(Ljava/lang/String;Ljava/util/Collection;)I");
    if (Session.modify == 0) return -1;

    Session.modRdn = getMethodID(env, Session.class, "modrdn",
        "(Ljava/lang/String;Ljava/lang/String;Z)I");
    if (Session.modRdn == 0) return -1;

    Session.compare = getMethodID(env, Session.class, "compare",
        "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/Object;)I");
    if (Session.compare == 0) return -1;

    return LDAP_SUCCESS;
}
