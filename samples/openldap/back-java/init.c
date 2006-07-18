/**
 * @author Endi S. Dewata
 */

#include "portable.h"
#include <stdio.h>

#include "slap.h"
#include "lber.h"

#ifndef __CYGWIN__
#include "dlfcn.h"
#endif

#include "java_back.h"

JNIEnv *env = NULL;
JavaVM *jvm = NULL;
JavaBackend *java_back = NULL;
//JavaBackendDB *java_back_db = NULL;

void *handle = NULL;
void *libjavaHandle = NULL;
void *libverifyHandle = NULL;


typedef struct {
    CreateJavaVM_t CreateJavaVM;
    GetDefaultJavaVMInitArgs_t GetDefaultJavaVMInitArgs;
} InvocationFunctions;

#ifdef SLAPD_JAVA_DYNAMIC

int back_java_LTX_init_module(
    int argc, 
    char *argv[]
) 
{
    BackendInfo bi;

    memset( &bi, '\0', sizeof(bi) );
    bi.bi_type = "java";
    bi.bi_init = java_back_initialize;

    backend_add(&bi);
    return 0;
}

#endif /* SLAPD_JAVA_DYNAMIC */

jstring newStringUTF(
    JNIEnv *env,
    char *str
)
{
    jstring res;

    Debug( LDAP_DEBUG_TRACE, "==> newStringUTF(%s)\n", str, 0, 0);
    res = (*env)->NewStringUTF(env, str);

    if (res == 0) {
        Debug( LDAP_DEBUG_TRACE, "<== newStringUTF(): String not created.\n", 0, 0, 0);
    } else {
        Debug( LDAP_DEBUG_TRACE, "<== newStringUTF(): String created.\n", 0, 0, 0);
    }

    return res;
}

jclass findClass(
    JNIEnv *env,
    char *className
)
{
    jclass clazz;

    Debug( LDAP_DEBUG_TRACE, "==> findClass(\"%s\")\n", className, 0, 0);
    clazz = (*env)->FindClass(env, className);

    if (clazz == 0) {
        Debug( LDAP_DEBUG_TRACE, "<== findClass(): Class not found.\n", 0, 0, 0);
    } else {
        Debug( LDAP_DEBUG_TRACE, "<== findClass(): Class found.\n", 0, 0, 0);
    }

    return clazz;
}

jmethodID getMethodID(
    JNIEnv *env,
    jclass clazz,
    char *methodName,
    char *methodSignature
)
{
    jmethodID mid;

    Debug( LDAP_DEBUG_TRACE, "==> getMethodID(%s, %s)\n", methodName, methodSignature, 0);
    mid = (*env)->GetMethodID(env, clazz, methodName, methodSignature);

    if (mid == 0) {
        Debug( LDAP_DEBUG_TRACE, "<== getMethodID(): Method not found.\n", 0, 0, 0);
    } else {
        Debug( LDAP_DEBUG_TRACE, "<== getMethodID(): Method found.\n", 0, 0, 0);
    }

    return mid;
}

jobject newObject(
    JNIEnv *env,
    jclass clazz,
    jmethodID method,
    ...
)
{
    jobject object;
    va_list args;

    Debug( LDAP_DEBUG_TRACE, "==> newObject()\n", 0, 0, 0);
    va_start(args, method);
    object = (*env)->NewObject(env, clazz, method, args);
    va_end(args);

    if (object == 0) {
        Debug( LDAP_DEBUG_TRACE, "<== newObject(): Object not created\n", 0, 0, 0);
    } else {
        Debug( LDAP_DEBUG_TRACE, "<== newObject(): Object created\n", 0, 0, 0);
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

int exceptionOccurred(
    JNIEnv *env
)
{
    jthrowable exc;

    exc = (*env)->ExceptionOccurred(env);
    if (exc) {
        if (slap_debug & 1024) fprintf(stderr, "Exception occured.\n");
        (*env)->ExceptionDescribe(env);
        (*env)->ExceptionClear(env);
    }       

    return exc ? 1 : 0;
}

int
java_back_initialize(
    BackendInfo	*bi
)
{
    Debug(LDAP_DEBUG_TRACE, "==> java_back_initialize()\n", 0, 0, 0);

    bi->bi_open = java_back_open;
    bi->bi_config = java_back_config;
    bi->bi_close = java_back_close;
    bi->bi_destroy = java_back_destroy;

    bi->bi_db_init = java_back_db_init;
    bi->bi_db_config = java_back_db_config;
    bi->bi_db_open = java_back_db_open;
    bi->bi_db_close = java_back_db_close;
    bi->bi_db_destroy = java_back_db_destroy;

    bi->bi_op_bind = java_back_bind;
    bi->bi_op_unbind = java_back_unbind;
    bi->bi_op_search = java_back_search;
    bi->bi_op_compare = java_back_compare;
    bi->bi_op_modify = java_back_modify;
    bi->bi_op_modrdn = java_back_modrdn;
    bi->bi_op_add = java_back_add;
    bi->bi_op_delete = java_back_delete;
    bi->bi_op_abandon = 0;
    bi->bi_op_cancel = 0;

    bi->bi_extended = 0;

    bi->bi_chk_referrals = 0;

    //bi->bi_acl_group = java_acl_group;
    //bi->bi_acl_attribute = java_acl_attribute;

    bi->bi_connection_init = java_connection_init;
    bi->bi_connection_destroy = java_connection_destroy;

    //------------------------------------------------------------------------------------------
    // Creating JavaBackend
    //------------------------------------------------------------------------------------------

    java_back = (JavaBackend*)ch_malloc(sizeof(JavaBackend));
    memset(java_back, '\0', sizeof(JavaBackend));

    Debug(LDAP_DEBUG_TRACE, "<== java_back_initialize()\n", 0, 0, 0);

    return 0;
}
		
int
java_back_open(
    BackendInfo *bi
)
{
    JavaVMInitArgs vm_args;
    jint i, res, counter;
    char classpath[1024];
    char libpath[1024];

    Debug( LDAP_DEBUG_TRACE, "==> java_back_open()\n", 0, 0, 0);

    //------------------------------------------------------------------------------------------
    // Creating JVM
    //------------------------------------------------------------------------------------------

    vm_args.version = JNI_VERSION_1_4;
    JNI_GetDefaultJavaVMInitArgs(&vm_args);

    vm_args.nOptions = 0;
    if (java_back->nclasspath > 0) vm_args.nOptions++;
    if (java_back->nlibpath > 0) vm_args.nOptions++;
    if (java_back->nproperties > 0) vm_args.nOptions += java_back->nproperties;

    vm_args.options = (JavaVMOption *)ch_calloc(vm_args.nOptions, sizeof(JavaVMOption));
    counter = 0;

    Debug( LDAP_DEBUG_TRACE, "Creating %d option(s):\n", vm_args.nOptions, 0, 0);

    if (java_back->nclasspath > 0) {
        classpath[0] = 0;

        for (i=0; i<java_back->nclasspath; i++) {
            if (i > 0) {
                strcat(classpath, PATH_SEPARATOR);
            }
            strcat(classpath, java_back->classpath[i]);
        }

        vm_args.options[counter].optionString = (char*)ch_malloc(1024);
        snprintf(vm_args.options[counter].optionString, 1024, "-Djava.class.path=%s", classpath);
        Debug( LDAP_DEBUG_TRACE, "[%d] %s\n", counter, vm_args.options[counter].optionString, 0);

        counter++;
    }

    if (java_back->nlibpath > 0) {
        libpath[0] = 0;

        for (i=0; i<java_back->nlibpath; i++) {
            if (i > 0) {
                strcat(libpath, PATH_SEPARATOR);
            }
            strcat(libpath, java_back->libpath[i]);
        }

        vm_args.options[counter].optionString = (char*)ch_malloc(1024);
        snprintf(vm_args.options[counter].optionString, 1024, "-Djava.ext.dirs=%s", libpath);
        Debug( LDAP_DEBUG_TRACE, "[%d] %s\n", counter, vm_args.options[counter].optionString, 0);

        counter++; 
    }

    if (java_back->nproperties > 0) {

        for (i=0; i<java_back->nproperties; i++) {

            vm_args.options[counter].optionString = (char*)ch_malloc(1024);
            snprintf(vm_args.options[counter].optionString, 1024, "-D%s", java_back->properties[i]);
            Debug( LDAP_DEBUG_TRACE, "[%d] %s\n", counter, vm_args.options[counter].optionString, 0);

            counter++;
        }
    }

    vm_args.ignoreUnrecognized = JNI_FALSE;

    //Debug( LDAP_DEBUG_TRACE, "==> JNI_CreateJavaVM()\n", 0, 0, 0);
    res = JNI_CreateJavaVM(&jvm, (void **)&env, &vm_args);
    //Debug( LDAP_DEBUG_TRACE, "<== JNI_CreateJavaVM(): RC=%d\n", res, 0, 0);

/*
#ifndef __CYGWIN__
    char *libjvm = "libjvm.so";
    char *symbol = "JNI_CreateJavaVM";
    fprintf(stderr, "Opening %s\n", libjvm);
    handle = dlopen(libjvm, RTLD_LAZY);
    if (!handle) {
        fprintf(stderr, "Cannot open library '%s': %s\n", libjvm, dlerror());
        return -1;
    }
    fprintf(stderr, "Loading symbol %s\n", symbol);
    CreateJavaVM_t jcvm = (CreateJavaVM_t) dlsym(handle, symbol);
    if (!jcvm) {
        fprintf(stderr, "Cannot load symbol '%s': %s\n", symbol, dlerror());
        dlclose(handle);
        return -1;
    }
    fprintf(stderr, "Symbol loaded! Now instantiating res...\n");
    res = jcvm(&jvm, (void **)&env, &vm_args);
    fprintf(stderr, "res instantiated successfully!\n");
#else
    res = JNI_CreateJavaVM(&jvm, (void **)&env, &vm_args);
#endif
*/
    if (res < 0) {
        Debug( LDAP_DEBUG_TRACE, "<== java_back_open(): Can't create Java VM (%i)\n", res, 0, 0);
        return -1;
    }

    //------------------------------------------------------------------------------------------
    // java.lang.String
    //------------------------------------------------------------------------------------------

    java_back->stringClass = findClass(env, "java/lang/String");
    if (java_back->stringClass == 0) return -1;

    //------------------------------------------------------------------------------------------
    // java.util.Iterator
    //------------------------------------------------------------------------------------------

    java_back->iteratorClass = findClass(env, "java/util/Iterator");
    if (java_back->iteratorClass == 0) return -1;

    java_back->iteratorHasNext = getMethodID(env, java_back->iteratorClass, "hasNext", "()Z");
    if (java_back->iteratorHasNext == 0) return -1;

    java_back->iteratorNext = getMethodID(env, java_back->iteratorClass, "next", "()Ljava/lang/Object;");
    if (java_back->iteratorNext == 0) return -1;

    //------------------------------------------------------------------------------------------
    // java.util.ArrayList
    //------------------------------------------------------------------------------------------

    java_back->arrayListClass = findClass(env, "java/util/ArrayList");
    if (java_back->arrayListClass == 0) return -1;
  
    java_back->arrayListConstructor = getMethodID(env, java_back->arrayListClass, "<init>", "()V");
    if (java_back->arrayListConstructor == 0) return -1;

    java_back->arrayListAdd = getMethodID(env, java_back->arrayListClass, "add", "(Ljava/lang/Object;)Z");
    if (java_back->arrayListAdd == 0) return -1;

    java_back->arrayListGet = getMethodID(env, java_back->arrayListClass, "get", "(I)Ljava/lang/Object;");
    if (java_back->arrayListGet == 0) return -1;

    java_back->arrayListSize = getMethodID(env, java_back->arrayListClass, "size", "()I");
    if (java_back->arrayListSize == 0) return -1;

    java_back->arrayListToArray = getMethodID(env, java_back->arrayListClass, "toArray", "()[Ljava/lang/Object;");
    if (java_back->arrayListToArray == 0) return -1;

    //------------------------------------------------------------------------------------------
    // java.io.FileInputStream
    //------------------------------------------------------------------------------------------

    java_back->fileInputStreamClass = findClass(env, "java/io/FileInputStream");
    if (java_back->fileInputStreamClass == 0) return -1;

    java_back->fileInputStreamConstructor = getMethodID(env, java_back->fileInputStreamClass, "<init>", "(Ljava/lang/String;)V");
    if (java_back->fileInputStreamConstructor == 0) return -1;

    //------------------------------------------------------------------------------------------
    // javax.naming.directory.SearchControls
    //------------------------------------------------------------------------------------------

    java_back->searchControlsClass = findClass(env, "javax/naming/directory/SearchControls");
    if (java_back->searchControlsClass == 0) return -1;
  
    java_back->searchControlsConstructor = getMethodID(env, java_back->searchControlsClass, "<init>", "()V");
    if (java_back->searchControlsConstructor == 0) return -1;

    java_back->searchControlsSetSearchScope = getMethodID(env, java_back->searchControlsClass,
        "setSearchScope", "(I)V");
    if (java_back->searchControlsSetSearchScope == 0) return -1;

    java_back->searchControlsSetCountLimit = getMethodID(env, java_back->searchControlsClass,
        "setCountLimit", "(J)V");
    if (java_back->searchControlsSetCountLimit == 0) return -1;

    java_back->searchControlsSetTimeLimit = getMethodID(env, java_back->searchControlsClass,
        "setTimeLimit", "(I)V");
    if (java_back->searchControlsSetTimeLimit == 0) return -1;

    java_back->searchControlsSetReturningAttributes = getMethodID(env, java_back->searchControlsClass,
        "setReturningAttributes", "([Ljava/lang/String;)V");
    if (java_back->searchControlsSetReturningAttributes == 0) return -1;

    //------------------------------------------------------------------------------------------
    // javax.naming.directory.SearchResult
    //------------------------------------------------------------------------------------------

    java_back->searchResultClass = findClass(env, "javax/naming/directory/SearchResult");
    if (java_back->searchResultClass == 0) return -1;
  
    java_back->searchResultGetName = getMethodID(env, java_back->searchResultClass, "getName", "()Ljava/lang/String;");
    if (java_back->searchResultGetName == 0) return -1;

    java_back->searchResultGetAttributes = getMethodID(env, java_back->searchResultClass,
        "getAttributes", "()Ljavax/naming/directory/Attributes;");
    if (java_back->searchResultGetAttributes == 0) return -1;

    //------------------------------------------------------------------------------------------
    // javax.naming.directory.BasicAttributes
    //------------------------------------------------------------------------------------------

    java_back->basicAttributesClass = findClass(env, "javax/naming/directory/BasicAttributes");
    if (java_back->basicAttributesClass == 0) return -1;
  
    java_back->basicAttributesConstructor = getMethodID(env, java_back->basicAttributesClass, "<init>", "()V");
    if (java_back->basicAttributesConstructor == 0) return -1;

    //------------------------------------------------------------------------------------------
    // javax.naming.directory.Attributes
    //------------------------------------------------------------------------------------------

    java_back->attributesClass = findClass(env, "javax/naming/directory/Attributes");
    if (java_back->attributesClass == 0) return -1;
  
    java_back->attributesGetAll = getMethodID(env, java_back->attributesClass,
        "getAll", "()Ljavax/naming/NamingEnumeration;");
    if (java_back->attributesGetAll == 0) return -1;

    java_back->attributesPut = getMethodID(env, java_back->attributesClass,
        "put", "(Ljavax/naming/directory/Attribute;)Ljavax/naming/directory/Attribute;");
    if (java_back->attributesPut == 0) return -1;

    //------------------------------------------------------------------------------------------
    // javax.naming.directory.BasicAttribute
    //------------------------------------------------------------------------------------------

    java_back->basicAttributeClass = findClass(env, "javax/naming/directory/BasicAttribute");
    if (java_back->basicAttributeClass == 0) return -1;
  
    java_back->basicAttributeConstructor = getMethodID(env, java_back->basicAttributeClass, "<init>", "(Ljava/lang/String;)V");
    if (java_back->basicAttributeConstructor == 0) return -1;

    //------------------------------------------------------------------------------------------
    // javax.naming.directory.Attribute
    //------------------------------------------------------------------------------------------

    java_back->attributeClass = findClass(env, "javax/naming/directory/Attribute");
    if (java_back->attributeClass == 0) return -1;
  
    java_back->attributeGetID = getMethodID(env, java_back->attributeClass,
        "getID", "()Ljava/lang/String;");
    if (java_back->attributeGetID == 0) return -1;

    java_back->attributeGetAll = getMethodID(env, java_back->attributeClass,
        "getAll", "()Ljavax/naming/NamingEnumeration;");
    if (java_back->attributeGetAll == 0) return -1;

    java_back->attributeAdd = getMethodID(env, java_back->attributeClass,
        "add", "(Ljava/lang/Object;)Z");
    if (java_back->attributeAdd == 0) return -1;

    //------------------------------------------------------------------------------------------
    // javax.naming.directory.ModificationItem
    //------------------------------------------------------------------------------------------

    java_back->modificationItemClass = findClass(env, "javax/naming/directory/ModificationItem");
    if (java_back->modificationItemClass == 0) return -1;
  
    java_back->modificationItemConstructor = getMethodID(env, java_back->modificationItemClass, "<init>",
        "(ILjavax/naming/directory/Attribute;)V");
    if (java_back->modificationItemConstructor == 0) return -1;

    //------------------------------------------------------------------------------------------
    // javax.naming.NamingEnumeration
    //------------------------------------------------------------------------------------------

    java_back->namingEnumerationClass = findClass(env, "javax/naming/NamingEnumeration");
    if (java_back->namingEnumerationClass == 0) return -1;
  
    java_back->namingEnumerationHasMore = getMethodID(env, java_back->namingEnumerationClass, "hasMore", "()Z");
    if (java_back->namingEnumerationHasMore == 0) return -1;

    java_back->namingEnumerationNext = getMethodID(env, java_back->namingEnumerationClass, "next", "()Ljava/lang/Object;");
    if (java_back->namingEnumerationNext == 0) return -1;

    //------------------------------------------------------------------------------------------
    // org.openldap.backend.Results
    //------------------------------------------------------------------------------------------

    java_back->resultsClass = findClass(env, "org/openldap/backend/Results");
    if (java_back->resultsClass == 0) return -1;

    java_back->resultsNext = getMethodID(env, java_back->resultsClass, "next", "()Ljava/lang/Object;");
    if (java_back->resultsNext == 0) return -1;

    java_back->resultsGetReturnCode = getMethodID(env, java_back->resultsClass, "getReturnCode", "()I");
    if (java_back->resultsGetReturnCode == 0) return -1;

    //------------------------------------------------------------------------------------------
    // org.openldap.backend.Backend
    //------------------------------------------------------------------------------------------

    java_back->backendClass = findClass(env, java_back->className);
    if (java_back->backendClass == 0) return -1;

    java_back->backendConstructor = getMethodID(env, java_back->backendClass, "<init>", "()V");
    if (java_back->backendConstructor == 0) return -1;

    java_back->backendInit = getMethodID(env, java_back->backendClass, "init", "()I");
    if (java_back->backendInit == 0) return -1;

    java_back->backendOpenConnection = getMethodID(env, java_back->backendClass, "openConnection", "(I)V");
    if (java_back->backendOpenConnection == 0) return -1;

    java_back->backendRemoveConnection = getMethodID(env, java_back->backendClass, "closeConnection", "(I)V");
    if (java_back->backendRemoveConnection == 0) return -1;

    java_back->backendBind = getMethodID(env, java_back->backendClass, "bind",
        "(ILjava/lang/String;Ljava/lang/String;)I");
    if (java_back->backendBind == 0) return -1;
  
    java_back->backendUnbind = getMethodID(env, java_back->backendClass, "unbind", "(I)I");
    if (java_back->backendUnbind == 0) return -1;
  
    java_back->backendSearch = getMethodID(env, java_back->backendClass, "search",
        "(ILjava/lang/String;Ljava/lang/String;Ljavax/naming/directory/SearchControls;)Lorg/openldap/backend/Results;");
    if (java_back->backendSearch == 0) return -1;
  
    java_back->backendAdd = getMethodID(env, java_back->backendClass, "add", "(ILjava/lang/String;Ljavax/naming/directory/Attributes;)I");
    if (java_back->backendAdd == 0) return -1;

    java_back->backendDelete = getMethodID(env, java_back->backendClass, "delete", "(ILjava/lang/String;)I");
    if (java_back->backendDelete == 0) return -1;

    java_back->backendModify = getMethodID(env, java_back->backendClass, "modify",
        "(ILjava/lang/String;Ljava/util/Collection;)I");
    if (java_back->backendModify == 0) return -1;

    java_back->backendModRdn = getMethodID(env, java_back->backendClass, "modrdn",
        "(ILjava/lang/String;Ljava/lang/String;)I");
    if (java_back->backendModRdn == 0) return -1;

    java_back->backendCompare = getMethodID(env, java_back->backendClass, "compare",
        "(ILjava/lang/String;Ljava/lang/String;Ljava/lang/Object;)I");
    if (java_back->backendCompare == 0) return -1;

    Debug(LDAP_DEBUG_TRACE, "<== java_back_open()\n", 0, 0, 0);

    return 0;
}

int
java_back_db_init(
    BackendDB	*be
)
{
    Debug(LDAP_DEBUG_TRACE, "==> java_back_db_init()\n", 0, 0, 0);
    Debug(LDAP_DEBUG_TRACE, "<== java_back_db_init()\n", 0, 0, 0);
/*
    java_back_db = (JavaBackendDB*)ch_malloc(sizeof(JavaBackendDB));
    memset(java_back_db, '\0', sizeof(JavaBackendDB));
*/

    return 0;
}

int
java_back_db_open(
    BackendDB *be
)
{
    jint res;
    jmethodID mid;
    BerValue *psuffix;
    jobjectArray suffixArray;
    int i;
    int length;
    jstring configHomeDirectory;
    jstring realHomeDirectory;
    jstring schemaDn;
    jstring rootDn;
    jstring rootPassword;

    Debug( LDAP_DEBUG_TRACE, "==> java_back_db_open()\n", 0, 0, 0);
    Debug( LDAP_DEBUG_TRACE, "Class clazz = Class.forName(\"%s\");\n", java_back->className, 0, 0);
    Debug( LDAP_DEBUG_TRACE, "Backend backend = (Backend)clazz.newInstance();\n", 0, 0, 0);

    java_back->backend = (*env)->NewObject(env, java_back->backendClass, java_back->backendConstructor);

    if (java_back->backend == 0) {
        Debug( LDAP_DEBUG_TRACE, "<== java_back_db_open(): Failed creating backend instance.\n", 0, 0, 0);
        return -1;
    }

    if (exceptionOccurred(env)) {
        Debug( LDAP_DEBUG_TRACE, "<== java_back_db_open(): Exception occured while creating backend instance.\n", 0, 0, 0);
        return -1;
    }

    Debug( LDAP_DEBUG_TRACE, "backend.init();\n", 0, 0, 0);

    res = (*env)->CallIntMethod(env, java_back->backend, java_back->backendInit);

    if (exceptionOccurred(env)) {
        Debug( LDAP_DEBUG_TRACE, "<== java_back_db_open(): Backend initialization failed.\n", 0, 0, 0);
        return -1;
    }

    Debug( LDAP_DEBUG_TRACE, "<== java_back_db_open()\n", 0, 0, 0);

    return 0;
}

int
java_connection_init(
    BackendDB	*be, Connection *conn
)
{
    JNIEnv *env;
    jint res;
    jmethodID backendCreateConnection;
  
    Debug( LDAP_DEBUG_TRACE, "==> java_connection_init()\n", 0, 0, 0);

    res = (*jvm)->AttachCurrentThread(jvm, (void**)&env, NULL);
    if (res < 0) {
        Debug( LDAP_DEBUG_TRACE, "<== java_connection_init(): Failed connecting to JVM.\n", 0, 0, 0);
        return -1;
    }

    // backend.openConnection();
    (*env)->CallVoidMethod(env, java_back->backend, java_back->backendOpenConnection, conn->c_connid);

    if (exceptionOccurred(env)) {
        Debug( LDAP_DEBUG_TRACE, "<== java_connection_init(): Failed initializing connection.\n", 0, 0, 0);
        return -1;
    }

    Debug( LDAP_DEBUG_TRACE, "<== java_connection_init()\n", 0, 0, 0);

    return 0;
}

int
java_connection_destroy(
    BackendDB	*be, Connection *conn
)
{
    JNIEnv *env;
    jint res;
    jmethodID backendRemoveConnection;

    Debug( LDAP_DEBUG_TRACE, "==> java_connection_destroy()\n", 0, 0, 0);

    res = (*jvm)->AttachCurrentThread(jvm, (void**)&env, NULL);
    if (res < 0) {
        Debug( LDAP_DEBUG_TRACE, "<== java_connection_destroy(): Failed attaching to JVM.\n", 0, 0, 0);
        return -1;
    }

    // backend.removeConnection();
    (*env)->CallVoidMethod(env, java_back->backend, java_back->backendRemoveConnection, conn->c_connid);

    if (exceptionOccurred(env)) {
        Debug( LDAP_DEBUG_TRACE, "<== java_connection_destroy(): Failed destroying connection.\n", 0, 0, 0);
        return -1;
    }

    Debug( LDAP_DEBUG_TRACE, "<== java_connection_destroy()\n", 0, 0, 0);

    return 0;
}

/*
int
java_acl_group(
    Backend	*be,
    Connection	*conn,
    Operation	*op,
    Entry	*e,
    struct berval	*bdn,
    struct berval	*edn,
    ObjectClass	*group_oc,
    AttributeDescription	*group_at
)
{
    Debug( LDAP_DEBUG_TRACE, "==> java_acl_group()\n", 0, 0, 0);
    Debug( LDAP_DEBUG_TRACE, "<== java_acl_group()\n", 0, 0, 0);

    return 0;
}

int
java_acl_attribute(
    Backend	*be,
    Connection	*conn,
    Operation	*op,
    Entry	*e,
    struct berval	*edn,
    AttributeDescription	*group_at,
    BerVarray	*vals
)
{
    Debug( LDAP_DEBUG_TRACE, "==> java_acl_attribute()\n", 0, 0, 0);
    Debug( LDAP_DEBUG_TRACE, "<== java_acl_attribute()\n", 0, 0, 0);

    return 0;
}
*/

#if SLAPD_JAVA == SLAPD_MOD_DYNAMIC

/* conditionally define the init_module() function */
SLAP_BACKEND_INIT_MODULE( java )

#endif /* SLAPD_JAVA == SLAPD_MOD_DYNAMIC */
