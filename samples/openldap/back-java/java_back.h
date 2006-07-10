/**
 * @author Endi S. Dewata
 */

#ifndef JAVA_BACK_H
#define JAVA_BACK_H 1

#include <jni.h>

#include "portable.h"
#include "slap.h"

LDAP_BEGIN_DECL

#ifdef WIN32
#define PATH_SEPARATOR ";"
#else
#define PATH_SEPARATOR ":"
#endif

typedef jint (JNICALL *CreateJavaVM_t)(JavaVM **pvm, void **env, void *args);
typedef jint (JNICALL *GetDefaultJavaVMInitArgs_t)(void *args);

extern JavaVM *jvm;

typedef struct java_backend_instance {

    int nclasspath;
    char **classpath;

    int nlibpath;
    char **libpath;

    int nproperty;
    char **property;

    char *className;
    char *configHomeDirectory;
    char *realHomeDirectory;
    char *properties;

    jobject backend;

    // java.lang.String
    jclass stringClass;

    // java.util.Iterator
    jclass iteratorClass;
    jmethodID iteratorHasNext;
    jmethodID iteratorNext;

    // java.io.FileInputStream
    jclass fileInputStreamClass;
    jmethodID fileInputStreamConstructor;

    // java.util.ArrayList
    jclass arrayListClass;
    jmethodID arrayListConstructor;
    jmethodID arrayListAdd;
    jmethodID arrayListGet;
    jmethodID arrayListSize;
    jmethodID arrayListToArray;

    // javax.naming.directory.SearchControls
    jclass searchControlsClass;
    jmethodID searchControlsConstructor;
    jmethodID searchControlsSetSearchScope;
    jmethodID searchControlsSetCountLimit;
    jmethodID searchControlsSetTimeLimit;
    jmethodID searchControlsSetReturningAttributes;

    // javax.naming.directory.SearchResult;
    jclass searchResultClass;
    jmethodID searchResultGetName;
    jmethodID searchResultGetAttributes;

    // javax.naming.directory.BasicAttributes
    jclass basicAttributesClass;
    jmethodID basicAttributesConstructor;

    // javax.naming.directory.Attributes
    jclass attributesClass;
    jmethodID attributesGetAll;
    jmethodID attributesPut;

    // javax.naming.directory.BasicAttribute
    jclass basicAttributeClass;
    jmethodID basicAttributeConstructor;

    // javax.naming.directory.Attribute
    jclass attributeClass;
    jmethodID attributeGetID;
    jmethodID attributeGetAll;
    jmethodID attributeAdd;

    // javax.naming.NamingEnumeration
    jclass namingEnumerationClass;
    jmethodID namingEnumerationNext;
    jmethodID namingEnumerationHasMore;

    // javax.naming.directory.ModificationItem
    jclass modificationItemClass;
    jmethodID modificationItemConstructor;

    // org.openldap.backend.Result
    jclass resultsClass;
    jmethodID resultsConstructor;
    jmethodID resultsNext;
    jmethodID resultsGetReturnCode;

    // org.openldap.backend.Backend
    jclass backendClass;
    jmethodID backendConstructor;
    jmethodID backendSetHomeDirectory;
    jmethodID backendSetSuffix;
    jmethodID backendSetSchema;
    jmethodID backendSetRoot;
    jmethodID backendSetProperties;
    jmethodID backendInit;
    jmethodID backendOpenConnection;
    jmethodID backendRemoveConnection;
    jmethodID backendBind;
    jmethodID backendUnbind;
    jmethodID backendSearch;
    jmethodID backendAdd;
    jmethodID backendDelete;
    jmethodID backendModify;
    jmethodID backendModRdn;
    jmethodID backendCompare;
} JavaBackend;

typedef struct java_config_list {
    char **argv;
    int argc;
    struct java_config_list *next;
} JavaConfigList;

typedef struct java_backend_db_instance {
    char *conn_classname;
    char *bdb_classname;

    struct JavaConfigList *config_first;
    struct JavaConfigList *config_last;
} JavaBackendDB;

extern JavaBackend *java_back;
extern JavaBackendDB *java_back_db;

LDAP_END_DECL

#include "proto-java.h"

#endif

