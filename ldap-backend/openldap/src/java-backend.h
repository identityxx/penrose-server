/**
 * @author Endi S. Dewata
 */

#ifndef JAVA_BACKEND_H
#define JAVA_BACKEND_H 1

#include <jni.h>

#include "portable.h"
#include "slap.h"

LDAP_BEGIN_DECL

#ifdef __CYGWIN__
#define PATH_SEPARATOR ";"
#else
#define PATH_SEPARATOR ":"
#endif

#define DEBUG(message)                    Debug(LDAP_DEBUG_TRACE, message, 0, 0, 0)
#define DEBUG1(message, arg1)             Debug(LDAP_DEBUG_TRACE, message, arg1, 0, 0)
#define DEBUG2(message, arg1, arg2)       Debug(LDAP_DEBUG_TRACE, message, arg1, arg2, 0)
#define DEBUG3(message, arg1, arg2, arg3) Debug(LDAP_DEBUG_TRACE, message, arg1, arg2, arg3)

jstring newStringUTF(JNIEnv *env, char *str);
jclass findClass(JNIEnv *env, char *className);
jmethodID getMethodID(JNIEnv *env, jclass clazz, char *methodName, char *methodSignature);

struct String {
    jclass class;
} String;

struct Iterator {
    jclass class;
    jmethodID hasNext;
    jmethodID next;
} Iterator;

struct ArrayList {
    jclass class;
    jmethodID constructor;
    jmethodID add;
    jmethodID get;
    jmethodID size;
    jmethodID toArray;
} ArrayList;

struct SearchControls {
    jclass class;
    jmethodID constructor;
    jmethodID setSearchScope;
    jmethodID setCountLimit;
    jmethodID setTimeLimit;
    jmethodID setReturningAttributes;
} SearchControls;

struct SearchResult {
    jclass class;
    jmethodID getName;
    jmethodID getAttributes;
} SearchResult;

struct BasicAttributes {
    jclass class;
    jmethodID constructor;
} BasicAttributes;

struct Attributes {
    jclass class;
    jmethodID getAll;
    jmethodID put;
} Attributes;

struct BasicAttribute {
    jclass class;
    jmethodID constructor;
} BasicAttribute;

struct java_backend_Attribute {
    jclass class;
    jmethodID getID;
    jmethodID getAll;
    jmethodID add;
} java_backend_Attribute;

struct NamingEnumeration {
    jclass class;
    jmethodID hasMore;
    jmethodID next;
} NamingEnumeration;

struct ModificationItem {
    jclass class;
    jmethodID constructor;
} ModificationItem;

struct java_backend_Backend {
    jclass class;
    jmethodID constructor;
    jmethodID init;
    jmethodID contains;
    jmethodID createSession;
    jmethodID getSession;
    jmethodID closeSession;
} java_backend_Backend;

struct Session {
    jclass class;
    jmethodID bind;
    jmethodID unbind;
    jmethodID search;
    jmethodID add;
    jmethodID delete;
    jmethodID modify;
    jmethodID modRdn;
    jmethodID compare;
} Session;

struct Results {
    jclass class;
    jmethodID hasNext;
    jmethodID next;
    jmethodID getReturnCode;
} Results;

typedef struct Config {

    char *className;

    int nclasspath;
    char **classpath;

    int nlibpath;
    char **libpath;

    int nproperties;
    char **properties;

    int noptions;
    char **options;

    int nsuffixes;
    char **suffixes;

} Config;

extern JavaVM *jvm;
extern Config config;
extern jobject backend;

LDAP_END_DECL

#include "proto-java.h"

#endif

