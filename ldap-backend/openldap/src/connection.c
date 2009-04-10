/**
 * @author Endi S. Dewata
 */

#include "portable.h"
#include <stdio.h>
#include "slap.h"
#include "java-backend.h"

int java_connection_init(BackendDB *be, Connection *conn) {

    DEBUG("==> java_connection_init()\n");

    JNIEnv *env;
    jint res = (*jvm)->AttachCurrentThread(jvm, (void**)&env, NULL);

    if (res < 0) {
        DEBUG("<== java_connection_init(): Failed connecting to JVM.\n");
        return -1;
    }

    jlong connectionId = conn->c_connid;

    DEBUG1("backend.createSession(%d);\n", connectionId);

    (*env)->CallVoidMethod(env, backend, java_backend_Backend.createSession, connectionId);

    jthrowable exc = (*env)->ExceptionOccurred(env);

    if (exc) {
        DEBUG("<== java_connection_init(): Failed initializing connection.\n");

        (*env)->ExceptionDescribe(env);
        (*env)->ExceptionClear(env);

        return -1;
    }

    DEBUG("<== java_connection_init()\n");

    return 0;
}

int java_connection_destroy(BackendDB *be, Connection *conn) {

    DEBUG("==> java_connection_destroy()\n");

    JNIEnv *env;
    jint res = (*jvm)->AttachCurrentThread(jvm, (void**)&env, NULL);

    if (res < 0) {
        DEBUG("<== java_connection_destroy(): Failed attaching to JVM.\n");
        return -1;
    }

    jlong connectionId = conn->c_connid;

    DEBUG1("backend.closeSession(%d);\n", connectionId);

    (*env)->CallVoidMethod(env, backend, java_backend_Backend.closeSession, connectionId);

    jthrowable exc = (*env)->ExceptionOccurred(env);

    if (exc) {
        DEBUG("<== java_connection_destroy(): Failed destroying connection.\n");

        (*env)->ExceptionDescribe(env);
        (*env)->ExceptionClear(env);

        return -1;
    }

    DEBUG("<== java_connection_destroy()\n");

    return 0;
}
