/**
 * @author Endi S. Dewata
 */

#include "portable.h"
#include <stdio.h>
#include "slap.h"
#include "java-backend.h"

int java_backend_bind(Operation *op, SlapReply *rs) {

    DEBUG("==> java_backend_bind()\n");

    jint res = LDAP_SUCCESS;

    jlong connectionId = op->o_conn->c_connid;
    DEBUG1("Connection ID: %d\n", connectionId);

    ////////////////////////////////////////////////////////////////////////////////
    // JVM
    ////////////////////////////////////////////////////////////////////////////////

    DEBUG("Attaching to JVM.\n");

    JNIEnv *env;
    res = (*jvm)->AttachCurrentThread(jvm, (void**)&env, NULL);

    if (res < 0) {
        DEBUG("Failed attaching to JVM.\n");
        res = LDAP_OPERATIONS_ERROR;
        goto send_result;
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Parameters
    ////////////////////////////////////////////////////////////////////////////////

    char *dn = op->o_req_dn.bv_val;
    char *password = op->orb_cred.bv_val;

    DEBUG1("Bind DN: %s\n", dn);
    DEBUG1("Password: %s\n", password);

    ////////////////////////////////////////////////////////////////////////////////
    // Conversion
    ////////////////////////////////////////////////////////////////////////////////

    jstring jdn = (*env)->NewStringUTF(env, dn);
    jstring jpassword = (*env)->NewStringUTF(env, password);

    ////////////////////////////////////////////////////////////////////////////////
    // Session
    ////////////////////////////////////////////////////////////////////////////////

    DEBUG1("Session session = backend.getSession(%d);\n", connectionId);

    jobject session = (*env)->CallObjectMethod(env,
        backend, java_backend_Backend.getSession, connectionId
    );

    if ((*env)->ExceptionOccurred(env)) {
        DEBUG("Failed getting session.\n");
        res = LDAP_OPERATIONS_ERROR;
        goto exception_handler;
    }

    if (session == NULL) {
        DEBUG1("session = backend.createSession(%d);\n", connectionId);

        session = (*env)->CallObjectMethod(env,
            backend, java_backend_Backend.createSession, connectionId
        );

        if ((*env)->ExceptionOccurred(env)) {
            DEBUG("Failed creating session.\n");
            res = LDAP_OPERATIONS_ERROR;
            goto exception_handler;
        }

        if (session == NULL) {
            DEBUG("Failed creating session.\n");
            res = LDAP_OPERATIONS_ERROR;
            goto send_result;
        }
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Bind
    ////////////////////////////////////////////////////////////////////////////////

    DEBUG("session.bind(dn, password);\n");

    res = (*env)->CallIntMethod(env,
        session, Session.bind, jdn, jpassword
    );

    if (res != LDAP_SUCCESS) {
        DEBUG1("Bind failed. RC=%d.\n", res);
        goto send_result;
    }

    goto detach_jvm;

exception_handler:
    (*env)->ExceptionDescribe(env);
    (*env)->ExceptionClear(env);

send_result:
    rs->sr_err = res;
    send_ldap_result(op, rs);

detach_jvm:
    DEBUG("Detaching JVM.\n");
    (*jvm)->DetachCurrentThread(jvm);

exit:
    return res;
}

int java_backend_unbind(Operation *op, SlapReply *rs) {

    DEBUG("==> java_backend_unbind()\n");

    jlong connectionId = op->o_conn->c_connid;
    DEBUG1("Connection ID: %d\n", connectionId);

    ////////////////////////////////////////////////////////////////////////////////
    // JVM
    ////////////////////////////////////////////////////////////////////////////////

    DEBUG("Attaching to JVM.\n");

    JNIEnv *env;
    jint res = (*jvm)->AttachCurrentThread(jvm, (void**)&env, NULL);

    if (res < 0) {
        DEBUG("Failed attaching to JVM.\n");
        res = LDAP_OPERATIONS_ERROR;
        goto send_result;
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Session
    ////////////////////////////////////////////////////////////////////////////////

    DEBUG1("Session session = backend.getSession(%d);\n", connectionId);

    jobject session = (*env)->CallObjectMethod(env,
        backend, java_backend_Backend.getSession, connectionId
    );

    if ((*env)->ExceptionOccurred(env)) {
        DEBUG("Failed getting session.\n");
        res = LDAP_OPERATIONS_ERROR;
        goto exception_handler;
    }

    if (session == NULL) {
        DEBUG1("session = backend.createSession(%d);\n", connectionId);

        session = (*env)->CallObjectMethod(env,
            backend, java_backend_Backend.createSession, connectionId
        );

        if ((*env)->ExceptionOccurred(env)) {
            DEBUG("Failed creating session.\n");
            res = LDAP_OPERATIONS_ERROR;
            goto exception_handler;
        }

        if (session == NULL) {
            DEBUG("Failed creating session.\n");
            res = LDAP_OPERATIONS_ERROR;
            goto send_result;
        }
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Unbind
    ////////////////////////////////////////////////////////////////////////////////

    DEBUG("session.unbind();\n");

    res = (*env)->CallIntMethod(env,
        session, Session.unbind
    );

    if (res != LDAP_SUCCESS) {
        DEBUG1("Unbind failed. RC=%d.\n", res);
        goto send_result;
    }

    goto send_result;

exception_handler:
    (*env)->ExceptionDescribe(env);
    (*env)->ExceptionClear(env);

send_result:
    rs->sr_err = res;
    send_ldap_result(op, rs);

detach_jvm:
    DEBUG("Detaching JVM.\n");
    (*jvm)->DetachCurrentThread(jvm);

exit:
    DEBUG("<== java_backend_unbind()\n");
    return res;
}


