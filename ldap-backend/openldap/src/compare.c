/**
 * @author Endi S. Dewata
 */

#include "portable.h"
#include <stdio.h>
#include "slap.h"
#include "java-backend.h"

int java_backend_compare(Operation *op, SlapReply *rs) {

    DEBUG("==> java_backend_compare()\n");

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

    AttributeAssertion *ava = (AttributeAssertion*)op->orc_ava;
    char *name = ava->aa_desc->ad_cname.bv_val;
    char *value = ava->aa_value.bv_val;

    DEBUG1("Target DN: %s\n", dn);
    DEBUG1("Name: %s\n", name);
    DEBUG1("Value: %s\n", value);

    ////////////////////////////////////////////////////////////////////////////////
    // Conversion
    ////////////////////////////////////////////////////////////////////////////////

    jstring jdn = (*env)->NewStringUTF(env, dn);
    jstring jname = (*env)->NewStringUTF(env, name);
    jstring jvalue = (*env)->NewStringUTF(env, value);

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
    // Compare
    ////////////////////////////////////////////////////////////////////////////////

    DEBUG("session.compare(dn, name, value);\n");
    res = (*env)->CallIntMethod(env, session, Session.compare, jdn, jname, jvalue);

    if ((*env)->ExceptionOccurred(env)) {
        DEBUG("Failed to compare values.\n");
        res = LDAP_OPERATIONS_ERROR;
        goto exception_handler;
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
    DEBUG("<== java_backend_compare()\n");
    return LDAP_SUCCESS;
}


