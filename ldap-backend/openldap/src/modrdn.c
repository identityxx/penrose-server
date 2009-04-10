/**
 * @author Endi S. Dewata
 */

#include "portable.h"
#include <stdio.h>
#include "slap.h"
#include "java-backend.h"

int java_backend_modrdn(Operation *op, SlapReply *rs) {

    DEBUG("==> java_backend_modrdn()\n");

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
    char *newRdn = op->orr_newrdn.bv_val;
    int deleteOldRdn = op->orr_deleteoldrdn;

    DEBUG1("Target DN: %s\n", dn);
    DEBUG1("New RDN: %s\n", newRdn);
    DEBUG1("Delete old RDN: %d\n", deleteOldRdn);

    ////////////////////////////////////////////////////////////////////////////////
    // Conversion
    ////////////////////////////////////////////////////////////////////////////////

    jstring jdn = (*env)->NewStringUTF(env, dn);
    jstring jnewRdn = (*env)->NewStringUTF(env, newRdn);
    jboolean jdeleteOldRdn = deleteOldRdn;

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
    // ModRDN
    ////////////////////////////////////////////////////////////////////////////////

    DEBUG2("session.modrdn(\"%s\", \"%s\")\n", dn, newRdn);

    res = (*env)->CallIntMethod(env,
        session, Session.modRdn, jdn, jnewRdn, jdeleteOldRdn
    );

    if ((*env)->ExceptionOccurred(env)) {
        DEBUG("Failed renaming entry.\n");
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
    DEBUG("<== java_backend_modrdn()\n");
    return LDAP_SUCCESS;
}

