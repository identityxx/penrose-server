/**
 * @author Endi S. Dewata
 */

#include "portable.h"
#include <stdio.h>
#include "slap.h"
#include "java-backend.h"

int java_backend_modify( Operation *op, SlapReply *rs ) { 

    DEBUG("==> java_backend_modify()\n");

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
    // Connection
    ////////////////////////////////////////////////////////////////////////////////

    Connection *conn = (Connection *)op->o_conn;

    ////////////////////////////////////////////////////////////////////////////////
    // Parameters
    ////////////////////////////////////////////////////////////////////////////////

    char *dn = op->o_req_dn.bv_val;
    Modifications *modifications = (Modifications *)op->orm_modlist;

    ////////////////////////////////////////////////////////////////////////////////
    // Conversion
    ////////////////////////////////////////////////////////////////////////////////

    jstring jdn = (*env)->NewStringUTF(env, dn);

    DEBUG("ArrayList modifications = new ArrayList();\n");

    jobject jmodifications = (*env)->NewObject(env, ArrayList.class, ArrayList.constructor);

    Modifications *m = modifications;
    while (m) {
        Modification *mod = &m->sml_mod;

        jint modOp = mod->sm_op;

        AttributeDescription *desc = mod->sm_desc;
        char *attributeName = desc->ad_cname.bv_val;

        jstring jattributeName = (*env)->NewStringUTF(env, attributeName);

        DEBUG1("Attribute attribute = new BasicAttribute(\"%s\");\n", attributeName);

        jobject attribute = (*env)->NewObject(env,
            BasicAttribute.class, BasicAttribute.constructor, jattributeName
        );

        if ((*env)->ExceptionOccurred(env)) {
            DEBUG("Failed creating attribute.\n");
            res = LDAP_OPERATIONS_ERROR;
            goto exception_handler;
        }

        BerValue *v = mod->sm_values;

        while (v && v->bv_val) {
        	char *value = v->bv_val;
            jstring jvalue = (*env)->NewStringUTF(env, value);

            DEBUG1("attribute.add(\"%s\");\n", value);

            (*env)->CallBooleanMethod(env, attribute, java_backend_Attribute.add, jvalue);

            if ((*env)->ExceptionOccurred(env)) {
                DEBUG("Failed adding attribute value.\n");
                res = LDAP_OPERATIONS_ERROR;
                goto exception_handler;
            }

            v++;
        }

        DEBUG1("ModificationItem modification = new ModificationItem(%d, attribute);\n", modOp);

        jobject modification = (*env)->NewObject(env,
            ModificationItem.class, ModificationItem.constructor,
            modOp, attribute
        );

        if ((*env)->ExceptionOccurred(env)) {
            DEBUG("Failed creating modification.\n");
            res = LDAP_OPERATIONS_ERROR;
            goto exception_handler;
        }

        DEBUG("modifications.add(modification);");
 
        res = (*env)->CallIntMethod(env, jmodifications, ArrayList.add, modification);

        if ((*env)->ExceptionOccurred(env)) {
            DEBUG("Failed adding modification to modifications.\n");
            res = LDAP_OPERATIONS_ERROR;
            goto exception_handler;
        }

        m = m->sml_next;
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
    // Modify
    ////////////////////////////////////////////////////////////////////////////////

    DEBUG1("session.modify(\"%s\", modifications);\n", dn);

    res = (*env)->CallIntMethod(env,
        session, Session.modify, jdn, jmodifications
    );

    if ((*env)->ExceptionOccurred(env)) {
        DEBUG("Failed modifying entry.\n");
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
    DEBUG("<== java_backend_modify()\n");
    return LDAP_SUCCESS;
}

