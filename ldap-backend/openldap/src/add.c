/**
 * @author Endi S. Dewata
 */

#include "portable.h"
#include <stdio.h>
#include "slap.h"
#include "lber.h"

#include "java-backend.h"

int java_backend_add(Operation *op, SlapReply *rs) {

    DEBUG("==> java_backend_add()\n");

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
    // Parameter
    ////////////////////////////////////////////////////////////////////////////////

    Entry *e = (Entry*)op->ora_e;
    char *dn = e->e_name.bv_val;

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
    // Conversion
    ////////////////////////////////////////////////////////////////////////////////

    jstring jdn = (*env)->NewStringUTF(env, dn);

    DEBUG("BasicAttributes basicAttributes = new BasicAttributes();\n");
    jobject jattributes = (*env)->NewObject(env,
        BasicAttributes.class, BasicAttributes.constructor
    );

    if ((*env)->ExceptionOccurred(env)) {
        DEBUG("Failed creating BasicAttributes.\n");
        res = LDAP_OPERATIONS_ERROR;
        goto exception_handler;
    }

    Attribute *a = e->e_attrs;

    while (a) {
        AttributeDescription *desc = a->a_desc;
        char *name = desc->ad_cname.bv_val;

        DEBUG1("Attribute attribute = new BasicAttribute(\"%s\");\n", name);

        jstring jname = (*env)->NewStringUTF(env, name);
        jobject jattribute = (*env)->NewObject(env,
            BasicAttribute.class, BasicAttribute.constructor,
            jname
        );

        if ((*env)->ExceptionOccurred(env)) {
            DEBUG("Failed creating BasicAttribute.\n");
            res = LDAP_OPERATIONS_ERROR;
            goto exception_handler;
        }

        BerValue *values = a->a_vals;

        while (values && values->bv_val) {
            char *value = values->bv_val;
            DEBUG1("attribute.add(\"%s\");\n", value);

            jstring jvalue = (*env)->NewStringUTF(env, value);
            (*env)->CallBooleanMethod(env,
                    jattribute, java_backend_Attribute.add,
                jvalue
            );

            if ((*env)->ExceptionOccurred(env)) {
                DEBUG("Failed adding attribute value.\n");
                res = LDAP_OPERATIONS_ERROR;
                goto exception_handler;
            }

            values++;
        }

        DEBUG("attributes.put(attribute);\n");
        (*env)->CallBooleanMethod(env,
            jattributes, Attributes.put,
            jattribute
        );

        if ((*env)->ExceptionOccurred(env)) {
            DEBUG("Failed adding attribute to set.\n");
            res = LDAP_OPERATIONS_ERROR;
            goto exception_handler;
        }

        a = a->a_next;
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Add
    ////////////////////////////////////////////////////////////////////////////////

    DEBUG("session.add(dn, attributes);\n");

    res = (*env)->CallIntMethod(env,
        session, Session.add,
        jdn, jattributes
    );

    if ((*env)->ExceptionOccurred(env)) {
        DEBUG("Failed adding entry.\n");
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
    DEBUG("<== java_backend_add()\n");
    return res;
}

