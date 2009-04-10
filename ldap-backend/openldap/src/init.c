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

#include "java-backend.h"

JavaVM *jvm = NULL;
Config config;
jobject backend;

int init_jvm(JNIEnv** env) {

    DEBUG("==> init_jvm()\n");

    int rc = 0;

    JavaVMInitArgs vm_args;
    vm_args.version = JNI_VERSION_1_4;

    JNI_GetDefaultJavaVMInitArgs(&vm_args);

    vm_args.nOptions = 0;
    if (config.nclasspath > 0) vm_args.nOptions++;
    if (config.nlibpath > 0) vm_args.nOptions++;
    if (config.nproperties > 0) vm_args.nOptions += config.nproperties;
    if (config.noptions > 0) vm_args.nOptions += config.noptions;

    DEBUG1("Creating %d option(s):\n", vm_args.nOptions);

    vm_args.options = (JavaVMOption*)ch_calloc(vm_args.nOptions, sizeof(JavaVMOption));
    int counter = 0;
    jint i;

    char classpath[1024];
    if (config.nclasspath > 0) {
        classpath[0] = 0;

        for (i=0; i<config.nclasspath; i++) {
            if (i > 0) {
                strcat(classpath, PATH_SEPARATOR);
            }
            strcat(classpath, config.classpath[i]);
        }

        vm_args.options[counter].optionString = (char*)ch_malloc(1024);
        snprintf(vm_args.options[counter].optionString, 1024, "-Djava.class.path=%s", classpath);
        DEBUG2("[%d] %s\n", counter, vm_args.options[counter].optionString);

        counter++;
    }

    char libpath[1024];
    if (config.nlibpath > 0) {
        libpath[0] = 0;

        for (i=0; i<config.nlibpath; i++) {
            if (i > 0) {
                strcat(libpath, PATH_SEPARATOR);
            }
            strcat(libpath, config.libpath[i]);
        }

        vm_args.options[counter].optionString = (char*)ch_malloc(1024);
        snprintf(vm_args.options[counter].optionString, 1024, "-Djava.ext.dirs=%s", libpath);
        DEBUG2("[%d] %s\n", counter, vm_args.options[counter].optionString);

        counter++;
    }

    for (i=0; i<config.nproperties; i++) {

        vm_args.options[counter].optionString = (char*)ch_malloc(1024);
        snprintf(vm_args.options[counter].optionString, 1024, "-D%s", config.properties[i]);
        DEBUG2("[%d] %s\n", counter, vm_args.options[counter].optionString);

        counter++;
    }

    for (i=0; i<config.noptions; i++) {

        vm_args.options[counter].optionString = (char*)ch_malloc(1024);
        snprintf(vm_args.options[counter].optionString, 1024, "%s", config.options[i]);
        DEBUG2("[%d] %s\n", counter, vm_args.options[counter].optionString);

        counter++;
    }

    vm_args.ignoreUnrecognized = JNI_FALSE;

    jint res = JNI_CreateJavaVM(&jvm, (void **)env, &vm_args);

    if (res < 0) {
        DEBUG1("Can't create JVM (%i).\n", res);

        rc = -1;
        goto exit;
    }

    DEBUG("JVM created.\n");

exit:
    DEBUG("<== init_jvm()\n");
    return rc;
}

int java_backend_open(BackendInfo *bi) {

    DEBUG("==> java_backend_open()\n");

    DEBUG("<== java_backend_open()\n");

    return 0;
}

int java_backend_db_init(BackendDB *be) {

    DEBUG("==> java_backend_db_init()\n");

    DEBUG("<== java_backend_db_init()\n");

    return 0;
}

int init_backend(JNIEnv* env) {

    DEBUG("==> init_backend()\n");

    int rc = 0;

    DEBUG1("Backend backend = new %s();\n", config.className);
    backend = (*env)->NewObject(env, java_backend_Backend.class, java_backend_Backend.constructor);

    if ((*env)->ExceptionOccurred(env)) {
        DEBUG("Exception occured.\n");
        rc = -1;
        goto exception_handler;
    }

    if (backend == 0) {
        DEBUG("Failed creating backend instance.\n");
        rc = -1;
        goto exit;
    }

    DEBUG("backend.init();\n");
    jint res = (*env)->CallIntMethod(env, backend, java_backend_Backend.init);

    if ((*env)->ExceptionOccurred(env)) {
        DEBUG("Backend initialization failed.\n");
        rc = -1;
        goto exception_handler;
    }

exception_handler:
    (*env)->ExceptionDescribe(env);
    (*env)->ExceptionClear(env);

exit:
    DEBUG("<== init_backend()\n");
    return rc;
}

int java_backend_db_open(BackendDB *be) {

    DEBUG("==> java_backend_db_open()\n");

    int rc = 0;
    JNIEnv *env;

    if (init_jvm(&env) != 0) {
        DEBUG("JVM initialization failed.\n");
        rc = -1;
        goto exit;
    }

    if (initClasses(env) != 0) {
        DEBUG("Class initialization failed.\n");
        rc = -1;
        goto exit;
    }

    if (init_backend(env) != 0) {
        DEBUG("Backend initialization failed.\n");
        rc = -1;
        goto exit;
    }

exit:
    DEBUG("<== java_backend_db_open()\n");
    return rc;
}

int java_back_initialize(BackendInfo *bi) {

    DEBUG("==> java_back_initialize()\n");

    bi->bi_open       = java_backend_open;
    bi->bi_config     = java_backend_config;
    bi->bi_close      = java_backend_close;
    bi->bi_destroy    = java_backend_destroy;

    bi->bi_db_init    = java_backend_db_init;
    bi->bi_db_config  = java_backend_db_config;
    bi->bi_db_open    = java_backend_db_open;
    bi->bi_db_close   = java_backend_db_close;
    bi->bi_db_destroy = java_backend_db_destroy;

    bi->bi_op_bind    = java_backend_bind;
    bi->bi_op_unbind  = java_backend_unbind;
    bi->bi_op_search  = java_backend_search;
    bi->bi_op_compare = java_backend_compare;
    bi->bi_op_modify  = java_backend_modify;
    bi->bi_op_modrdn  = java_backend_modrdn;
    bi->bi_op_add     = java_backend_add;
    bi->bi_op_delete  = java_backend_delete;
    bi->bi_op_abandon = 0;
    bi->bi_op_cancel  = 0;

    bi->bi_extended = 0;

    bi->bi_chk_referrals = 0;

    bi->bi_connection_init    = java_connection_init;
    bi->bi_connection_destroy = java_connection_destroy;

    DEBUG("<== java_back_initialize()\n");

    return 0;
}

#if SLAPD_JAVA == SLAPD_MOD_DYNAMIC

#ifdef SLAP_BACKEND_INIT_MODULE

SLAP_BACKEND_INIT_MODULE( java )

#else

int init_module(int argc, char *argv[]) {
    BackendInfo bi;

    memset(&bi, '\0', sizeof(bi));
    bi.bi_type = "java";
    bi.bi_init = java_back_initialize;

    backend_add(&bi);

    return 0;
}

#endif

#endif
