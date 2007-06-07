#include <stdio.h>
#include <shadow.h>

int main(int argc, char **argv) {

    if (argc != 2) {
        printf("Usage: test-shadow <name>\n");
        return -1;
    }

    char* name = argv[1];

    setspent();

    struct spwd* shadow = getspnam(name);

    if (!shadow) {
        printf("Shadow %s not found.\n", name);
        return -1;
    }

    printf("Username    : %s\n", shadow->sp_namp);
    printf("Password    : %s\n", shadow->sp_pwdp);
    printf("Last change : %d\n", shadow->sp_lstchg);
    printf("Mininum     : %d\n", shadow->sp_min);
    printf("Maximum     : %d\n", shadow->sp_max);
    printf("Warning     : %d\n", shadow->sp_warn);
    printf("Inactive    : %d\n", shadow->sp_inact);
    printf("Expire      : %d\n", shadow->sp_expire);
    printf("Flag        : %d\n", shadow->sp_flag);

    endspent();

    return 0;
}
