#include <stdio.h>
#include <netdb.h>

int main(int argc, char **argv) {

    if (argc != 2) {
        printf("Usage: test-rpc <name>\n");
        return -1;
    }

    char* name = argv[1];

    struct rpcent* rpc = getrpcbyname(name);

    if (rpc == NULL) {
        printf("RPC %s not found.\n", name);
        return -1;
    }

    printf("Name   : %s\n", rpc->r_name);

    char** alias = rpc->r_aliases;
    while (alias != NULL && (*alias) != NULL) {
        printf("Alias  : %s\n", (*alias));
        alias++;
    }

    printf("Number : %d\n", rpc->r_number);

    return 0;
}
