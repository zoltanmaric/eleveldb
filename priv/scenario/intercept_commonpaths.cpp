
// Included by using "libfi -E /path/to/this/file"

// #include <sys/file.h>
// #include <fcntl.h>
// #include <unistd.h>

static char *interesting_strings[] = {
    /* Interesting eleveldb things */
    (char *) ".log",
    (char *) "CURRENT",
    (char *) "LOCK",
    (char *) "LOG",
    (char *) "MANIFEST",
    (char *) "sst_",
    (char *) ".db",
    /* Interesting bitcask things */
    (char *) "bitcask.data",
    (char *) "bitcask.hint",
    (char *) "generic.qc" /* Used by generic_qc_fsm.erl */
};

static char *goofy_strings[] = {
    (char *) "goofy1",
    (char *) "goofy2"
};

char **examine_args_string[] = {
    interesting_strings,
    goofy_strings
};
int examine_args_string_lastindex[] = {
    9,
    1
};

// GAH!!!! OS X (and perhaps Linux) won't allow me to use
//         LIBFI's FUNCTION_BODY_x86() macro to interpose
//         open(2) or any number of other system call wrappers.
//         Short of auto-generating yet another header file that will
//         correctly define the constant that I need ... I will just
//         hard-wire them here.

#define GAH___LOCK_SH 0x01
#define GAH___LOCK_EX 0x02

#ifdef __APPLE__
#define GAH___F_SETLK  8
#define GAH___F_SETLKW 9
#endif /* __APPLE__ */
#ifdef linux
#define GAH___F_SETLK  6
#define GAH___F_SETLKW 7
#endif /* linux */
#ifdef __FreeBSD__
#define GAH___F_SETLK  6
#define GAH___F_SETLKW 7
#endif /* __FreeBSD__ */

#define GAH___O_WRONLY 0x0001
#define GAH___O_RDWR   0x0002

static int flock_op_array[] = { GAH___LOCK_SH, GAH___LOCK_EX };
static int fcntl_cmd_array[] = { GAH___F_SETLK, GAH___F_SETLKW };
static int open_write_op_array[] = { GAH___O_WRONLY, GAH___O_RDWR };

int *examine_args_int[] = {
    flock_op_array,
    fcntl_cmd_array,
    open_write_op_array
};
int examine_args_int_lastindex[] = {
    1,
    1,
    1
};
