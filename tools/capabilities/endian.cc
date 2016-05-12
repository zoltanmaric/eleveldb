
/*
    Returns success (zero) if little-endian, failure (non-zero) if big-endian.
    Does NOT check for mixed-endian, which we shouldn't encounter these days.
*/

#if     (defined(__LITTLE_ENDIAN__) && (__LITTLE_ENDIAN__ != 0))
#define ENDIAN_RESULT   0
#elif   (defined(__BIG_ENDIAN__) && (__BIG_ENDIAN__ != 0))
#define ENDIAN_RESULT   1
#else
int one = 1;
#define ENDIAN_RESULT   !*((char *) & one)
#endif

int main(void)
{
    return  ENDIAN_RESULT;
}
