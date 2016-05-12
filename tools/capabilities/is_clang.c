
int main(void)
{
#if (defined(__clang__) && (__clang__ > 0))
    return  0;
#else
    return  1;
#endif
}
