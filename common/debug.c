#include "stdio.h"
#include "execinfo.h"
#include "stdlib.h"

void print_stack(void);

/**
 * int atexit(void (*func)(void));
 * 注册终止函数(即main执行结束后调用的函数),可以多达32个
 */
void exit_debug(void)
{
	atexit(print_stack);
}

void print_stack(void)
{
	void *array[20];
	int size;
	char **strings;
	int i;

	size = backtrace(array, 20);

	strings = backtrace_symbols(array, size);

	for(i=0;i<size;i++)
	{
		printf("%d : %s\n", i, strings[i]);
	}
}

