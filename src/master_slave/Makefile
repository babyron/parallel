exe_framework:*.* ./common/* ./structure/data.h
	mpicc *.c ./common/*.c -o exe_framework -g -lpthread -lm -rdynamic  -O0
	
clean::
	rm exe_framework

