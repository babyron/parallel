exe_framework:*.* ../common/* ../data.h
	mpicc *.c ../common/*.c -o exe_framework -g -lpthread -lm -rdynamic -Werror -O0
	
clean::
	rm exe_framework

