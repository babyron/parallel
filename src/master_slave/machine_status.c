/*
 * machine_status.c
 *
 *  Created on: 2015年5月28日
 *      Author: ron
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "machine_status.h"
#include "structure/data.h"

void init_local_machine_status() {
	local_machine_status.CPU_core_num = get_local_machine_CPU_core_num();
	local_machine_status.GPU_core_num = 5;
	local_machine_status.CPU_free = 1000;
	local_machine_status.GPU_load = 0;
	local_machine_status.memory_free = 1000;
	local_machine_status.network_free = 0;
	local_machine_status.IO_bus_capacity = 100;
	local_machine_status.network_capacity = get_local_machine_network_capacity();
	local_machine_status.memory_total = get_local_machine_memory_total();
	local_machine_status.memory_swap = 0;

	factors_init();
}

int get_version1() {
	return 0;
}

int get_local_machine_CPU_core_num() {
	int version;

	version = get_version1();	//??何用
	if (version == 0) {
		return get_local_machine_CPU_core_num_red_hat();
	} else {
		printf("unknown version");
		log_error("unknown version");
		exit(1);
		return -1;
	}
}

int get_local_machine_CPU_core_num_red_hat() {
	FILE *fp;
	char *line;
	size_t len;
	int core_num;
	size_t ret;

	fp = fopen("/proc/stat", "r");

	core_num = 0;

	while (1) {
		line = NULL;
		ret = getline(&line, &len, fp);
		if (ret == -1) {
			free(line);
			break;
		} else {
			if (strstr(line, "cpu")) {
				core_num++;
			}
			free(line);
		}
	}
	core_num--;
	fclose(fp);

	return core_num;
}

int get_local_machine_memory_total_red_hat() {
	FILE *fp;
	char *line;
	char sub[20];
	unsigned long int memtotal;
	int memtotal_int;
	size_t len;
	int ret;

	fp = fopen("/proc/meminfo", "r");

	while (1) {
		line = NULL;
		ret = getline(&line, &len, fp);
		if (ret == -1) {
			free(line);
			break;
		} else {
			if (strstr(line, "MemTotal")) {
				sscanf(line, "%s %ld", sub, &memtotal);
				free(line);
				break;
			}
			free(line);
		}
	}

	memtotal_int = memtotal / 1024 / 10;
	fclose(fp);

	return memtotal_int;
}

int get_local_machine_network_capacity() {
	FILE *fp;
	char cmd[40];
	char *line;
	char *save_ptr;
	char *parameter;
	size_t len;
	int speed;
	int ret;

	speed = 100;
	return (speed * 1024) / 8;
}

int get_local_machine_memory_total() {
	int version;

	version = get_version1();

	if (version == 0) {
		return get_local_machine_memory_total_red_hat();
	} else {
		printf("unknown version");
		log_error("unknown version");
		exit(1);
		return -1;
	}
}


