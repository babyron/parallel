/*
 * machine_status.c
 *
 *  Created on: 2015年5月28日
 *      Author: ron
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "machine_status.h"
#include "structure/data.h"
#include "data_computation.h"
#include "computing_machine.h"

/*====================private declaration====================*/
static float CPU_free_factor;
static float memory_free_factor;
static float network_free_factor;

static void factors_init() {
	CPU_free_factor = local_machine_status.CPU_core_num;
	memory_free_factor = (float) local_machine_status.memory_total
			/ (1024.0 * 1.5 / 10.0);	//1.5G
	network_free_factor = (float) local_machine_status.network_capacity
			/ (100.0 * 1024.0 / 8.0);	//100mb
	if (network_free_factor == 0) {
		printf("unknown network capacity! default factor = 1.0\n");
		network_free_factor = 1.0;
	}
}

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

//gain cpu cores num
//only Linux operating system works OK
int get_local_machine_CPU_core_num()
{
#if defined(_SC_NPROCESSORS_ONLN)
    return sysconf(_SC_NPROCESSORS_ONLN);
#else
    return 1;
#endif
}

int get_local_machine_memory_total() {
	FILE *fp;
	char *line;
	char sub[20];
	unsigned long int memtotal = 0;
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

	if(memtotal_int == 0){
		//TODO log
	}

	return memtotal_int;
}

int get_local_machine_network_capacity() {
	return 12800;
}

void get_cpu_and_network_load()
{
	get_cpu_and_network_load_red_hat();
}

void get_cpu_and_network_load_red_hat()
{
	struct cpu_stat_element t1, t2;
	unsigned long int total_num1[16];
	unsigned long int total_num2[16];
	unsigned int network_load;
	float cpu_load;

	get_cpu_stat(&t1);
//	get_network_stat(total_num1);

	usleep(PROB_INTERVAL);

	get_cpu_stat(&t2);
//	get_network_stat(total_num2);

	cpu_load = calc_cpu_load(t1, t2);
//	network_load = calc_network_load(total_num1,total_num2);

	network_load = 0;

//	local_machine_status.CPU_free = (local_machine_status.CPU_free)*0.5+((1.0-cpu_load)*1000*CPU_free_factor)*0.5;
	local_machine_status.CPU_free = 10000;

//	local_machine_status.network_free = (local_machine_status.network_free)*0.5+((1.0-network_load)*1000*network_free_factor)*0.5;
	local_machine_status.network_free = 10000;
}

float calc_cpu_load(struct cpu_stat_element t1, struct cpu_stat_element t2)
{
	unsigned long int total_differ;
	unsigned long int idle_differ;
	float ret;

	total_differ = 0;

	total_differ +=(t2.user - t1.user);
	total_differ +=(t2.nice - t1.nice);
	total_differ +=(t2.system - t1.system);
	total_differ +=(t2.idle - t1.idle);
	total_differ +=(t2.io_wait - t1.io_wait);
	total_differ +=(t2.irq - t1.irq);
	total_differ +=(t2.soft_irq - t1.soft_irq);

	idle_differ = t2.idle - t1.idle;

	ret = 1.0 - (float)idle_differ / (float)total_differ;

	return ret;
}

void get_cpu_stat(struct cpu_stat_element *t)
{
	FILE *fp;
	char *parameter;
	char *line;
	int ret;
	size_t len;

	line = NULL;

	fp = fopen("/proc/stat", "r");
	if(fp == NULL)
	{
		perror("open proc stat error!!\n");
		log_error("open proc stat error!!\n");
		exit(1);
	}
	getline(&line, &len, fp);
	ret = fclose(fp);

	strtok(line, " ");

	parameter = strtok(NULL, " ");
	t->user = strtoul(parameter, NULL, 10);

	parameter = strtok(NULL, " ");
	t->nice = strtoul(parameter, NULL, 10);

	parameter = strtok(NULL, " ");
	t->system = strtoul(parameter, NULL, 10);

	parameter = strtok(NULL, " ");
	t->idle = strtoul(parameter, NULL, 10);

	parameter = strtok(NULL, " ");
	t->io_wait = strtoul(parameter, NULL, 10);

	parameter = strtok(NULL, " ");
	t->irq = strtoul(parameter, NULL, 10);

	parameter = strtok(NULL, " ");
	t->soft_irq = strtoul(parameter, NULL, 10);

	free(line);
}

void get_memory_load()
{
	get_memory_load_red_hat();
}

void get_memory_load_red_hat()
{
/*
	FILE *fp;
	char *line;
	char sub[10];
	unsigned long int total_mem;
	unsigned long int free_mem;
	unsigned long int used_mem;
	float rate;
	size_t len;

	fp = fopen("/proc/meminfo","r");

	line = NULL;
	getline(&line,&len,fp);
	sscanf(line,"%s %ld\n",sub,&total_mem);
	free(line);

	line = NULL;
	getline(&line,&len,fp);
	sscanf(line,"%s %ld\n",sub,&free_mem);
	free(line);

	fclose(fp);

	used_mem = total_mem - free_mem;

	rate = (float)free_mem/(float)total_mem;
	local_machine_status.memory_free = rate*1000*memory_free_factor;
*/
	local_machine_status.memory_free = 10000;
}
