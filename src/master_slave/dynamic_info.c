#define _GNU_SOURCE

#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <pthread.h>
#include "./structure/data.h"
#include "./common/api.h"
#include "data_computation.h"
#include "dynamic_info.h"

/*====================private declaration====================*/
static void get_version();
static void get_cpu_and_network_load();
static void get_cpu_and_network_load_red_hat();
static float calc_cpu_load(struct cpu_stat_element t1,struct cpu_stat_element t2);
static void get_cpu_stat(struct cpu_stat_element *t);
static void get_memory_load();
static void get_memory_load_red_hat();
static void get_network_stat(unsigned long int total_num[16]);
static int can_send_machine_heart_beat();
static int can_send_sub_cluster_heart_beat();

//TODO
static void get_version()
{
	version = 0;
}

static void get_cpu_and_network_load()
{

	if(version==0)
	{
		get_cpu_and_network_load_red_hat();
	}
}


static void get_cpu_and_network_load_red_hat()
{
	struct cpu_stat_element t1,t2;
	unsigned long int total_num1[16];
	unsigned long int total_num2[16];
	unsigned int network_load;
	float cpu_load;

	get_cpu_stat(&t1);
//	get_network_stat(total_num1);

	usleep(PROB_INTERVAL);

	get_cpu_stat(&t2);
//	get_network_stat(total_num2);

	cpu_load = calc_cpu_load(t1,t2);
//	network_load = calc_network_load(total_num1,total_num2);

	network_load = 0;

//	local_machine_status.CPU_free = (local_machine_status.CPU_free)*0.5+((1.0-cpu_load)*1000*CPU_free_factor)*0.5;
	local_machine_status.CPU_free = 10000;

//	local_machine_status.network_free = (local_machine_status.network_free)*0.5+((1.0-network_load)*1000*network_free_factor)*0.5;
	local_machine_status.network_free = 10000;
}

static float calc_cpu_load(struct cpu_stat_element t1,struct cpu_stat_element t2)
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

	ret = 1.0 - (float)idle_differ/(float)total_differ;

	return ret;
}

static void get_cpu_stat(struct cpu_stat_element *t)
{
	FILE *fp;
	char *parameter;
	char *line;
	int ret;
	size_t len;

	line = NULL;

	fp = fopen("/proc/stat","r");
	if(fp==NULL)
	{
		perror("open proc stat error!!\n");
		log_error("open proc stat error!!\n");
		exit(1);
	}
	getline(&line,&len,fp);
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

static void get_memory_load()
{
	if(version==0)
	{
		get_memory_load_red_hat();
	}
}

static void get_memory_load_red_hat()
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

static void get_network_stat(unsigned long int total_num[16])
{
	FILE *fp;
	char *line;
	char *parameter;
	char *sub;
	unsigned long int num[16];

	size_t len;
	int ret;
	int i;

	fp = fopen("/proc/net/dev","r");

	line = NULL;
	getline(&line,&len,fp);
	free(line);

	for(i = 0; i < 16; i++)
	{
		total_num[i] = 0;
	}


	while(1)
	{
		line = NULL;
		ret = getline(&line, &len, fp);
		if(ret == -1)
		{
			free(line);
			break;
		}
		if(strstr(line,APP_ETH) != NULL)
		{
			strtok(line, ":");
			parameter = strtok(NULL, ":");
			sscanf(parameter, "%ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld",&num[0],&num[1],&num[2],&num[3],&num[4],&num[5],&num[6],&num[7],&num[8],&num[9],&num[10],&num[11],&num[12],&num[13],&num[14],&num[15]);
			for(i = 0; i < 16; i++)
			{
				total_num[i] += num[i];
			}

			free(line);		//only APP_ETH
			break;
		}
		free(line);
	}

	fclose(fp);
}

/**
 * 用于判断一个节点是否能正常工作
 */
static int can_send_machine_heart_beat()
{
	struct timeval t;
	suseconds_t total_msec;

	gettimeofday(&t, NULL);

	total_msec = (t.tv_sec - last_machine_heart_beat_time.tv_sec)*1000000;

	total_msec += (t.tv_usec - last_machine_heart_beat_time.tv_usec);

	if(total_msec>PROB_INTERVAL)
	{
		return 1;
	}
	else
	{
		return 0;
	}
}

static int can_send_sub_cluster_heart_beat()
{
	struct timeval t;
	suseconds_t total_msec;

	gettimeofday(&t, NULL);

	total_msec = (t.tv_sec - last_sub_cluster_heart_beat_time.tv_sec) * 1000000;
	if(total_msec<0)
	{
		printf("sec is negtive!!!\n");
		log_error("sec is negtive!!\n");
		exit(1);
	}

	total_msec += (t.tv_usec - last_sub_cluster_heart_beat_time.tv_usec);

	if(total_msec>PROB_INTERVAL)
	{
		return 1;
	}
	else
	{
		return 0;
	}
}

/**
 * 该心跳函数用于检验节点是否工作正常，如果正常的话则一定时间有心跳发出，否则没有
 */
void send_machine_heart_beat()
{
	static int is_first;
	if(is_first == 0)
	{
		API_machine_heart_beat();
		gettimeofday(&last_machine_heart_beat_time, NULL);
		additional_sub_task_count = 0;
		is_first = 1;
	}
	else
	{
		if(can_send_machine_heart_beat())
		{
			API_machine_heart_beat();
			gettimeofday(&last_machine_heart_beat_time, NULL);
			additional_sub_task_count = 0;
		}
	}
}

//send heart beat info
void send_sub_cluster_heart_beat()
{
	static int is_first;

	if(is_first == 0)
	{
		API_sub_cluster_heart_beat();
		gettimeofday(&last_sub_cluster_heart_beat_time, NULL);
		is_first = 1;
	}
	else
	{
		if(can_send_sub_cluster_heart_beat())
		{
			API_sub_cluster_heart_beat();
			gettimeofday(&last_sub_cluster_heart_beat_time, NULL);
		}
	}
}

void *sub_cluster_heart_beat_daemon(void *arg)
{
	pthread_setcanceltype(PTHREAD_CANCEL_DEFERRED, NULL);

	while(1)
	{
		send_sub_cluster_heart_beat();

		sleep(2);

		if(sub_scheduler_on==0)
		{
			break;
		}
	}

	return NULL;
}

void *machine_heart_beat_daemon(void *arg)
{
	while(1)
	{
		send_machine_heart_beat();

		if(local_machine_role == 0)
		{
			sleep(2);
		}
		else
		{
			sleep(1);
		}
	}
}

void *dynamic_info_get_daemon(void *arg)
{
	get_version();

	while(1)
	{
		get_cpu_and_network_load();
		get_memory_load();
	}

	return NULL;
}

int calc_network_load(unsigned long int total_num1[16], unsigned long int total_num2[16])
{
	unsigned long int recv_byte;
	unsigned long int send_byte;
	unsigned long int total_byte;
	int ret;

	recv_byte = total_num2[0] - total_num1[0];
	send_byte = total_num2[8] - total_num1[8];

	total_byte = recv_byte + send_byte;

	if(max_byte_per_prob_interval!=0)
	{
		ret = total_byte/max_byte_per_prob_interval;
	}
	else
	{
		ret = 0;
	}

	return ret;
}
