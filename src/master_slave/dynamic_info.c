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
#include "machine_status.h"

/*====================private declaration====================*/
//static void get_network_stat(unsigned long int total_num[16]);
static int can_send_machine_heart_beat();
static int can_send_sub_cluster_heart_beat();

/*
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
*/
/**
 * 用于判断一个节点是否能正常工作
 * TODO
 */
static int can_send_machine_heart_beat()
{
	struct timeval t;
	suseconds_t total_msec;

	gettimeofday(&t, NULL);

	total_msec = (t.tv_sec - last_machine_heart_beat_time.tv_sec) * 1000000;

	total_msec += (t.tv_usec - last_machine_heart_beat_time.tv_usec);

	if(total_msec > PROB_INTERVAL)
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

		if(sub_scheduler_on == 0)
		{
			break;
		}
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

	if(max_byte_per_prob_interval != 0)
	{
		ret = total_byte / max_byte_per_prob_interval;
	}
	else
	{
		ret = 0;
	}

	return ret;
}
