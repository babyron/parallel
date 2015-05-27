/**
 *该文件用于记录日志信息，提供程序运行过程中所产生的响应数据
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
//#include "./common/api.h"
#include "./structure/data.h"
#include "data_computation.h"

time_t start_up_start_time;
time_t start_up_finish_time;

char file_path[128];

/**
 * MPI初始化开始运行时间
 */
void log_before_start_up()
{
	start_up_start_time = time(NULL);
}

/**
 * MPI初始化结束时间
 */
void log_after_start_up()
{
	start_up_finish_time = time(NULL);
}

void log_record_start_up()
{
	FILE *fp;
	char id_c[6];

	strcpy(file_path,"./log/log.");
	itoa(id_c, global_machine_id);
	strcat(file_path, id_c);

	fp = fopen(file_path,"w");

	fprintf(fp,"START_UP\n");

	fprintf(fp,"start_up_start_time$%ld$%d\n", (long)start_up_start_time, global_machine_id);
	fprintf(fp,"start_up_finish_time$%ld$%d\n", (long)start_up_finish_time, global_machine_id);
	fclose(fp);
}

void log_API(char *msg, msg_t type, time_t start, time_t finish)
{
	FILE *fp;
	int is_record_msg;

	is_record_msg = 1;

	if(type == MACHINE_HEART_BEAT || type == SUB_CLUSTER_HEART_BEAT)
	{
		return;
	}

	pthread_mutex_lock(&log_m_lock);

	fp = fopen(file_path, "a");

	fprintf(fp, "%s\n", "API\n");

	if(is_record_msg == 1)
	{
		fprintf(fp, "%ld$%ld$%d$%d$%d$%s\n", start, finish, global_machine_id, local_machine_role, sub_master_comm_id, msg);
	}
	else
	{
		fprintf(fp, "%ld$%ld$%d$%d$%d$%s\n", start, finish, global_machine_id, local_machine_role, sub_master_comm_id, "NULL");
	}

	fclose(fp);

	pthread_mutex_unlock(&log_m_lock);
}

void log_main_master()
{
	FILE *fp;

	struct sub_cluster_status_list_element *t_sub_cluster_list;
	int job_num;
	int group_num;
	int free_machine_num;
	int unavailable_machine_num;
	int in_group_machine_num;
	int *group_machine_num_array;
	int i;

	job_num = running_job_num;

	pthread_mutex_lock(&sub_cluster_list_m_lock);

	t_sub_cluster_list = sub_cluster_list;
	group_num = 0;
	while(t_sub_cluster_list != NULL)
	{
		group_num ++;
		t_sub_cluster_list = t_sub_cluster_list->next;
	}

	group_machine_num_array = (int *)malloc(group_num * sizeof(int));

	t_sub_cluster_list = sub_cluster_list;
	i = 0;
	while(t_sub_cluster_list != NULL)
	{
		group_machine_num_array[i] = t_sub_cluster_list->sub_machine_num;
		t_sub_cluster_list = t_sub_cluster_list->next;
	}

	pthread_mutex_unlock(&sub_cluster_list_m_lock);

	pthread_mutex_lock(&master_machine_array_m_lock);

	free_machine_num = 0;
	unavailable_machine_num = 0;
	in_group_machine_num = 0;
	for(i = 0; i < master_machine_num; i++)
	{
		if(master_machine_array[i].machine_status==1)
		{
			free_machine_num++;
		}
		else if(master_machine_array[i].machine_status==0)
		{
			unavailable_machine_num++;
		}
		else
		{
			in_group_machine_num++;
		}
	}

	pthread_mutex_unlock(&master_machine_array_m_lock);

	pthread_mutex_lock(&log_m_lock);

	fp = fopen(file_path,"a");

	fprintf(fp,"MAIN_MASTER\n");
	fprintf(fp,"%ld$%d$%d$%d$%d$%d\n",time(NULL),job_num,group_num,free_machine_num,in_group_machine_num,unavailable_machine_num);

	for(i = 0; i < group_num; i++)
	{
		fprintf(fp, "%d,", group_machine_num_array[i]);
	}
	fprintf(fp,"$\n");

	fclose(fp);

	pthread_mutex_unlock(&log_m_lock);
}

void log_sub_master()
{
	FILE *fp;
	struct schedule_list_element *t_schedule_list;
	int schedule_list_task_num;

	pthread_mutex_lock(&schedule_list_m_lock);

	t_schedule_list = schedule_list;
	schedule_list_task_num = 0;
	while(t_schedule_list!=NULL)
	{
		schedule_list_task_num++;
		t_schedule_list = t_schedule_list->next;
	}

	pthread_mutex_unlock(&schedule_list_m_lock);

	pthread_mutex_lock(&log_m_lock);

	fp = fopen(file_path,"a");

	fprintf(fp,"SUB_MASTER\n");
	fprintf(fp,"%d\n",schedule_list_task_num);

	fclose(fp);

	pthread_mutex_unlock(&log_m_lock);
}

void log_error(char *msg)
{
	FILE *fp;

	pthread_mutex_lock(&log_m_lock);

	fp = fopen(file_path,"a");

	fprintf(fp,"ERROR\n");
	fprintf(fp,"%ld$%d$%d$%d$%s\n",(long)time(NULL),global_machine_id,local_machine_role,sub_master_comm_id,msg);

	fclose(fp);


	fp = fopen("./log/error.file","a");

	fprintf(fp,"%s,%s\n",file_path,msg);

	fclose(fp);

	pthread_mutex_unlock(&log_m_lock);
}
