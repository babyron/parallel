#include "stdio.h"
#include "unistd.h"
#include "pthread.h"
#include "sys/socket.h"
#include "arpa/inet.h"
#include "strings.h"
#include "stdlib.h"
#include "string.h"
#include "../data.h"
#include "../master_slave/data_computation.h"
#include "assert.h"
#include "communication.h"
#include "sys/types.h"
#include "signal.h"
#include "sys/msg.h"

void master_get_machine_ip(int machine_id,char *ip);
char *itoa(int num);
void sub_get_machine_ip(int machine_id,char *ip);
void fill_schedule_unit_assign_msg(struct schedule_unit_description_element schedule_unit,char *send_msg,int num,char *priority_modified_msg);
long int get_msg_type(int type,int job_id,int top_id,int id[10]);
struct child_wait_all_list_element *find_element_in_child_wait_all_list_c(int type,int job_id,int top_id,int id[10]);
void delete_element_from_child_wait_all_list_c(struct child_wait_all_list_element *t);
struct child_wait_all_list_element *find_element_in_child_wait_all_list_s(int type,int job_id,int top_id,int id[10]);
struct child_wait_all_list_element *find_element_in_child_wait_all_list_c(int type,int job_id,int top_id,int id[10]);
void delete_element_from_child_wait_all_list_s(struct child_wait_all_list_element *t);
struct sub_cluster_status_list_element *get_sub_cluster_element(int sub_cluster_id);
void check_modified_priority(struct sub_cluster_status_list_element *list,int *num,char **ret_char);
void check_modified_priority_without_lock(struct sub_cluster_status_list_element *list,int *num,char **ret_char);
void fill_modified_priority_msg(struct schedule_unit_priority_list_element *t,int new_priority,char *tt);
int master_get_sub_task_priority(int type,int job_id,int top_id,int *parent_id);
struct sub_cluster_status_list_element *get_sub_cluster_element_without_lock(int sub_cluster_id);
int sub_cluster_heart_beat_data_available();
int sub_find_machine_comm_id(int sub_machine_id);

int API_sub_cluster_heart_beat()
{
	char *send_msg;
	char *t_arg;
	char *ret_msg;
	int i;

	if(sub_cluster_heart_beat_data_available()!=1)
	{
		printf("quit not ava!\n");
		return 0;
	}

	send_msg = (char *)malloc((6+sub_machine_num*25+1)*sizeof(char));

	t_arg = itoa(sub_machine_num);
	strcpy(send_msg,t_arg);
	free(t_arg);
	strcat(send_msg,",");

	for(i=0;i<sub_machine_num;i++)
	{

		t_arg = itoa(sub_machine_array[i].machine_description.CPU_free);
		if(sub_machine_array[i].machine_description.CPU_free==0)
		{
			printf("send is 0\n");
			log_error("send is 0\n");
			exit(1);
		}
		strcat(send_msg,t_arg);
		free(t_arg);
		strcat(send_msg,"_");

		t_arg = itoa(sub_machine_array[i].machine_description.GPU_load);
		strcat(send_msg,t_arg);
		free(t_arg);
		strcat(send_msg,"_");

		t_arg = itoa(sub_machine_array[i].machine_description.memory_free);
		strcat(send_msg,t_arg);
		free(t_arg);
		strcat(send_msg,"_");

		t_arg = itoa(sub_machine_array[i].machine_description.network_free);
		strcat(send_msg,t_arg);
		free(t_arg);
		strcat(send_msg,"_");
	}

	send_recv_msg(0,0,SUB_CLUSTER_HEART_BEAT,send_msg,&ret_msg);
	free(ret_msg);

	free(send_msg);

	return 1;
}

int sub_cluster_heart_beat_data_available()
{
	int i;

	for(i=0;i<sub_machine_num;i++)
	{
		if(sub_machine_array[i].machine_description.CPU_free==0)
		{
			printf("CPU not ava\n");
			return 0;
		}

		if(sub_machine_array[i].machine_description.memory_free==0)
		{
			printf("mem not ava\n");
			return 0;
		}

		if(sub_machine_array[i].machine_description.network_free==0)
		{
			printf("net not ava\n");
			return 0;
		}
	}

	return 1;
}

/**
 * 用于向主节点返回自身信息的心跳守护进程
 * 如果是子主节点的从节点或计算节点，向子主节点发送心跳
 * 如果是空闲节点或是子主节点,向主节点发送心跳
 */
int API_machine_heart_beat()
{
	char send_msg[24];
	char *ret_msg;
	char *t_arg;


	t_arg = itoa(local_machine_status.CPU_free);
	strcpy(send_msg,t_arg);
	free(t_arg);
	strcat(send_msg,",");

	t_arg = itoa(local_machine_status.GPU_load);
	strcat(send_msg,t_arg);
	free(t_arg);
	strcat(send_msg,",");

	t_arg = itoa(local_machine_status.memory_free);
	strcat(send_msg,t_arg);
	free(t_arg);
	strcat(send_msg,",");

	t_arg = itoa(local_machine_status.network_free);
	strcat(send_msg,t_arg);
	free(t_arg);

	if(local_machine_role==SUB_MASTER_MACHINE||local_machine_role==COMPUTATION_MACHINE)
	{
		if(sub_master_comm_id==0)
		{
			printf("sub_master_comm_id==0\n");
			log_error("sub_master_comm_id==0\n");
			exit(1);
		}
		send_recv_msg(sub_master_comm_id,1,MACHINE_HEART_BEAT,send_msg,&ret_msg);//sub_master_id属于什么变量？？
		free(ret_msg);
	}
	else if(local_machine_role==FREE_MACHINE||local_machine_role==HALF_SUB_MASTER_MACHINE)
	{
		send_recv_msg(0,0,MACHINE_HEART_BEAT,send_msg,&ret_msg);
		free(ret_msg);
	}
	else
	{
		printf("machine heart beat unknown role!!!\n");
		log_error("machine heart bear unknow role!!!\n");
		exit(1);
	}

	return 1;
}


int API_sub_cluster_shut_down(int sub_master_id)
{
	char ip[16];
	char send_msg[10];
	char *ret_msg;

	strcpy(send_msg,"SHUT_DOWN");

	send_recv_msg(sub_master_id,1,SUB_CLUSTER_SHUT_DOWN,send_msg,&ret_msg);
	free(ret_msg);

	return 1;
}

int API_back_to_main_master(int comm_source)
{
	char *ret_msg;
	char send_msg[5];

	strcpy(send_msg,"BACK");

	printf("API_ back to main master comm source = %d\n",comm_source);
	send_recv_msg(comm_source,2,BACK_TO_MAIN_MASTER,send_msg,&ret_msg);
	free(ret_msg);

	return 1;
}

int API_sub_cluster_destroy(int sub_master_id)
{
	char ip[16];
	char *ret_msg;
	char send_msg[5];

	strcpy(send_msg,"QIUT");

	send_recv_msg(sub_master_id,1,SUB_CLUSTER_DESTROY,send_msg,&ret_msg);
	free(ret_msg);

	return 1;
}

/*
int API_get_sub_task_ip_m_to_s(char *send_msg,char *sub_task_ip)
{
	char *ret_msg;

	send_recv_msg(sub_master_ip,1,GET_SUB_TASK_IP,send_msg,&ret_msg);

	strcpy(sub_task_ip,ret_msg+4);

	free(ret_msg);

	return 1;
}

int API_get_sub_task_ip_s_to_m(char *send_msg,char *sub_task_ip)
{
	char *ret_msg;

	send_recv_msg(master_ip,0,GET_SUB_TASK_IP,send_msg,&ret_msg);

	strcpy(sub_task_ip,ret_msg+4);

	free(ret_msg);

	return 1;
}

int API_get_sub_task_ip_c_to_s(char *send_msg,char *sub_task_ip)
{
	char *ret_msg;

	send_recv_msg(sub_master_ip,1,GET_SUB_TASK_IP,send_msg,&ret_msg);

	strcpy(sub_task_ip,ret_msg+4);

	free(ret_msg);

	return 1;
}
*/

int API_child_wake_up_all_c_to_p(int type,int job_id,int top_id,int id[10],char *ret_arg)
{
	struct child_wait_all_list_element *t;
	long int msg_type;
	char *send_msg;
	void *final_send_msg;
	char *t_msg;
	int msg_queue_id;
	int pid;
	int ret;
	int i;

	send_msg = (char *)malloc(24+60+strlen(ret_arg)+1);

	t_msg = itoa(type);
	strcpy(send_msg,t_msg);
	free(t_msg);
	strcat(send_msg,",");

	t_msg = itoa(job_id);
	strcat(send_msg,t_msg);
	free(t_msg);
	strcat(send_msg,",");

	t_msg = itoa(top_id);
	strcat(send_msg,t_msg);
	free(t_msg);
	strcat(send_msg,",");

	for(i=0;i<10;i++)
	{
		t_msg = itoa(id[i]);
		strcat(send_msg,t_msg);
		free(t_msg);
		strcat(send_msg,"_");
	}
	strcat(send_msg,"|");
	strcat(send_msg,ret_arg);

	pthread_mutex_lock(&child_wait_all_list_c_m_lock);

	t = find_element_in_child_wait_all_list_c(type,job_id,top_id,id);
	pid = t->pid;
	delete_element_from_child_wait_all_list_c(t);

	pthread_mutex_unlock(&child_wait_all_list_c_m_lock);

	msg_queue_id = msgget(WAKE_UP_MSG_QUEUE_KEY,IPC_CREAT|0666);
	if(msg_queue_id==-1)
	{
		perror("API_child_wake_up_all_c_to_p,msgget error\n");
		log_error("API_child_wake_up_all_c_to_p,msgget error\n");
		exit(1);
	}
	msg_type = get_msg_type(type,job_id,top_id,id);

	final_send_msg = (void *)malloc(sizeof(long int)+strlen(send_msg)+1*sizeof(char));

	memcpy((void *)final_send_msg,(void *)&msg_type,sizeof(long int));
	memcpy((void *)((char *)final_send_msg+(int)sizeof(long int)),(void *)send_msg,(size_t)(strlen(send_msg)+1*sizeof(char)));
	ret = msgsnd(msg_queue_id,final_send_msg,sizeof(long int)+strlen(send_msg)+1*sizeof(char),0);
	if(ret==-1)
	{
		perror("API_child_wake_up_all_c_to_p,msgsnd error\n");
		log_error("API_child_wake_up_all_c_to_p,msgsnd error\n");
		exit(1);
	}

	kill(pid,SIGUSR1);
	
	free(final_send_msg);
	free(send_msg);

	return 1;
}

void delete_element_from_child_wait_all_list_c(struct child_wait_all_list_element *t)
{
	struct child_wait_all_list_element *t_child_wait_all_list_c;

	assert(child_wait_all_list_c!=NULL);

	if(child_wait_all_list_c==t)
	{
		child_wait_all_list_c = child_wait_all_list_c->next;
	}
	else
	{
		t_child_wait_all_list_c = child_wait_all_list_c;
		while(t_child_wait_all_list_c->next!=NULL)
		{
			if(t_child_wait_all_list_c->next==t)
			{
				t_child_wait_all_list_c->next = t->next;
				break;
			}

			t_child_wait_all_list_c = t_child_wait_all_list_c->next;
		}
	}

	free(t);
}



struct child_wait_all_list_element *find_element_in_child_wait_all_list_s(int type,int job_id,int top_id,int id[10])
{
	struct child_wait_all_list_element *t_child_wait_all_list_s;
	int i;

	t_child_wait_all_list_s = child_wait_all_list_s;
//	printf("t = %d,job = %d top = %d id %d %d %d!!!\n",type,job_id,top_id,id[0],id[1],id[2]);
	while(t_child_wait_all_list_s!=NULL)
	{
//		printf("list: type = %d, job_id = %d top_id = %d ids:%2d,%2d,%2d\n",t_child_wait_all_list_s->type,t_child_wait_all_list_s->job_id,t_child_wait_all_list_s->top_id,t_child_wait_all_list_s->id[0],t_child_wait_all_list_s->id[1],t_child_wait_all_list_s->id[2]);
		if((t_child_wait_all_list_s->type==type)&&(t_child_wait_all_list_s->job_id==job_id))
		{
			if(type==0)
			{
				if(t_child_wait_all_list_s->top_id==top_id)
				{
					break;
				}
			}
			else
			{
				for(i=0;i<10;i++)
				{
					if(t_child_wait_all_list_s->id[i]!=id[i])
					{
						break;
					}
				}
				if(i==10)
				{
					break;
				}
			}
		}

		t_child_wait_all_list_s = t_child_wait_all_list_s->next;
	}

	assert(t_child_wait_all_list_s!=NULL);

	return t_child_wait_all_list_s;
}

struct child_wait_all_list_element *find_element_in_child_wait_all_list_c(int type,int job_id,int top_id,int id[10])
{
	struct child_wait_all_list_element *t_child_wait_all_list_c;
	int i;

	t_child_wait_all_list_c = child_wait_all_list_c;
	while(t_child_wait_all_list_c!=NULL)
	{
		if((t_child_wait_all_list_c->type==type)&&(t_child_wait_all_list_c->job_id==job_id))
		{
			if(type==0)
			{
				if(t_child_wait_all_list_c->top_id==top_id)
				{
					break;
				}
			}
			else
			{
				for(i=0;i<10;i++)
				{
					if(t_child_wait_all_list_c->id[i]!=id[i])
					{
						break;
					}
				}
				if(i==10)
				{
					break;
				}
			}
		}

		t_child_wait_all_list_c = t_child_wait_all_list_c->next;
	}

	assert(t_child_wait_all_list_c!=NULL);

	return t_child_wait_all_list_c;
}

int API_child_wake_up_all_s_to_c(int type,int job_id,int top_id,int id[10],char *ret_arg)
{
	struct child_wait_all_list_element *t;
	char *send_msg;
	char *t_msg;
	char *ret_msg;
	char ip[16];
	int sub_machine_id;
	int comm_source;
	int len;
	int i;

	len = strlen(ret_arg);

	send_msg = (char *)malloc((24+60+len)*sizeof(char));

	pthread_mutex_lock(&child_wait_all_list_s_m_lock);

	t = find_element_in_child_wait_all_list_s(type,job_id,top_id,id);
	sub_machine_id = t->machine_id;
	delete_element_from_child_wait_all_list_s(t);

	pthread_mutex_unlock(&child_wait_all_list_s_m_lock);
	comm_source = sub_find_machine_comm_id(sub_machine_id);

//	find comm source of machine_id global

	t_msg = itoa(type);
	strcpy(send_msg,t_msg);
	free(t_msg);
	strcat(send_msg,",");

	t_msg = itoa(job_id);
	strcat(send_msg,t_msg);
	free(t_msg);
	strcat(send_msg,",");

	t_msg = itoa(top_id);
	strcat(send_msg,t_msg);
	free(t_msg);
	strcat(send_msg,",");

	for(i=0;i<10;i++)
	{
		t_msg = itoa(id[i]);
		strcat(send_msg,t_msg);
		free(t_msg);
		strcat(send_msg,"_");
	}
	strcat(send_msg,",");

	strcat(send_msg,ret_arg);
	send_recv_msg(comm_source,2,CHILD_WAKE_UP_ALL,send_msg,&ret_msg);

	free(send_msg);
	free(ret_msg);

	return 1;
}

void delete_element_from_child_wait_all_list_s(struct child_wait_all_list_element *t)
{
	struct child_wait_all_list_element *t_child_wait_all_list_s;

	assert(child_wait_all_list_s!=NULL);

	if(t==child_wait_all_list_s)
	{
		child_wait_all_list_s = child_wait_all_list_s->next;
		free(t);
	}
	else
	{
		t_child_wait_all_list_s = child_wait_all_list_s;
		while(t_child_wait_all_list_s->next!=NULL)
		{
			if(t_child_wait_all_list_s->next==t)
			{
				t_child_wait_all_list_s->next = t->next;
				break;
			}

			t_child_wait_all_list_s = t_child_wait_all_list_s->next;
		}
		free(t);
	}
}

struct child_wait_all_list_element *find_machine_id_in_child_wait_all_list_s(int type,int job_id,int top_id,int id[10])
{
	struct child_wait_all_list_element *t_child_wait_all_list_s;
	int machine_id;
	int i;

	t_child_wait_all_list_s = child_wait_all_list_s;
	while(t_child_wait_all_list_s!=NULL)
	{
		if((t_child_wait_all_list_s->type==type)&&(t_child_wait_all_list_s->job_id==job_id))
		{
			if(type==0)
			{
				if(t_child_wait_all_list_s->top_id==top_id)
				{
					machine_id = t_child_wait_all_list_s->machine_id;
					break;
				}
			}
			else
			{
				for(i=0;i<10;i++)
				{
					if(t_child_wait_all_list_s->id[i]!=id[i])
					{
						break;
					}
				}
				if(i==10)
				{
					machine_id = t_child_wait_all_list_s->machine_id;
					break;
				}
			}
		}

		t_child_wait_all_list_s = t_child_wait_all_list_s->next;
	}

	assert(t_child_wait_all_list_s!=NULL);

	return t_child_wait_all_list_s;

}

int API_child_wake_up_all_m_to_s(struct child_wait_all_list_element *t,int child_num,char **args)
{
	struct sub_cluster_status_list_element *tt;
	char send_msg[24+60+64*child_num];
	char *ret_msg;
	char *t_msg;
	char ip[16];
	int i;

	t_msg = itoa(t->type);
	strcpy(send_msg,t_msg);
	free(t_msg);
	strcat(send_msg,",");

	t_msg = itoa(t->job_id);
	strcat(send_msg,t_msg);
	free(t_msg);
	strcat(send_msg,",");

	t_msg = itoa(t->top_id);
	strcat(send_msg,t_msg);
	free(t_msg);
	strcat(send_msg,",");

	for(i=0;i<10;i++)
	{
		t_msg = itoa(t->id[i]);
		strcat(send_msg,t_msg);
		free(t_msg);
		strcat(send_msg,"_");
	}
	strcat(send_msg,",");

	for(i=0;i<child_num;i++)
	{
		strcat(send_msg,args[i]);
		strcat(send_msg,"_");
	}

	tt = get_sub_cluster_element_without_lock(t->sub_cluster_id);
//	master_get_machine_ip(tt->sub_master_id,ip);

	send_recv_msg(tt->sub_master_id,1,CHILD_WAKE_UP_ALL,send_msg,&ret_msg);

	free(ret_msg);

	return 1;
}

int API_child_wait_all_s_to_m(char *arg)
{
	char *ret_msg;

	send_recv_msg(0,0,CHILD_WAIT_ALL,arg,&ret_msg);
	free(ret_msg);

	return 1;
}

int API_child_wait_all_c_to_s(char *arg)
{
	char *ret_msg;

	send_recv_msg(sub_master_comm_id,1,CHILD_WAIT_ALL,arg,&ret_msg);
	free(ret_msg);

	return 1;
}

int API_child_create_s_to_m(char *arg)
{
	char *ret_msg;

	send_recv_msg(0,0,CHILD_CREATE,arg,&ret_msg);
	free(ret_msg);

	return 1;
}

int API_child_create_c_to_s(char *arg)
{
	char *ret_msg;

	send_recv_msg(sub_master_comm_id,1,CHILD_CREATE,arg,&ret_msg);
	free(ret_msg);

	return 1;
}

void API_schedule_unit_finish(int type,int schedule_unit_num,int job_id,int top_id,char *arg,int **ids,char **args)
{
	char send_msg[18+schedule_unit_num*(60+64)];
	char *ret_msg;
	char *t_msg;
	int i,j;

	t_msg = itoa(type);
	strcpy(send_msg,t_msg);
	free(t_msg);
	strcat(send_msg,",");


	t_msg = itoa(schedule_unit_num);
	strcat(send_msg,t_msg);
	free(t_msg);
	strcat(send_msg,",");

	t_msg = itoa(job_id);
	strcat(send_msg,t_msg);
	free(t_msg);
	strcat(send_msg,",");

	if(type==0)
	{
		t_msg = itoa(top_id);
		strcat(send_msg,t_msg);
		free(t_msg);
		strcat(send_msg,",");

		strcat(send_msg,arg);
	}
	else
	{
		for(i=0;i<schedule_unit_num;i++)
		{
			for(j=0;j<10;j++)
			{
				t_msg = itoa(ids[i][j]);
				strcat(send_msg,t_msg);
				free(t_msg);
				strcat(send_msg,"_");
			}
		}
		strcat(send_msg,",");
		for(i=0;i<schedule_unit_num;i++)
		{
			strcat(send_msg,args[i]);
			strcat(send_msg,"_");
		}
	}

	send_recv_msg(0,0,SCHEDULE_UNIT_FINISH,send_msg,&ret_msg);

	free(ret_msg);
}


int API_sub_task_finish_c_to_s(char *arg)
{
	char *send_msg;
	char *ret_msg;

	send_msg = strdup(arg);
	printf("api_sub task finish c to s sub master comm id = %d\n",sub_master_comm_id);
	send_recv_msg(sub_master_comm_id,1,SUB_TASK_FINISH,arg,&ret_msg);
	free(ret_msg);
	free(send_msg);

	return 1;
}

/**
 * 在这里发出了SUB_TASK_ASSIGN的信号
 */
int API_sub_task_assign(char *path,struct sub_task_exe_arg_element exe_arg,int best_node_id)
{
	char send_msg[136+60];
	char *ret_msg;
	char *t_msg;
	char ip[16];
	int best_comm_id;
	int i;


	strcpy(send_msg,path);
	
	strcat(send_msg,",");

	t_msg = itoa(exe_arg.type);
	strcat(send_msg,t_msg);
	free(t_msg);
	strcat(send_msg,",");

	t_msg = itoa(exe_arg.job_id);
	strcat(send_msg,t_msg);
	free(t_msg);
	strcat(send_msg,",");

	t_msg = itoa(exe_arg.top_id);
	strcat(send_msg,t_msg);
	free(t_msg);
	strcat(send_msg,",");

	for(i=0;i<10;i++)
	{
		t_msg = itoa(exe_arg.id[i]);
		strcat(send_msg,t_msg);
		free(t_msg);
		strcat(send_msg,"_");
	}

	strcat(send_msg,",");

	strcat(send_msg,exe_arg.arg);

	best_comm_id = sub_find_machine_comm_id(best_node_id);

	send_recv_msg(best_comm_id,2,SUB_TASK_ASSIGN,send_msg,&ret_msg);

	free(ret_msg);

	return 1;
}

int API_schedule_unit_assign(struct schedule_unit_description_element schedule_unit,int sub_cluster_id)
{
	struct sub_cluster_status_list_element *t;
	struct schedule_unit_priority_list_element *tt;
	char *send_msg;
	char *ret_msg;
	char *priority_modified_msg;
	char sub_master_ip[16];
	int num;
	int i,j;


	pthread_mutex_lock(&sub_cluster_list_m_lock);

	t = get_sub_cluster_element_without_lock(sub_cluster_id);

//	check_modified_priority(t,&num,&priority_modified_msg);
	check_modified_priority_without_lock(t,&num,&priority_modified_msg);


	t->schedule_unit_count++;//该集群中运行的任务数加1

	pthread_mutex_unlock(&sub_cluster_list_m_lock);

	tt = (struct schedule_unit_priority_list_element *)malloc(sizeof(struct schedule_unit_priority_list_element));

	tt->schedule_unit_type = schedule_unit.schedule_unit_type;
	tt->priority = schedule_unit.priority;
	if(schedule_unit.schedule_unit_type==0)
	{
		tt->job_id = schedule_unit.job_id;
		tt->top_id = schedule_unit.top_id;
		tt->schedule_unit_num = 1;
		for(i=0;i<10;i++)
		{
			tt->parent_id[i] = 0;
		}
	}
	else
	{
		tt->job_id = schedule_unit.job_id;
		tt->top_id = 0;
		tt->schedule_unit_num = schedule_unit.schedule_unit_num;
		for(i=0;i<10;i++)
		{
			tt->parent_id[i] = 0;
		}

		for(i=0;i<10;i++)
		{
			tt->parent_id[i] = schedule_unit.ids[0][i];
			if(schedule_unit.ids[0][i]==0)
			{
				tt->parent_id[i-1] = 0;//一个任务包，只有自己的id号是不同的，其余都相同
				break;
			}
		}
	}

	pthread_mutex_lock(&sub_cluster_list_m_lock);

	tt->next = t->schedule_unit_priority_list;
	t->schedule_unit_priority_list = tt;

	pthread_mutex_unlock(&sub_cluster_list_m_lock);

	send_msg = (char *)malloc((132+60+6+schedule_unit.schedule_unit_num*(84)+num*34)*sizeof(char));
	fill_schedule_unit_assign_msg(schedule_unit,send_msg,num,priority_modified_msg);

	send_recv_msg(t->sub_master_id,1,SCHEDULE_UNIT_ASSIGN,send_msg,&ret_msg);

	free(send_msg);
	free(ret_msg);
	free(priority_modified_msg);

	return 1;
}

void check_modified_priority(struct sub_cluster_status_list_element *list,int *num,char **ret_char)
{
	struct schedule_unit_priority_list_element *t;
	int new_priority;
	int old_len;
	char tt[50];

	(*num) = 0;

	*ret_char = (char *)malloc(1*sizeof(char));
	strcpy(*ret_char,"");

	pthread_mutex_lock(&sub_cluster_list_m_lock);

	t = list->schedule_unit_priority_list;

	while(t!=NULL)
	{
		new_priority = master_get_sub_task_priority(t->schedule_unit_type,t->job_id,t->top_id,t->parent_id);
		if(t->priority!=new_priority)
		{
			fill_modified_priority_msg(t,new_priority,tt);
			old_len = strlen(*ret_char);
			*ret_char = (char *)realloc(*ret_char,(old_len+strlen(tt)+1)*sizeof(char));
			if(strlen(tt)>34)
			{
				printf(" tt  too  long !!!!!!!!%ld\n",strlen(tt));
				log_error("tt too long !!!!!!!!\n");
				exit(1);
			}
			strcat(*ret_char,tt);
			t->priority = new_priority;
			(*num)++;
		}

		t = t->next;
	}

	pthread_mutex_unlock(&sub_cluster_list_m_lock);
}

void check_modified_priority_without_lock(struct sub_cluster_status_list_element *list,int *num,char **ret_char)
{
	struct schedule_unit_priority_list_element *t;
	int new_priority;
	int old_len;
	char tt[50];

	(*num) = 0;

	*ret_char = (char *)malloc(1*sizeof(char));
	strcpy(*ret_char,"");

	t = list->schedule_unit_priority_list;

	while(t!=NULL)
	{
		new_priority = master_get_sub_task_priority(t->schedule_unit_type,t->job_id,t->top_id,t->parent_id);
		if(t->priority!=new_priority)
		{
			fill_modified_priority_msg(t,new_priority,tt);
			old_len = strlen(*ret_char);
			*ret_char = (char *)realloc(*ret_char,(old_len+strlen(tt)+1)*sizeof(char));
			if(strlen(tt)>34)
			{
				printf(" tt  too  long !!!!!!!!%ld\n",strlen(tt));
				log_error("tt too long !!!!!!!!\n");
				exit(1);
			}
			strcat(*ret_char,tt);
			t->priority = new_priority;
			(*num)++;
		}

		t = t->next;
	}

}

void fill_modified_priority_msg(struct schedule_unit_priority_list_element *t,int new_priority,char *tt)
{
	char *parameter;
	int i;

	strcpy(tt,"");

	if(t->schedule_unit_type==0)	//type,job_id,top_id,0_0_0_0_,priority
	{
		strcat(tt,"0,");

		parameter = itoa(t->job_id);
		strcat(tt,parameter);
		free(parameter);
		strcat(tt,",");

		parameter = itoa(t->top_id);
		strcat(tt,parameter);
		free(parameter);
		strcat(tt,",");

		for(i=0;i<10;i++)
		{
			strcat(tt,"0_");
		}
		strcat(tt,",");

		parameter = itoa(new_priority);
		strcat(tt,parameter);
		free(parameter);
		strcat(tt,",");
	}
	else				//type,job_id,0,1_2_3....,priority
	{
		strcat(tt,"1,");

		parameter = itoa(t->job_id);
		strcat(tt,parameter);
		free(parameter);
		strcat(tt,",");

		parameter = itoa(0);
		strcat(tt,parameter);
		free(parameter);
		strcat(tt,",");

		for(i=0;i<10;i++)
		{
			parameter = itoa(t->parent_id[i]);
			strcat(tt,parameter);
			free(parameter);
			strcat(tt,"_");
		}
		strcat(tt,",");

		parameter = itoa(new_priority);
		strcat(tt,parameter);
		free(parameter);
		strcat(tt,",");
	}
}

/*
int API_job_submit(char *master_ip,char *job_path)
{
	char *ret_msg;

	send_recv_msg(master_ip,0,JOB_SUBMIT,job_path,&ret_msg);
	free(ret_msg);

	return 1;
}
*/

int API_registration_m(struct machine_description_element local_machine_status)
{
	char msg[44];
	char *ret_msg;
	char *t_msg;

	t_msg = itoa(local_machine_status.CPU_core_num);
	strcpy(msg,t_msg);
	free(t_msg);
	strcat(msg,",");

	t_msg = itoa(local_machine_status.GPU_core_num);
	strcat(msg,t_msg);
	free(t_msg);
	strcat(msg,",");

	t_msg = itoa(local_machine_status.IO_bus_capacity);
	strcat(msg,t_msg);
	free(t_msg);
	strcat(msg,",");

	t_msg = itoa(local_machine_status.network_capacity);
	strcat(msg,t_msg);
	free(t_msg);
	strcat(msg,",");

	t_msg = itoa(local_machine_status.memory_total);
	strcat(msg,t_msg);
	free(t_msg);
	strcat(msg,",");

	t_msg = itoa(local_machine_status.memory_swap);
	strcat(msg,t_msg);
	free(t_msg);

	send_recv_msg(0, 0, REGISTRATION_M,msg,&ret_msg);

	free(ret_msg);

	return 1;
}

int API_registration_s(int sub_master_id,struct machine_description_element local_machine_status)
{
	char msg[62];
	char *ret_msg;
	char *t_msg;
	char *parameter;

	t_msg = itoa(local_machine_status.CPU_core_num);
	strcpy(msg,t_msg);
	free(t_msg);
	strcat(msg,",");

	t_msg = itoa(local_machine_status.GPU_core_num);
	strcat(msg,t_msg);
	free(t_msg);
	strcat(msg,",");

	t_msg = itoa(local_machine_status.IO_bus_capacity);
	strcat(msg,t_msg);
	free(t_msg);
	strcat(msg,",");

	t_msg = itoa(local_machine_status.network_capacity);
	strcat(msg,t_msg);
	free(t_msg);
	strcat(msg,",");

	t_msg = itoa(local_machine_status.memory_total);
	strcat(msg,t_msg);
	free(t_msg);
	strcat(msg,",");

	t_msg = itoa(local_machine_status.memory_swap);
	strcat(msg,t_msg);
	free(t_msg);
	strcat(msg,",");

	t_msg = itoa(local_machine_status.CPU_free);
	strcat(msg,t_msg);
	free(t_msg);
	strcat(msg,",");

	t_msg = itoa(local_machine_status.memory_free);
	strcat(msg,t_msg);
	free(t_msg);
	strcat(msg,",");

	t_msg = itoa(local_machine_status.network_free);
	strcat(msg,t_msg);
	free(t_msg);

	send_recv_msg(sub_master_id,1,REGISTRATION_S,msg,&ret_msg);

	sub_machine_id = atoi(ret_msg+4);

	free(ret_msg);

	return 1;
}

void API_sub_scheduler_assign(struct sub_cluster_status_list_element *t)
{
	char *send_msg;
	char *t_arg;
	char *ret_msg;
	char *t_comm_id;
	int i;

	send_msg = (char *)malloc(12+t->sub_machine_num*16);


	t_arg = itoa(t->sub_cluster_id);
	strcpy(send_msg,t_arg);
	free(t_arg);
	strcat(send_msg,",");

	t_arg = itoa(t->sub_machine_num);
	strcat(send_msg,t_arg);
	free(t_arg);
	strcat(send_msg,",");

	for(i=0;i<t->sub_machine_num;i++)
	{
		t_comm_id = itoa(t->sub_machine_id_list[i]);
		strcat(send_msg,t_comm_id);
		strcat(send_msg,",");
		free(t_comm_id);
	}

	send_recv_msg(t->sub_master_id,2,SUB_SCHEDULER_ASSIGN,send_msg,&ret_msg);

	free(ret_msg);
	free(send_msg);
}

void API_computation_node_assign(int machine_id)
{
	char msg[16],ip[16];
	char *ret_msg;
	char *parameter;

	parameter = itoa(master_machine_array[machine_id-1].sub_master_id);
	strcpy(msg,parameter);
	free(parameter);

	send_recv_msg(machine_id,2,COMPUTATION_NODE_ASSIGN,msg,&ret_msg);

	free(ret_msg);
}

/*
void master_get_machine_ip(int machine_id,char *ip)
{
	if(machine_id<=0||machine_id>master_machine_num)
	{
		printf("master_get_machine_ip:machine id error : %d, should be (1-%d) \n",machine_id,master_machine_num);
		exit(1);
	}

	strcpy(ip,master_machine_array[machine_id-1].machine_ip);
}

void sub_get_machine_ip(int machine_id,char *ip)
{
	if(machine_id<=0||machine_id>sub_machine_num)
	{
		printf("sub_get_machine_ip:machine id error : %d, should be (1-%d) \n",machine_id,sub_machine_num);
		exit(1);
	}

	strcpy(ip,sub_machine_array[machine_id-1].machine_ip);
}
*/

char *itoa(int num)
{
	char *num_c;
	char t[6];
	int index;
	int i;

	if(num>65536||num<0)
	{
		printf("itoa:num invalid %d,(0-65535)\n",num);
		log_error("itoa:num invalid num\n");
		exit(1);
	}

	num_c = (char *)malloc(6*sizeof(char));

	if(num==0)
	{
		num_c[0] = '0';
		num_c[1] = '\0';
		return num_c;
	}

	index = 0;

	while(num>0)
	{
		t[index] = num%10 + '0';
		num=num/10;
		index++;
	}

	num_c[index] = '\0';

	for(i=0;i<index;i++)
	{
		num_c[i] = t[index-i-1];
	}

	return num_c;
}

char *ltoa(int num)
{
	char *num_c;
	char t[8];
	int index;
	int i;

	if(num>9999999||num<0)
	{
		printf("ltoa:num invalid %d,(0-9999999)\n",num);
		log_error("ltoa:num invalid\n");
		exit(1);
	}

	num_c = (char *)malloc(8*sizeof(char));

	if(num==0)
	{
		num_c[0] = '0';
		num_c[1] = '\0';
		return num_c;
	}

	index = 0;

	while(num>0)
	{
		t[index] = num%10 + '0';
		num=num/10;
		index++;
	}

	num_c[index] = '\0';

	for(i=0;i<index;i++)
	{
		num_c[i] = t[index-i-1];
	}

	return num_c;
}
void fill_schedule_unit_assign_msg(struct schedule_unit_description_element schedule_unit,char *send_msg,int num,char *priority_modified_msg)
{
	char *t_msg;
	char *parameter;
	int i,j;


	if(schedule_unit.schedule_unit_type==0)
	{
		strcpy(send_msg,"0,");
	}
	else if(schedule_unit.schedule_unit_type==1)
	{
		strcpy(send_msg,"1,");
	}
	else
	{
		printf("fill_schedule_unit_assign:schedule_unit_type error:%d (0,1)\n",schedule_unit.schedule_unit_type);
		log_error("fill_schedule_unit_assign:schedule_unit_type error\n");
		exit(1);
	}

	strcat(send_msg,schedule_unit.prime_sub_task_description.sub_task_path);
	strcat(send_msg,",");

	t_msg = itoa(schedule_unit.prime_sub_task_description.CPU_prefer);
	strcat(send_msg,t_msg);
	free(t_msg);
	strcat(send_msg,",");

	t_msg = itoa(schedule_unit.prime_sub_task_description.GPU_prefer);
	strcat(send_msg,t_msg);
	free(t_msg);
	strcat(send_msg,",");

	t_msg = itoa(schedule_unit.prime_sub_task_description.exe_time);
	strcat(send_msg,t_msg);
	free(t_msg);
	strcat(send_msg,",");

	t_msg = itoa(schedule_unit.prime_sub_task_description.exe_density);
	strcat(send_msg,t_msg);
	free(t_msg);
	strcat(send_msg,",");

	t_msg = itoa(schedule_unit.prime_sub_task_description.memory_demand);
	strcat(send_msg,t_msg);
	free(t_msg);
	strcat(send_msg,",");

	t_msg = itoa(schedule_unit.prime_sub_task_description.network_density);
	strcat(send_msg,t_msg);
	free(t_msg);
	strcat(send_msg,",");

	t_msg = itoa(schedule_unit.prime_sub_task_description.weight[0]);
	strcat(send_msg,t_msg);
	free(t_msg);
	strcat(send_msg,",");

	t_msg = itoa(schedule_unit.prime_sub_task_description.weight[1]);
	strcat(send_msg,t_msg);
	free(t_msg);
	strcat(send_msg,",");

	t_msg = itoa(schedule_unit.prime_sub_task_description.weight[2]);
	strcat(send_msg,t_msg);
	free(t_msg);
	strcat(send_msg,",");

	t_msg = itoa(schedule_unit.prime_sub_task_description.arg_type);
	strcat(send_msg,t_msg);
	free(t_msg);
	strcat(send_msg,",");

	strcat(send_msg,schedule_unit.prime_sub_task_description.arg);
	strcat(send_msg,",");

	t_msg = itoa(schedule_unit.schedule_unit_num);
	strcat(send_msg,t_msg);
	free(t_msg);
	strcat(send_msg,",");

	t_msg = itoa(schedule_unit.job_id);
	strcat(send_msg,t_msg);
	free(t_msg);
	strcat(send_msg,",");


	t_msg = itoa(schedule_unit.top_id);
	strcat(send_msg,t_msg);
	free(t_msg);
	strcat(send_msg,",");


	t_msg = itoa(schedule_unit.priority);
	strcat(send_msg,t_msg);
	free(t_msg);
	strcat(send_msg,",");

	if(schedule_unit.schedule_unit_type==1)
	{
		for(i=0;i<schedule_unit.schedule_unit_num;i++)
		{
			for(j=0;j<10;j++)
			{
				t_msg = itoa(schedule_unit.ids[i][j]);
				strcat(send_msg,t_msg);
				free(t_msg);
				strcat(send_msg,"_");
			}
//			strcat(send_msg,"|");
		}
	}
	else
	{
//		strcat(send_msg,"NULL_");
	}

	if(schedule_unit.schedule_unit_type==1)
	{
		for(i=0;i<schedule_unit.schedule_unit_num;i++)
		{
			strcat(send_msg,schedule_unit.args[i]);
			strcat(send_msg,"_");
		}
	}
	else
	{
//		strcat(send_msg,"NULL");
	}

	parameter = itoa(num);
	strcat(send_msg,parameter);
	free(parameter);
	strcat(send_msg,",");

	strcat(send_msg,priority_modified_msg);
}

int master_get_sub_task_priority(int type,int job_id,int top_id,int *parent_id)
{
	struct job_description_element *t_running_job_list,*t_finished_job_list;
	int ret;


	pthread_mutex_lock(&running_job_list_m_lock);

	t_running_job_list = running_job_list;
	while(t_running_job_list!=NULL)
	{
		if(t_running_job_list->job_id==job_id)
		{
			break;
		}

		t_running_job_list = t_running_job_list->next;
	}

	if(t_running_job_list==NULL)
	{
		printf("want to find %d\n",job_id);
		t_running_job_list = running_job_list;
		while(t_running_job_list!=NULL)
		{
			printf(" running job list : %ld\n",t_running_job_list->job_id);

			t_running_job_list = t_running_job_list->next;
		}

		t_finished_job_list = finished_job_list;
		while(t_finished_job_list!=NULL)
		{
			printf("finished list : %ld\n",t_finished_job_list->job_id);

			t_finished_job_list = t_finished_job_list->next;
		}

	}

	assert(t_running_job_list!=NULL);

	pthread_mutex_unlock(&running_job_list_m_lock);

	if(type==0)
	{
		ret = t_running_job_list->job.normal_sub_task_description_array[top_id-1].priority;
	}
	else
	{
		ret = t_running_job_list->job.normal_sub_task_description_array[parent_id[0]-1].priority;
	}

	assert(ret!=0);

	return ret;
}

int master_get_sub_task_priority_without_lock(int type,int job_id,int top_id,int *parent_id)
{
	struct job_description_element *t_running_job_list;
	int ret;

	t_running_job_list = running_job_list;
	while(t_running_job_list!=NULL)
	{
		if(t_running_job_list->job_id==job_id)
		{
			break;
		}

		t_running_job_list = t_running_job_list->next;
	}

	assert(t_running_job_list!=NULL);

	if(type==0)
	{
		ret = t_running_job_list->job.normal_sub_task_description_array[top_id-1].priority;
	}
	else
	{
		ret = t_running_job_list->job.normal_sub_task_description_array[parent_id[0]-1].priority;
	}

	assert(ret!=0);

	return ret;
}

int master_find_machine_id(int comm_source)
{
	return comm_source;
/*	
	struct sockaddr_in *client_addr_in;
	char *ip;
	int i;

	client_addr_in = (struct sockaddr_in *)&client_addr;

	ip = inet_ntoa(client_addr_in->sin_addr);
	for(i=0;i<master_machine_num;i++)
	{
		if(!(strcmp(master_machine_array[i].machine_ip,ip)))
		{
			return i+1;
		}
	}

	printf("cannot find master machine id!  ip = %s!\n",ip);
	exit(1);

	return -1;
*/
}

int sub_find_machine_comm_id(int sub_machine_id)
{
	return sub_machine_array[sub_machine_id-1].comm_id;
}

int sub_find_machine_id(int comm_id)
{
	int i;

	for(i=0;i<sub_machine_num;i++)
	{
		if(sub_machine_array[i].comm_id==comm_id)
		{
			return i+1;
		}
	}

	return 0;
}

long int get_msg_type(int type,int job_id,int top_id,int id[10])
{
	long int sum;
	int i;

	if(type==0)
	{
		sum = job_id+top_id;
	}
	else
	{
		sum = job_id;
		for(i=0;i<10;i++)
		{
			sum+=id[i];
		}
	}

	return sum;
}

struct sub_cluster_status_list_element *get_sub_cluster_element(int sub_cluster_id)
{
	struct sub_cluster_status_list_element *t_sub_cluster_list,*ret;

	ret = NULL;

	pthread_mutex_lock(&sub_cluster_list_m_lock);

	t_sub_cluster_list = sub_cluster_list;
	while(t_sub_cluster_list!=NULL)
	{
		if(t_sub_cluster_list->sub_cluster_id==sub_cluster_id)
		{
			ret = t_sub_cluster_list;
			break;
		}

		t_sub_cluster_list = t_sub_cluster_list->next;
	}

	pthread_mutex_unlock(&sub_cluster_list_m_lock);

	if(ret==NULL)
	{
		log_error("fun:get_sub_cluster_element NULL error\n");
		exit(1);
	}
	assert(ret!=NULL);

	return ret;
}
struct sub_cluster_status_list_element *get_sub_cluster_element_without_lock(int sub_cluster_id)
{
	struct sub_cluster_status_list_element *t_sub_cluster_list,*ret;

	ret = NULL;

	t_sub_cluster_list = sub_cluster_list;
	while(t_sub_cluster_list!=NULL)
	{
		if(t_sub_cluster_list->sub_cluster_id==sub_cluster_id)
		{
			ret = t_sub_cluster_list;
			break;
		}

		t_sub_cluster_list = t_sub_cluster_list->next;
	}

	if(ret==NULL)
	{
		log_error("get_sub_cluster_element_without_lock NULL error\n");
		exit(1);
	}
	assert(ret!=NULL);

	return ret;
}

struct sub_cluster_status_list_element *get_sub_cluster_element_through_sub_master_id(int sub_master_id)
{
	struct sub_cluster_status_list_element *t_sub_cluster_list,*ret;

	ret = NULL;
	pthread_mutex_lock(&sub_cluster_list_m_lock);

	t_sub_cluster_list = sub_cluster_list;
	while(t_sub_cluster_list!=NULL)
	{
		if(t_sub_cluster_list->sub_master_id==sub_master_id)
		{
			ret = t_sub_cluster_list;
			break;
		}

		t_sub_cluster_list = t_sub_cluster_list->next;
	}
	pthread_mutex_unlock(&sub_cluster_list_m_lock);
	
//	assert(ret!=NULL);	cannot assert here since may a sub cluster heart beat come after master destory a sub cluster's data structure

	return ret;
}
