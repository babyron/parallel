#include "stdio.h"
#include "unistd.h"
#include "pthread.h"
#include "sys/socket.h"
#include "arpa/inet.h"
#include "strings.h"
#include "stdlib.h"
#include "string.h"
#include "../data.h"
#include "data_computation.h"
#include "../common/api.h"
#include "assert.h"

msg_t msg_type_sub_scheduler(char *msg);
void schedule_unit_assign_handler(int comm_source,int ack_tag,char *arg);
void id_to_array(int *id,char **save_ptr);
void read_prime_sub_task_description(struct prime_sub_task_description_element *prime_sub_task_description,char **save_ptr);
void registration_s_handler(int comm_source,int ack_tag,char *arg);
void add_to_schedule_unit_status_list(const char *arg);
void send_schedule_unit_finish_msg(int type,int schedule_unit_num,int job_id,int top_id,char *arg,int **ids);
void delete_element_from_schedule_list(int type,int job_id,int top_id,int id[10]);
int **delete_schedule_unit_status_element_get_ids(int type,int job_id,int top_id,int id[10],int *schedule_unit_num,int *success,char ***ret_args);
void delete_from_schedule_unit_status_list(struct schedule_unit_status_list_element *t);
void sub_task_finish_handler(int comm_source,int ack_tag,char *arg);
void child_create_c_to_s_handler(int comm_source,int ack_tag,char *arg);
void child_wait_all_c_to_s_handler(int comm_source,int ack_tag,char *arg);
void add_ret_arg_to_schedule_unit_status_list(int job_id,int id[10],char *ret_arg);
void child_wake_up_all_m_to_s_handler(int comm_source,int ack_tag,char *arg);
void get_sub_task_ip_handler(int comm_source,int ack_tag,char *arg);
void sub_cluster_destroy_handler(int comm_source,int ack_tag,char *arg);
void machine_heart_beat_s_handler(int comm_source,int ack_tag,char *arg);
void running_schedule_list_modify_priority(char **save_ptr);
void schedule_unit_status_list_modify_priority(char **save_ptr);

void *sub_scheduler_server_handler(void *arg)
{

	struct server_arg_element *server_arg;
	int comm_source,comm_tag,length,ack_tag;
	char *parameter;
	char *save_ptr;
	char *final;

	int i;

	MPI_Status status;

	server_arg = (struct server_arg_element *)arg;

	parameter = strtok_r(server_arg->msg,";",&save_ptr);	//comm_tag;length;ack_tag
	comm_tag = atoi(parameter);

	parameter = strtok_r(NULL,";",&save_ptr);
	length = atoi(parameter);

	parameter = strtok_r(NULL,";",&save_ptr);
	ack_tag = atoi(parameter);

	comm_source = server_arg->status.MPI_SOURCE;

	final = (char *)malloc(length*sizeof(char));

	if(comm_source<0)
	{
		printf("source < 0  danger!\n");
		log_error("source < 0 danger!\n");
		exit(1);
	}

	MPI_Recv(final,length,MPI_CHAR,comm_source,comm_tag,MPI_COMM_WORLD,&status);

	printf("sub m recv is %s!\n",final);

	switch(msg_type_sub_scheduler(final))
	{
		case SCHEDULE_UNIT_ASSIGN:
			schedule_unit_assign_handler(comm_source,ack_tag,final);
			printf("msg : schedule unit assign\n");
			break;
		case MACHINE_HEART_BEAT:
			machine_heart_beat_s_handler(comm_source,ack_tag,final);
			printf("msg : machine heart beat_s\n");
			break;
		case REGISTRATION_S://在这里处理注册的计算节点
			registration_s_handler(comm_source,ack_tag,final);
			printf("msg : registration s\n");
			break;
		case SUB_TASK_FINISH:
			sub_task_finish_handler(comm_source,ack_tag,final);
			printf("msg : sub task finish\n");
			break;
		case CHILD_CREATE:
			child_create_c_to_s_handler(comm_source,ack_tag,final);
			printf("msg : child create c_to_s\n");
			break;
		case CHILD_WAIT_ALL:
			child_wait_all_c_to_s_handler(comm_source,ack_tag,final);
			printf("msg : child wait all c_to_s\n");
			break;
		case CHILD_WAKE_UP_ALL:
			child_wake_up_all_m_to_s_handler(comm_source,ack_tag,final);
			printf("msg : child wake up all m_to_s\n");
			break;
/*
		case GET_SUB_TASK_IP:
			printf("msg : get sub task ip c_to_s||m_to_s\n");
			get_sub_task_ip_handler(comm_source,ack_tag,final);
			break;
*/
		case SUB_CLUSTER_DESTROY:
			sub_cluster_destroy_handler(comm_source,ack_tag,final);
			printf("msg : sub cluster destroy\n");
			break;
		default:
			printf("unknown msg\n");
			log_error("unknown msg\n");
			exit(1);
	}

	free(final);
	free(server_arg->msg);
	free(server_arg);

	return NULL;
}

void machine_heart_beat_s_handler(int comm_source,int ack_tag,char *arg)
{
	int CPU_free;
	int GPU_load;
	int memory_free;
	int network_free;
	char *t_arg;
	char *parameter;
	char *save_ptr;
	int machine_id;
	int i;

	i=0;

	while(arg[i]!=';')
	{
		i++;
	}

	i++;

	t_arg = strdup(arg+i);

	send_ack_msg(comm_source,ack_tag,"");

	parameter = strtok_r(t_arg,",",&save_ptr);
	CPU_free = atoi(parameter);

	parameter = strtok_r(NULL,",",&save_ptr);
	GPU_load = atoi(parameter);

	parameter = strtok_r(NULL,",",&save_ptr);
	memory_free = atoi(parameter);

	parameter = strtok_r(NULL,",",&save_ptr);
	network_free = atoi(parameter);

	machine_id = sub_find_machine_id(comm_source);
	//sometimes the machine_id can be 0 since sub_cluster has been canceled.

	if(machine_id!=0)
	{
		sub_machine_array[machine_id-1].machine_description.CPU_free = CPU_free;
		sub_machine_array[machine_id-1].machine_description.GPU_load = GPU_load;
		sub_machine_array[machine_id-1].machine_description.memory_free = memory_free;
		sub_machine_array[machine_id-1].machine_description.network_free = network_free;
	}


	free(t_arg);
}

void sub_cluster_destroy_handler(int comm_source,int ack_tag,char *arg)
{
	int ret;
	int i;

	sub_scheduler_on = 0;

	pthread_mutex_lock(&local_machine_role_m_lock);

	local_machine_role = FREE_MACHINE;

	pthread_mutex_unlock(&local_machine_role_m_lock);

//	pthread_cancel(sub_scheduler_tid);
//	pthread_cancel(sub_cluster_heart_beat_daemon_tid);

	send_ack_msg(comm_source,ack_tag,"");

	printf("before API back to main %d\n",sub_machine_num);
	for(i=0;i<sub_machine_num;i++)
	{
		API_back_to_main_master(sub_machine_array[i].comm_id);
	}

	printf("after API back to main\n");
	free(sub_machine_array);

}

/*
void get_sub_task_ip_handler(int comm_source,int ack_tag,char *arg)
{
	char *t_arg;
	char *tt_arg;
	char *reply_msg;
	char *save_ptr;
	char *parameter;
	char ret_ip[16];
	int type;
	int job_id;
	int top_id;
	int id[10];
	int i;

	i = 0;
	while(arg[i]!=';')
	{
		i++;
	}
	i++;

	t_arg = strdup(arg+i);


	parameter = strtok_r(t_arg,",",&save_ptr);
	type = atoi(parameter);

	parameter = strtok_r(NULL,",",&save_ptr);
	job_id = atoi(parameter);

	parameter = strtok_r(NULL,",",&save_ptr);
	top_id = atoi(parameter);

	for(i=0;i<10;i++)
	{
		parameter = strtok_r(NULL,"_",&save_ptr);
		id[i] = atoi(parameter);
	}

	machine_id = is_in_schedule_list(type,job_id,top_id,id);

	if(machine_id!=0)
	{
		sub_get_machine_ip(machine_id,ret_ip);
		reply_msg = strdup(ret_ip);
	}
	else
	{
		tt_arg = strdup(arg);
		API_get_sub_task_ip_s_to_m(tt_arg,ret_ip);
		reply_msg = strdup(ret_ip);
		free(tt_arg);
	}

	send_ack_msg(comm_source,ack_tag,reply_msg);

	free(reply_msg);
	free(t_arg);

}
*/

int is_in_schedule_list(int type,int job_id,int top_id,int id[10])
{
	struct schedule_list_element *t_schedule_list;
	int i;

	pthread_mutex_lock(&schedule_list_m_lock);

	t_schedule_list = schedule_list;
	while(t_schedule_list!=NULL)
	{
		if((type==t_schedule_list->type)&&(job_id==t_schedule_list->job_id))
		{
			if(type==0)
			{
				if(top_id==t_schedule_list->top_id)
				{
					break;
				}
			}
			else
			{
				for(i=0;i<10;i++)
				{
					if(id[i]!=t_schedule_list->id[i])
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

		t_schedule_list = t_schedule_list->next;
	}

	pthread_mutex_unlock(&schedule_list_m_lock);

	if(t_schedule_list!=NULL)
	{
		return t_schedule_list->exe_machine_id;
	}
	else
	{
		return 0;
	}
}

void child_wake_up_all_m_to_s_handler(int comm_source,int ack_tag,char *arg)
{
	char *ret_arg;
	char *t_arg;
	char *parameter;
	char *save_ptr;
	int type;
	int job_id;
	int top_id;
	int id[10];
	int i;

	i=0;
	while(arg[i]!=';')
	{
		i++;
	}
	i++;

	t_arg = strdup(arg+i);

	send_ack_msg(comm_source,ack_tag,"");

	parameter = strtok_r(t_arg,",",&save_ptr);
	type = atoi(parameter);

	parameter = strtok_r(NULL,",",&save_ptr);
	job_id = atoi(parameter);

	parameter = strtok_r(NULL,",",&save_ptr);
	top_id = atoi(parameter);

	for(i=0;i<10;i++)
	{
		parameter = strtok_r(NULL,"_",&save_ptr);
		id[i] = atoi(parameter);
	}

	ret_arg = strtok_r(NULL,",",&save_ptr);

	API_child_wake_up_all_s_to_c(type,job_id,top_id,id,ret_arg);

	free(t_arg);

}

void child_wait_all_c_to_s_handler(int comm_source,int ack_tag,char *arg)
{
	struct child_wait_all_list_element *t;
	char *t_arg,*tt_arg;
	char *parameter;
	char sub_cluster_id_c[6];
	char *save_ptr;
	int len;
	int i;

	i = 0;
	while(arg[i]!=';')
	{
		i++;
	}
	i++;

	t_arg = strdup(arg+i);
	tt_arg = strdup(arg+i);

	send_ack_msg(comm_source,ack_tag,"");

	t = (struct child_wait_all_list_element *)malloc(sizeof(struct child_wait_all_list_element));
	parameter = strtok_r(t_arg,",",&save_ptr);
	t->type = atoi(parameter);

	parameter = strtok_r(NULL,",",&save_ptr);
	t->job_id = atoi(parameter);

	parameter = strtok_r(NULL,",",&save_ptr);
	t->top_id = atoi(parameter);

	for(i=0;i<10;i++)
	{
		parameter = strtok_r(NULL,"_",&save_ptr);
		t->id[i] = atoi(parameter);
	}

	parameter = strtok_r(NULL,",",&save_ptr);
	t->machine_id = atoi(parameter);


	pthread_mutex_lock(&child_wait_all_list_s_m_lock);

	t->next = child_wait_all_list_s;
	child_wait_all_list_s = t;

	pthread_mutex_unlock(&child_wait_all_list_s_m_lock);

	len = strlen(tt_arg);
	i = len-1;

	while(tt_arg[i]!=',')
	{
		i--;
	}
	tt_arg[i] = '\0';

	strcat(tt_arg,",");
	itoa(sub_cluster_id_c, sub_cluster_id);
	strcat(tt_arg,sub_cluster_id_c);

	API_child_wait_all_s_to_m(tt_arg);

	free(t_arg);
	free(tt_arg);
}

void child_create_c_to_s_handler(int comm_source,int ack_tag,char *arg)
{
	char *t_arg;
	int i;

	i = 0;
	while(arg[i]!=';')
	{
		i++;
	}
	i++;

	t_arg = strdup(arg+i);

	send_ack_msg(comm_source,ack_tag,"");

	API_child_create_s_to_m(t_arg);

	free(t_arg);
}

void sub_task_finish_handler(int comm_source,int ack_tag,char *arg)
{
//	unit complelte
//	send sub unit finish if needed;
	int type;
	int job_id;
	int top_id;
	int id[10];
	int **ids;
	int schedule_unit_num;
	char *ret_arg;
	char **ret_args;
	char *t_arg;
	char *parameter;
	char *t_id;
	char *save_ptr;
	int success;
	int i;

	i = 0;
	while(arg[i]!=';')
	{
		i++;
	}
	i++;

	t_arg = strdup(arg+i);

	send_ack_msg(comm_source,ack_tag,"");

	parameter = strtok_r(t_arg,",",&save_ptr);
	type = atoi(parameter);

	parameter = strtok_r(NULL,",",&save_ptr);
	job_id = atoi(parameter);

	parameter = strtok_r(NULL,",",&save_ptr);
	top_id = atoi(parameter);

	for(i=0;i<10;i++)
	{
		parameter = strtok_r(NULL,"_",&save_ptr);
		id[i] = atoi(parameter);
	}

	parameter = strtok_r(NULL,",",&save_ptr);
	ret_arg = strdup(parameter);

	free(t_arg);

	delete_element_from_schedule_list(type,job_id,top_id,id);

	if(type==1)
	{
		add_ret_arg_to_schedule_unit_status_list(job_id,id,ret_arg);

	}

	ids = delete_schedule_unit_status_element_get_ids(type,job_id,top_id,id,&schedule_unit_num,&success,&ret_args);		//should get args also

	if(success==1)
	{
		API_schedule_unit_finish(type,schedule_unit_num,job_id,top_id,ret_arg,ids,ret_args);
		if(type==1)
		{
			for(i=0;i<schedule_unit_num;i++)
			{
				free(ret_args[i]);
				free(ids[i]);
			}
			free(ret_args);
			free(ids);
		}
	}

	free(ret_arg);
}

void add_ret_arg_to_schedule_unit_status_list(int job_id,int id[10],char *ret_arg)
{
	struct schedule_unit_status_list_element *t_schedule_unit_status_list;
	int i,j;

	pthread_mutex_lock(&schedule_unit_status_list_m_lock);

	t_schedule_unit_status_list = schedule_unit_status_list;
	while(t_schedule_unit_status_list!=NULL)
	{
		if((t_schedule_unit_status_list->job_id==job_id)&&(t_schedule_unit_status_list->schedule_unit_type==1))
		{
			for(i=0;i<t_schedule_unit_status_list->schedule_unit_num;i++)
			{
				for(j=0;j<10;j++)
				{
					if(t_schedule_unit_status_list->ids[i][j]!=id[j])
					{
						break;
					}
				}
				if(j==10)
				{
					break;
				}
			}

			if(i<t_schedule_unit_status_list->schedule_unit_num)
			{
				t_schedule_unit_status_list->ret_args[i] = strdup(ret_arg);
			}
		}

		t_schedule_unit_status_list = t_schedule_unit_status_list->next;
	}

	pthread_mutex_unlock(&schedule_unit_status_list_m_lock);
}

void registration_s_handler(int comm_source,int ack_tag,char *arg)
{
	char *parameter;
	char *t_arg;
	char *machine_id_c;
	char *save_ptr;
	int machine_id;
	int i;

	i = 0;

	while(arg[i]!=';')
	{
		i++;
	}

	i++;


	//logic need to be modified
	//
	//
	//
	//
	//
	//
	//
	//
	t_arg = strdup(arg+i);

	printf("registration s  = %s!\n",t_arg);

	machine_id = sub_find_machine_id(comm_source);
	itoa(machine_id_c, machine_id);

	send_ack_msg(comm_source,ack_tag, machine_id_c);


	parameter = strtok_r(t_arg,",",&save_ptr);
	sub_machine_array[machine_id-1].machine_description.CPU_core_num = atoi(parameter);

	parameter = strtok_r(NULL,",",&save_ptr);
	sub_machine_array[machine_id-1].machine_description.GPU_core_num = atoi(parameter);

	parameter = strtok_r(NULL,",",&save_ptr);
	sub_machine_array[machine_id-1].machine_description.IO_bus_capacity = atoi(parameter);

	parameter = strtok_r(NULL,",",&save_ptr);
	sub_machine_array[machine_id-1].machine_description.network_capacity = atoi(parameter);

	parameter = strtok_r(NULL,",",&save_ptr);
	sub_machine_array[machine_id-1].machine_description.memory_total = atoi(parameter);

	parameter = strtok_r(NULL,",",&save_ptr);
	sub_machine_array[machine_id-1].machine_description.memory_swap = atoi(parameter);

	parameter = strtok_r(NULL,",",&save_ptr);
	sub_machine_array[machine_id-1].machine_description.CPU_free = atoi(parameter);

	if(sub_machine_array[machine_id-1].machine_description.CPU_free==0)
	{
		printf("CPU==0 !!\n");
		log_error("CPU==0 !!\n");
		exit(1);
	}

	parameter = strtok_r(NULL,",",&save_ptr);
	sub_machine_array[machine_id-1].machine_description.memory_free = atoi(parameter);

	parameter = strtok_r(NULL,",",&save_ptr);
	sub_machine_array[machine_id-1].machine_description.network_free = atoi(parameter);

	free(t_arg);

	sub_machine_array[machine_id-1].machine_status = 1;

	for(i=0;i<sub_machine_num;i++)
	{
		if(sub_machine_array[i].machine_status==0)
		{
			break;
		}
	}

	if(i==sub_machine_num)
	{
		printf("all online~~~\n");
	}

}

void schedule_unit_assign_handler(int comm_source,int ack_tag,char *arg)
{
	struct waiting_schedule_list_element *t_head,*t,*t_waiting_schedule_list;
	struct prime_sub_task_description_element prime_sub_task_description;
	char *t_arg,*tt_arg;
	char *parameter;
	char *save_ptr;
	int sub_unit_num;
	int type;
	int job_id;
	int top_id;
	int priority;
	int i;

	i = 0;

	while(arg[i]!=';')
	{
		i++;
	}

	i++;

	t_arg = strdup(arg+i);
	tt_arg = strdup(arg+i);

	send_ack_msg(comm_source,ack_tag,"");


	add_to_schedule_unit_status_list(t_arg);


//	t = (struct waiting_schedule_list *)malloc(sizeof(struct waiting_schedule_list));
//	t->next = NULL;

	parameter = strtok_r(tt_arg,",",&save_ptr);

	type = atoi(parameter);

	read_prime_sub_task_description(&prime_sub_task_description,&save_ptr);

	parameter = strtok_r(NULL,",",&save_ptr);
	sub_unit_num = atoi(parameter);

	parameter = strtok_r(NULL,",",&save_ptr);
	job_id = atoi(parameter);

	parameter = strtok_r(NULL,",",&save_ptr);
	top_id = atoi(parameter);

	parameter = strtok_r(NULL,",",&save_ptr);
	priority = atoi(parameter);

	if(type==1)
	{
		t_head = NULL;
		for(i=0;i<sub_unit_num;i++)
		{
			t = (struct waiting_schedule_list_element *)malloc(sizeof(struct waiting_schedule_list_element));
			t->next = t_head;
			t_head = t;
		}

		t = t_head;

		while(t!=NULL)
		{
			t->type = type;
			t->prime_sub_task_description = prime_sub_task_description;
			t->job_id = job_id;
			t->top_id = top_id;
			t->priority = priority;
			id_to_array(t->id,&save_ptr);
			t = t->next;
		}

		t = t_head;

		while(t!=NULL)
		{
			parameter = strtok_r(NULL,"_",&save_ptr);
			strcpy(t->prime_sub_task_description.arg,parameter);
			t = t->next;
		}
	}
	else
	{
		t_head = (struct waiting_schedule_list_element *)malloc(sizeof(struct waiting_schedule_list_element));

		t_head->type = type;
		t_head->prime_sub_task_description = prime_sub_task_description;
		t_head->job_id = job_id;
		t_head->top_id = top_id;
		t_head->priority = priority;
		for(i=0;i<10;i++)
		{
			t_head->id[i] = 0;
		}
		t_head->next = NULL;
	}

//	running_schedule_list_modify_priority(&save_ptr);

	pthread_mutex_lock(&waiting_schedule_list_m_lock);

	if(waiting_schedule_list==NULL)
	{
		waiting_schedule_list = t_head;
	}
	else
	{
		t_waiting_schedule_list = waiting_schedule_list;

		while(t_waiting_schedule_list->next!=NULL)
		{
			t_waiting_schedule_list = t_waiting_schedule_list->next;
		}

		t_waiting_schedule_list->next = t_head;
	}

	pthread_mutex_unlock(&waiting_schedule_list_m_lock);

	free(t_arg);
	free(tt_arg);
}

void running_schedule_list_modify_priority(char **save_ptr)
{
	struct schedule_list_element *t_schedule_list;
	struct waiting_schedule_list_element *t_waiting_schedule_list;
	int num;
	int type;
	int job_id;
	int top_id;
	int parent_id[10];
	int priority;
	int schedule_list_parent_id[10];
	int waiting_list_parent_id[10];
	int waiting_array_parent_id[10];
	int find_flag;
	char *parameter;
	int i,j,k;

	parameter = strtok_r(NULL,",",save_ptr);
	num = atoi(parameter);

	for(i=0;i<num;i++)
	{
		parameter = strtok_r(NULL,",",save_ptr);
		type = atoi(parameter);
		parameter = strtok_r(NULL,",",save_ptr);
		job_id = atoi(parameter);
		parameter = strtok_r(NULL,",",save_ptr);
		top_id = atoi(parameter);

		for(j=0;j<10;j++)
		{
			parameter = strtok_r(NULL,"_",save_ptr);
			parent_id[j] = atoi(parameter);
		}

		parameter = strtok_r(NULL,",",save_ptr);
		priority = atoi(parameter);

		find_flag = 0;

		pthread_mutex_lock(&schedule_list_m_lock);
		pthread_mutex_lock(&waiting_schedule_list_m_lock);
		pthread_mutex_lock(&waiting_schedule_array_m_lock);

		t_schedule_list = schedule_list;
		while(t_schedule_list!=NULL)
		{
			if((t_schedule_list->type==type)&&(t_schedule_list->job_id==job_id))
			{
				if(type==0)
				{
					if(t_schedule_list->top_id==top_id)
					{
						find_flag = 1;
						t_schedule_list->priority = priority;
						break;
					}
				}
				else
				{
					for(j=0;j<10;j++)
					{
						schedule_list_parent_id[j] = 0;
					}

					for(j=0;j<10;j++)
					{
						schedule_list_parent_id[j] = t_schedule_list->id[j];
						if(t_schedule_list->id[j]==0)
						{
							schedule_list_parent_id[j-1] = 0;
							break;
						}
					}

					for(j=0;j<10;j++)
					{
						if(schedule_list_parent_id[j]!=parent_id[j])
						{
							break;
						}
					}

					if(j==10)
					{
						find_flag = 1;
						t_schedule_list->priority = priority;
					}
				}
			}

			t_schedule_list = t_schedule_list->next;
		}


		if(find_flag==0)
		{

			t_waiting_schedule_list = waiting_schedule_list;
			while(t_waiting_schedule_list!=NULL)
			{

				if((type==t_waiting_schedule_list->type)&&(job_id==t_waiting_schedule_list->job_id))
				{
					if(type==0)
					{
						if(top_id==t_waiting_schedule_list->top_id)
						{
							find_flag = 1;
							t_waiting_schedule_list->priority = priority;
							break;
						}
					}
					else
					{
						for(j=0;j<10;j++)
						{
							waiting_list_parent_id[j] = 0;
						}
						for(j=0;j<10;j++)
						{
							waiting_list_parent_id[j] = t_waiting_schedule_list->id[j];
							if(t_waiting_schedule_list->id[j]==0)
							{
								waiting_list_parent_id[j-1] = 0;
								break;
							}
						}

						for(j=0;j<10;j++)
						{
							if(waiting_list_parent_id[j]!=parent_id[j])
							{
								break;
							}
						}

						if(j==0)
						{
							find_flag = 1;	//cannot break here since there may have many sub task derived from one parent
							t_waiting_schedule_list->priority = priority;
						}
					}

				}



				t_waiting_schedule_list = t_waiting_schedule_list->next;
			}

			if(find_flag==0)
			{

				for(j=0;j<waiting_schedule_array_num;j++)
				{
					if((waiting_schedule_array[j].type==type)&&(waiting_schedule_array[j].job_id==job_id))
					{
						if(type==0)
						{
							if(waiting_schedule_array[j].top_id==top_id)
							{
								find_flag = 1;
								waiting_schedule_array[j].priority = priority;
								break;
							}
						}
						else
						{
							for(k=0;k<10;k++)
							{
								waiting_array_parent_id[k] = 0;
							}

							for(k=0;k<10;k++)
							{
								waiting_array_parent_id[k] = waiting_schedule_array[j].id[k];
								if(waiting_schedule_array[j].id[k]==0)
								{
									waiting_array_parent_id[k-1] = 0;
									break;
								}
							}

							for(k=0;k<10;k++)
							{
								if(waiting_array_parent_id[k]!=parent_id[k])
								{
									break;
								}
							}

							if(k==10)
							{
								find_flag = 1;
								waiting_schedule_array[j].priority = priority;
							}
						}
					}
				}

			}

		}

//		assert(find_flag==1);	//some times task already finished and cannot be found in sub master any more

		pthread_mutex_unlock(&waiting_schedule_array_m_lock);
		pthread_mutex_unlock(&waiting_schedule_list_m_lock);
		pthread_mutex_unlock(&schedule_list_m_lock);
	}

}

void id_to_array(int *id,char **save_ptr)
{
	char *parameter;
	int i;

	for(i=0;i<10;i++)
	{
		parameter = strtok_r(NULL,"_",save_ptr);
		id[i] = atoi(parameter);
	}
}

void read_prime_sub_task_description(struct prime_sub_task_description_element *prime_sub_task_description,char **save_ptr)
{
	char *parameter;

	parameter = strtok_r(NULL,",",save_ptr);
	strcpy(prime_sub_task_description->sub_task_path,parameter);

	parameter = strtok_r(NULL,",",save_ptr);
	prime_sub_task_description->CPU_prefer = atoi(parameter);

	parameter = strtok_r(NULL,",",save_ptr);
	prime_sub_task_description->GPU_prefer = atoi(parameter);

	parameter = strtok_r(NULL,",",save_ptr);
	prime_sub_task_description->exe_time = atoi(parameter);

	parameter = strtok_r(NULL,",",save_ptr);
	prime_sub_task_description->exe_density = atoi(parameter);

	parameter = strtok_r(NULL,",",save_ptr);
	prime_sub_task_description->memory_demand = atoi(parameter);

	parameter = strtok_r(NULL,",",save_ptr);
	prime_sub_task_description->network_density = atoi(parameter);

	parameter = strtok_r(NULL,",",save_ptr);
	prime_sub_task_description->weight[0] = atoi(parameter);

	parameter = strtok_r(NULL,",",save_ptr);
	prime_sub_task_description->weight[1] = atoi(parameter);

	parameter = strtok_r(NULL,",",save_ptr);
	prime_sub_task_description->weight[2] = atoi(parameter);

	parameter = strtok_r(NULL,",",save_ptr);
	prime_sub_task_description->arg_type = atoi(parameter);

	parameter = strtok_r(NULL,",",save_ptr);
	strcpy(prime_sub_task_description->arg,parameter);
}

msg_t msg_type_sub_scheduler(char *msg)
{
	char type[64];
	int i;

	for(i=0;i<5;i++)
	{
		type[i] = msg[i];
	}

	type[i] = '\0';

	if(strcmp(type,"TYPE:"))
	{
		printf("msg_type:start of msg is not TYPE:\n");
		log_error("msg_type:start of msg is not TYPE:\n");
		exit(1);
		return UNKNOWN;
	}

	for(i=0;i<64;i++)
	{
		if(msg[i+5]!=';')
		{
			type[i] = msg[i+5];
		}
		else
		{
			type[i] = '\0';
			break;
		}
	}

	if(!strcmp(type,"SCHEDULE_UNIT_ASSIGN"))
	{
		return SCHEDULE_UNIT_ASSIGN;
	}
	else if(!strcmp(type,"MACHINE_HEART_BEAT"))
	{
		return MACHINE_HEART_BEAT;
	}
	else if(!strcmp(type,"REGISTRATION_S"))
	{
		return REGISTRATION_S;
	}
	else if(!strcmp(type,"SUB_TASK_FINISH"))
	{
		return SUB_TASK_FINISH;
	}
	else if(!strcmp(type,"CHILD_CREATE"))
	{
		return CHILD_CREATE;
	}
	else if(!strcmp(type,"CHILD_WAIT_ALL"))
	{
		return CHILD_WAIT_ALL;
	}
	else if(!strcmp(type,"CHILD_WAKE_UP_ALL"))
	{
		return CHILD_WAKE_UP_ALL;
	}
	else if(!strcmp(type,"GET_SUB_TASK_IP"))
	{
		return GET_SUB_TASK_IP;
	}
	else if(!strcmp(type,"SUB_CLUSTER_DESTROY"))
	{
		return SUB_CLUSTER_DESTROY;
	}
	else
	{
		return UNKNOWN;
	}
}

void add_to_schedule_unit_status_list(const char *arg)
{
	struct schedule_unit_status_list_element *t_schedule_unit_status_list,*t;
	struct prime_sub_task_description_element prime_sub_task_description;
	char *t_arg;
	char *parameter;
	char *save_ptr;
	int i,j;

	t_arg = strdup(arg);

	t = (struct schedule_unit_status_list_element *)malloc(sizeof(struct schedule_unit_status_list_element));

	parameter = strtok_r(t_arg,",",&save_ptr);
	t->schedule_unit_type = atoi(parameter);

	read_prime_sub_task_description(&prime_sub_task_description,&save_ptr);

	parameter = strtok_r(NULL,",",&save_ptr);
	t->schedule_unit_num = atoi(parameter);

	parameter = strtok_r(NULL,",",&save_ptr);
	t->job_id = atoi(parameter);

	parameter = strtok_r(NULL,",",&save_ptr);
	t->top_id = atoi(parameter);

	parameter = strtok_r(NULL,",",&save_ptr);
	t->priority = atoi(parameter);

	t->status = RUNNING;
	t->ids = NULL;
	t->status_array = NULL;
	t->ret_args = NULL;

	if(t->schedule_unit_type==1)
	{
		t->ids = (int **)malloc(t->schedule_unit_num*sizeof(int *));
		for(i=0;i<t->schedule_unit_num;i++)
		{
			t->ids[i] = (int *)malloc(10*sizeof(int));
		}

		for(i=0;i<t->schedule_unit_num;i++)
		{
			for(j=0;j<10;j++)
			{
				parameter = strtok_r(NULL,"_",&save_ptr);
				t->ids[i][j] = atoi(parameter);
			}
		}

		t->status_array = (status_t *)malloc(t->schedule_unit_num*sizeof(status_t));

		t->ret_args = (char **)malloc(t->schedule_unit_num*sizeof(char *));

		for(i=0;i<t->schedule_unit_num;i++)
		{
			t->status_array[i] = RUNNING;
		}

		for(i=0;i<t->schedule_unit_num;i++)
		{
			parameter = strtok_r(NULL,"_",&save_ptr);
		}
	}

	t->next = NULL;

	schedule_unit_status_list_modify_priority(&save_ptr);

	free(t_arg);

	pthread_mutex_lock(&schedule_unit_status_list_m_lock);

	t->next = schedule_unit_status_list;
	schedule_unit_status_list = t;

	pthread_mutex_unlock(&schedule_unit_status_list_m_lock);
}

void schedule_unit_status_list_modify_priority(char **save_ptr)
{
	struct schedule_unit_status_list_element *t_schedule_unit_status_list;
	int num;
	int type;
	int job_id;
	int top_id;
	int priority;
	int parent_id[10];
	int schedule_unit_parent_id[10];
	char *parameter;
	int i,j;

	parameter = strtok_r(NULL,",",save_ptr);
	num = atoi(parameter);

	for(i=0;i<num;i++)
	{
		parameter = strtok_r(NULL,",",save_ptr);
		type = atoi(parameter);
		parameter = strtok_r(NULL,",",save_ptr);
		job_id = atoi(parameter);
		parameter = strtok_r(NULL,",",save_ptr);
		top_id = atoi(parameter);

		for(j=0;j<10;j++)
		{
			parameter = strtok_r(NULL,"_",save_ptr);
			parent_id[j] = atoi(parameter);
		}

		parameter = strtok_r(NULL,",",save_ptr);
		priority = atoi(parameter);

//		printf("want to find type:%d,job_id:%d,top_id:%d,parent_id:%d\n",type,job_id,top_id,parent_id[0]);

		pthread_mutex_lock(&schedule_unit_status_list_m_lock);

		t_schedule_unit_status_list = schedule_unit_status_list;
		while(t_schedule_unit_status_list!=NULL)
		{
			if(t_schedule_unit_status_list->schedule_unit_type==0)
			{
//				printf("myinfo type:%d,job_id:%d,top_id:%d,no_parent_id\n",0,t_schedule_unit_status_list->job_id,t_schedule_unit_status_list->top_id);
			}
			else
			{
//				printf("myinfo type:%d,job_id:%d,top_id:%d,parent_id:%d\n",1,t_schedule_unit_status_list->job_id,t_schedule_unit_status_list->top_id,t_schedule_unit_status_list->ids[0][0]);
			}
			if((t_schedule_unit_status_list->schedule_unit_type==type)&&(t_schedule_unit_status_list->job_id==job_id))
			{
				if(type==0)
				{
					if(t_schedule_unit_status_list->top_id==top_id)
					{
						t_schedule_unit_status_list->priority = priority;
						break;
					}
				}
				else
				{
					for(j=0;j<10;j++)
					{
						schedule_unit_parent_id[j] = 0;
					}

					for(j=0;j<10;j++)
					{
						schedule_unit_parent_id[j] = t_schedule_unit_status_list->ids[0][j];
						if(t_schedule_unit_status_list->ids[0][j]==0)
						{
							schedule_unit_parent_id[j-1] = 0;
							break;
						}
					}

					for(j=0;j<10;j++)
					{
						if(schedule_unit_parent_id[j]!=parent_id[j])
						{
							break;
						}
					}

					if(j==10)
					{
						t_schedule_unit_status_list->priority = priority;
						break;
					}
				}
			}

			t_schedule_unit_status_list = t_schedule_unit_status_list->next;
		}

//		assert(t_schedule_unit_status_list!=NULL);

		pthread_mutex_unlock(&schedule_unit_status_list_m_lock);
	}
}

void delete_from_schedule_unit_status_list(struct schedule_unit_status_list_element *t)
{
	struct schedule_unit_status_list_element *t_schedule_unit_status_list;
	int i;

	assert(t);

	if(t==schedule_unit_status_list)
	{
		schedule_unit_status_list = schedule_unit_status_list->next;

		if(t->schedule_unit_type==1)
		{
			for(i=0;i<t->schedule_unit_num;i++)
			{
				free(t->ids[i]);
			}
			free(t->ids);
			free(t->status_array);
		}

		free(t);
	}
	else
	{
		t_schedule_unit_status_list = schedule_unit_status_list;
		while(t_schedule_unit_status_list->next!=NULL)
		{
			if(t_schedule_unit_status_list->next==t)
			{
				t_schedule_unit_status_list->next = t->next;
	
				if(t->schedule_unit_type==1)
				{
					for(i=0;i<t->schedule_unit_num;i++)
					{
						free(t->ids[i]);
					}
					free(t->ids);
					free(t->status_array);
				}
				free(t);
				break;
			}
			t_schedule_unit_status_list = t_schedule_unit_status_list->next;
		}
		assert(t_schedule_unit_status_list!=NULL);
	}
}

void delete_element_from_schedule_list(int type,int job_id,int top_id,int id[10])
{
	struct schedule_list_element *t_schedule_list,*t;
	int i;

	pthread_mutex_lock(&schedule_list_m_lock);

	assert(schedule_list);
	assert((type==0)||(type==1));

	t_schedule_list = schedule_list;
	while(t_schedule_list!=NULL)
	{
		if((type==t_schedule_list->type)&&(job_id==t_schedule_list->job_id))
		{
			if(type==0)
			{
				if(top_id==t_schedule_list->top_id)
				{
					break;
				}
			}
			else
			{
				for(i=0;i<10;i++)
				{
					if(id[i]!=t_schedule_list->id[i])
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

		t_schedule_list = t_schedule_list->next;
	}

	t = t_schedule_list;

	assert(t);

	if(t==schedule_list)
	{
		schedule_list = schedule_list->next;
	}
	else
	{
		t_schedule_list = schedule_list;
		while(t_schedule_list->next!=NULL)
		{
			if(t_schedule_list->next==t)
			{
				t_schedule_list->next = t->next;
				break;
			}

			t_schedule_list = t_schedule_list->next;
		}
	}

	free(t);

	pthread_mutex_unlock(&schedule_list_m_lock);
}

int **delete_schedule_unit_status_element_get_ids(int type,int job_id,int top_id,int id[10],int *schedule_unit_num,int *success,char ***ret_args)
{
	struct schedule_unit_status_list_element *t_schedule_unit_status_list;
	int **ids;
	int i,j;

	assert((type==0||type==1));

	*success = 0;

	ids = NULL;

	if(type==0)
	{
		pthread_mutex_lock(&schedule_unit_status_list_m_lock);

		t_schedule_unit_status_list = schedule_unit_status_list;
		while(t_schedule_unit_status_list!=NULL)
		{
			if((t_schedule_unit_status_list->job_id==job_id)&&(t_schedule_unit_status_list->top_id==top_id)&&(t_schedule_unit_status_list->schedule_unit_type==type))
			{
				t_schedule_unit_status_list->status = FINISHED;
				break;
			}
			t_schedule_unit_status_list = t_schedule_unit_status_list->next;
		}

		delete_from_schedule_unit_status_list(t_schedule_unit_status_list);

		pthread_mutex_unlock(&schedule_unit_status_list_m_lock);

		*schedule_unit_num = 1;

		*success = 1;
	}
	else
	{
		pthread_mutex_lock(&schedule_unit_status_list_m_lock);

		t_schedule_unit_status_list = schedule_unit_status_list;

		while(t_schedule_unit_status_list!=NULL)
		{
			if((t_schedule_unit_status_list->job_id==job_id)&&(t_schedule_unit_status_list->schedule_unit_type==type))
			{
				for(i=0;i<t_schedule_unit_status_list->schedule_unit_num;i++)
				{
					for(j=0;j<10;j++)
					{
						if(id[j]!=t_schedule_unit_status_list->ids[i][j])
						{
							break;
						}
					}
					if(j==10)
					{
						break;
					}
				}

				if(i<t_schedule_unit_status_list->schedule_unit_num)
				{
					t_schedule_unit_status_list->status_array[i] = FINISHED;
//					if all finished  send msg
					for(j=0;j<t_schedule_unit_status_list->schedule_unit_num;j++)
					{
						if(t_schedule_unit_status_list->status_array[j]!=FINISHED)
						{
							break;
						}
					}

					if(j==t_schedule_unit_status_list->schedule_unit_num)
					{
//		delete from list	&&			send msg
//

						ids = (int **)malloc(t_schedule_unit_status_list->schedule_unit_num*sizeof(int *));
						for(i=0;i<t_schedule_unit_status_list->schedule_unit_num;i++)
						{
							ids[i] = (int *)malloc(10*sizeof(int));
						}

						for(i=0;i<t_schedule_unit_status_list->schedule_unit_num;i++)
						{
							for(j=0;j<10;j++)
							{
								ids[i][j] = t_schedule_unit_status_list->ids[i][j];
							}
						}



						*ret_args = t_schedule_unit_status_list->ret_args;
						*schedule_unit_num = t_schedule_unit_status_list->schedule_unit_num;
						delete_from_schedule_unit_status_list(t_schedule_unit_status_list);
						*success = 1;

						break;
					}
				}
			}

			t_schedule_unit_status_list = t_schedule_unit_status_list->next;
		}

		pthread_mutex_unlock(&schedule_unit_status_list_m_lock);
	}

	return ids;
}

