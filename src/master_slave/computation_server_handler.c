#include <stdio.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/socket.h>
#include <sys/msg.h>
#include <sys/ipc.h>
#include <sys/types.h>
#include <arpa/inet.h>
#include <strings.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <mpi.h>
#include "./structure/data.h"
#include "data_computation.h"
#include "./common/api.h"
#include "./common/communication.h"


msg_t msg_type_computation(char *msg);
void sub_scheduler_assign_handler(int comm_source,int ack_tag,char *arg);
void computation_node_assign_handler(int comm_source,int ack_tag,char *arg);
void *sub_scheduler_server(void *arg);
void sub_task_assign_handler(int comm_source,int ack_tag,char *arg);
void sub_scheduler_init();
void run_sub_task(char *t_arg);
//void o_sub_task_finish_handler(int comm_source,int ack_tag,char *arg);
//void o_child_create_handler(int comm_source,int ack_tag,char *arg);
//void o_child_wait_all_handler(int comm_source,int ack_tag,char *arg);
void child_wake_up_all_handler(int comm_source,int ack_tag,char *arg);
void add_element_to_sub_task_running_list(char *arg);
void delete_element_from_sub_task_running_list(char *arg);
//void o_get_sub_task_handler(int comm_source,int ack_tag,char *arg);
void back_to_main_master_handler(int comm_source,int ack_tag,char *arg);
void o_mpi_sub_task_finish_handler(char *arg);
void o_mpi_child_create_handler(char *arg);
void o_mpi_child_wait_all_handler(char *arg);

char queue_recv_msg[38+6+1000*64+16];		//max child_num 1000

void *computation_server_handler(void *arg)
{
	struct server_arg_element *server_arg;
	int comm_source,comm_tag,length,ack_tag;
	char *parameter;
	char *save_ptr;
	char *final;
	char ack_tag_c[10];
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
	//在解析完消息内容后接受真实的消息内容
	MPI_Recv(final,length,MPI_CHAR,comm_source,comm_tag,MPI_COMM_WORLD,&status);

	printf("com recv is %s!\n",final);

	switch(msg_type_computation(final))
	{
		case SUB_SCHEDULER_ASSIGN:
			sub_scheduler_assign_handler(comm_source,ack_tag,final);
			printf("msg : sub scheduler assign\n");
			break;
		case COMPUTATION_NODE_ASSIGN:
			computation_node_assign_handler(comm_source,ack_tag,final);//发出REGISTRATION_S信号
			printf("msg : computation node assign\n");
			break;
		case SUB_TASK_ASSIGN://这个信号由sub_scheduler_assign_handler里的调度函数发出
			sub_task_assign_handler(comm_source,ack_tag,final);//开始执行任务
			printf("msg : sub task assign\n");
			break;
		case CHILD_WAKE_UP_ALL:
			child_wake_up_all_handler(comm_source,ack_tag,final);
			printf("msg : child wake up all s_to_c\n");
			break;
/*			
		case SUB_TASK_FINISH:
			printf("msg : out sub task finish\n");
			o_sub_task_finish_handler(comm_source,ack_tag,final);
			break;
		case CHILD_CREATE:
			printf("msg : out child create\n");
			o_child_create_handler(comm_source,ack_tag,final);
			break;
		case CHILD_WAIT_ALL:
			printf("msg : out child wait all\n");
			o_child_wait_all_handler(comm_source,ack_tag,final);
			break;
		case GET_SUB_TASK_IP:
			printf("msg : out get sub task ip\n");
			o_get_sub_task_handler(comm_source,ack_tag,final);
			break;
*/
		case BACK_TO_MAIN_MASTER:
			back_to_main_master_handler(comm_source,ack_tag,final);
			printf("msg : back to main master\n");
			break;
		default:
			printf("computation:unknown msg\n");
			log_error("computation:unknown msg\n");
			exit(1);
	}

	free(final);
	free(server_arg->msg);
	free(server_arg);

	return NULL;
}

/**
 * 从消息队列中获取消息来处理本地进程的状态，这是在一个节点内的操作
 */
void *local_msg_daemon(void *arg)
{
	char *final;
	int msg_queue_id;
	int msg_type;
	int ret;

	msg_type = MPI_LOCAL_DEAMON_MSG_TYPE;

	msg_queue_id = msgget(MSG_QUEUE_KEY,IPC_CREAT|0666);//设置消息队列

	while(1)
	{
		//接受消息队列中的消息？？msgsnd
		ret = msgrcv(msg_queue_id,queue_recv_msg,sizeof(long int)+(72+60)*sizeof(char),msg_type,0);
		if(ret==-1)
		{
			perror("msgrecv error!\n");
			log_error("msgrecv error! local_msg_daemon\n");
			exit(1);
		}

		final = strdup(queue_recv_msg+sizeof(long int));
		printf("final_recv_msg = %s\n",final);

		switch(msg_type_computation(final))
		{
			case SUB_TASK_FINISH:
				o_mpi_sub_task_finish_handler(final);
				printf("msg : out sub task finish\n");
				break;
			case CHILD_CREATE:
				o_mpi_child_create_handler(final);
				printf("msg : out child create\n");
				break;				
			case CHILD_WAIT_ALL:
				o_mpi_child_wait_all_handler(final);
				printf("msg : out child wait all\n");
				break;
/*				
			case GET_SUB_TASK_IP:
				printf("msg : out get sub task ip\n");
				o_get_sub_task_handler(comm_source,ack_tag,final);
				break;
*/
			default:
				printf("unknown msg type!\n");
				log_error("unknown msg type! local_msg_daemon\n");
				exit(1);
		}
		
//		o_mpi_sub_task_finish_handler(final_recv_msg);

		free(final);
	}
}

void back_to_main_master_handler(int comm_source,int ack_tag,char *arg)
{
	pthread_mutex_lock(&local_machine_role_m_lock);

	local_machine_role = FREE_MACHINE;

	pthread_mutex_unlock(&local_machine_role_m_lock);

	send_ack_msg(comm_source,ack_tag,"");

	API_registration_m(local_machine_status);
}

/*
void o_get_sub_task_handler(int comm_source,int ack_tag,char *arg)
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

	if(is_in_sub_task_running_list(type,job_id,top_id,id)==1)
	{
		reply_msg = strdup(local_ip);
	}
	else
	{
		tt_arg = strdup(arg);
		API_get_sub_task_ip_c_to_s(tt_arg,ret_ip);
		reply_msg = strdup(ret_ip);
		free(tt_arg);
	}

	send_ack_msg(comm_source,ack_tag,reply_msg);

	free(reply_msg);
	free(t_arg);

}
*/

int is_in_sub_task_running_list(int type,int job_id,int top_id,int id[10])
{
	struct sub_task_running_list_element *t_sub_task_running_list;
	int i;

	pthread_mutex_lock(&sub_task_running_list_m_lock);

	t_sub_task_running_list = sub_task_running_list;

	while(t_sub_task_running_list!=NULL)
	{
		if((type==t_sub_task_running_list->type)&&(job_id==t_sub_task_running_list->job_id))
		{
			if(type==0)
			{
				if(top_id==t_sub_task_running_list->top_id)
				{
					break;
				}
			}
			else
			{
				for(i=0;i<10;i++)
				{
					if(id[i]!=t_sub_task_running_list->id[i])
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

		t_sub_task_running_list = t_sub_task_running_list->next;
	}

	pthread_mutex_unlock(&sub_task_running_list_m_lock);

	if(t_sub_task_running_list!=NULL)
	{
		return 1;
	}
	else
	{
		return 0;
	}
}

void child_wake_up_all_handler(int comm_source,int ack_tag,char *arg)
{
	int type;
	int job_id;
	int top_id;
	int id[10];
	int pid;
	char *t_arg;
	char *ret_arg;
	char *parameter;
	char *save_ptr;
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

	ret_arg = strdup(strtok_r(NULL,",",&save_ptr));

	API_child_wake_up_all_c_to_p(type,job_id,top_id,id,ret_arg);

	free(ret_arg);
	free(t_arg);
}

void o_mpi_child_wait_all_handler(char *arg)
{
	struct child_wait_all_list_element t_child_wait_all_list;
	struct child_wait_all_list_element *t;
	char *t_arg,*tt_arg;
	char machine_id_c[6];
	char *parameter;
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
	t->pid = atoi(parameter);

	t->next = NULL;

	pthread_mutex_lock(&child_wait_all_list_c_m_lock);

	t->next = child_wait_all_list_c;
	child_wait_all_list_c = t;

	pthread_mutex_unlock(&child_wait_all_list_c_m_lock);


	//delete pid arg
	len = strlen(tt_arg);
	i = len-1;
	while(tt_arg[i]!=',')
	{
		i--;
	}
	tt_arg[i] = '\0';

	strcat(tt_arg,",");
	itoa(machine_id_c, sub_machine_id);
	strcat(tt_arg, machine_id_c);

	API_child_wait_all_c_to_s(tt_arg);

	free(t_arg);
	free(tt_arg);
}

/*
void o_child_wait_all_handler(int comm_source,int ack_tag,char *arg)
{
	struct child_wait_all_list_element t_child_wait_all_list,*t;
	char *t_arg,*tt_arg;
	char *machine_id_c;
	char *parameter;
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
	t->pid = atoi(parameter);

	t->next = NULL;

	pthread_mutex_lock(&child_wait_all_list_c_m_lock);

	t->next = child_wait_all_list_c;
	child_wait_all_list_c = t;

	pthread_mutex_unlock(&child_wait_all_list_c_m_lock);


	//delete pid arg
	len = strlen(tt_arg);
	i = len-1;
	while(tt_arg[i]!=',')
	{
		i--;
	}
	tt_arg[i] = '\0';

	strcat(tt_arg,",");
	machine_id_c = itoa(sub_machine_id);
	strcat(tt_arg,machine_id_c);

	API_child_wait_all_c_to_s(tt_arg);

	free(t_arg);
	free(tt_arg);
	free(machine_id_c);
}
*/

void o_mpi_child_create_handler(char *arg)
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

	API_child_create_c_to_s(t_arg);

	free(t_arg);
}

/*
void o_child_create_handler(int comm_source,int ack_tag,char *arg)
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

	API_child_create_c_to_s(t_arg);

	free(t_arg);
}
*/

void o_mpi_sub_task_finish_handler(char *arg)
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

	delete_element_from_sub_task_running_list(t_arg);

	printf("before API sub task finish c to s\n");
	API_sub_task_finish_c_to_s(t_arg);
	printf("after API sub task finish c to s\n");

	free(t_arg);
}

/*
void o_sub_task_finish_handler(int comm_source,int ack_tag,char *arg)
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

	delete_element_from_sub_task_running_list(t_arg);

	send_ack_msg(comm_source,ack_tag,"");

	API_sub_task_finish_c_to_s(t_arg);

	free(t_arg);
}
*/

void delete_element_from_sub_task_running_list(char *arg)
{
	struct sub_task_running_list_element *t_sub_task_running_list,*t;
	char *t_arg;
	char *parameter;
	char *save_ptr;
	int type;
	int job_id;
	int top_id;
	int id[10];
	int i;

	assert(sub_task_running_list!=NULL);

	t_arg = strdup(arg);

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

	pthread_mutex_lock(&sub_task_running_list_m_lock);

	t_sub_task_running_list = sub_task_running_list;
	while(t_sub_task_running_list!=NULL)
	{
		if((type==t_sub_task_running_list->type)&&(job_id==t_sub_task_running_list->job_id))
		{
			if(type==0)
			{
				if(top_id==t_sub_task_running_list->top_id)
				{
					break;
				}
			}
			else
			{
				for(i=0;i<10;i++)
				{
					if(id[i]!=t_sub_task_running_list->id[i])
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

		t_sub_task_running_list = t_sub_task_running_list->next;
	}

	assert(t_sub_task_running_list!=NULL);

	t = t_sub_task_running_list;

	if(t==sub_task_running_list)
	{
		sub_task_running_list = sub_task_running_list->next;
		free(t);
	}
	else
	{
		t_sub_task_running_list = sub_task_running_list;

		while(t_sub_task_running_list->next!=NULL)
		{
			if(t_sub_task_running_list->next==t)
			{
				t_sub_task_running_list->next = t->next;
				free(t);
				break;
			}

			t_sub_task_running_list = t_sub_task_running_list->next;
		}

		assert(t_sub_task_running_list);
	}

	pthread_mutex_unlock(&sub_task_running_list_m_lock);

	free(t_arg);
}

void sub_task_assign_handler(int comm_source,int ack_tag,char *arg)
{
	char *t_arg;
	char *tt_arg;
	char *parameter;
	int i;

	i = 0;
	while(arg[i]!=';')
	{
		i++;
	}

	i++;

	t_arg = strdup(arg+i);

	add_element_to_sub_task_running_list(t_arg);

	send_ack_msg(comm_source,ack_tag,"");

	run_sub_task(t_arg);

	free(t_arg);
/*
	additional_sub_task_count++;
	if(additional_sub_task_count>=5)
	{
		send_machine_heart_beat();
	}
	*/
}

void add_element_to_sub_task_running_list(char *arg)
{
	struct sub_task_running_list_element *t;
	char *t_arg;
	char *parameter;
	char *save_ptr;
	int i;

	t = (struct sub_task_running_list_element *)malloc(sizeof(struct sub_task_running_list_element));

	t_arg = strdup(arg);

	printf("t_arg = %s\n",t_arg);

	parameter = strtok_r(t_arg,",",&save_ptr);

	parameter = strtok_r(NULL,",",&save_ptr);
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

	t->status = RUNNING;

	t->next = NULL;

	pthread_mutex_lock(&sub_task_running_list_m_lock);

	t->next = sub_task_running_list;

	sub_task_running_list = t;

	pthread_mutex_unlock(&sub_task_running_list_m_lock);

	free(t_arg);
}

void run_sub_task(char *i_arg)
{
	char file_path[64];
	char **arg;
	char *t_arg;
	char *parameter;
	char *save_ptr;
	int ret;
	int i;

	t_arg = strdup(i_arg);

	arg = (char **)malloc(7*sizeof(char *));

	strcpy(file_path,"/usr/test/mpi/");

	parameter = strtok_r(t_arg,",",&save_ptr);
	strcat(file_path,parameter);

	for(i=0;i<5;i++)
	{
		parameter = strtok_r(NULL,",",&save_ptr);
		arg[i] = strdup(parameter);
	}

	arg[5] = strdup("127.0.0.1");
	arg[6] = NULL;

	for(;;)
	{
		ret = vfork();
	
		if(ret==0)
		{
			execv(file_path,arg);
		}
		else if(ret>0)
		{
			break;
		}
		else if(ret==-1)
		{
			printf("vfork error!\n");
			log_error("vfork error!\n");
			exit(1);
			//restart here
		}
	}
	
	for(i=0;i<6;i++)
	{
		free(arg[i]);
	}
	free(arg);

	free(t_arg);
}

/**
 * 计算节点向所管理的节点注册
 */
void computation_node_assign_handler(int comm_source,int ack_tag,char *arg)
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

	pthread_mutex_lock(&local_machine_role_m_lock);
	//？？这段代码表示什么意思，多个角色？？
	if(local_machine_role==FREE_MACHINE)
	{
		local_machine_role = COMPUTATION_MACHINE;
	}
	else if(local_machine_role==HALF_SUB_MASTER_MACHINE)
	{
		local_machine_role = SUB_MASTER_MACHINE;
	}
	else
	{
		printf("strang assign computation node previous status = %d\n",local_machine_role);
	}

	pthread_mutex_unlock(&local_machine_role_m_lock);

	printf("arg = %s!\n",t_arg);

	sub_master_comm_id = atoi(t_arg);
	printf("!@#sub master comm id = %d\n",sub_master_comm_id);

//modified
	API_registration_s(sub_master_comm_id,local_machine_status);

	send_ack_msg(comm_source,ack_tag,"");
	free(t_arg);
}

/**
 * 接收从主节点传出的负责调度的信号，节点类型变为HALF_SUB_MASTER_MACHINE，开始工作
 */
void sub_scheduler_assign_handler(int comm_source,int ack_tag,char *arg)
{
	pthread_attr_t attr;
	void *ret_val;
	char *t_arg;
	int ret;
	int i;

	i = 0;

	while(arg[i]!=';')
	{
		i++;
	}

	i++;

	t_arg = strdup(arg+i);

	printf("arg = %s!\n",t_arg);

	sub_scheduler_init(t_arg);

	pthread_mutex_lock(&local_machine_role_m_lock);

	local_machine_role = HALF_SUB_MASTER_MACHINE;

	pthread_mutex_unlock(&local_machine_role_m_lock);

	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr,PTHREAD_CREATE_DETACHED);

//	pthread_create(&sub_scheduler_server_tid,&attr,sub_scheduler_server,NULL);
//	pthread_create(&sub_scheduler_tid,&attr,sub_scheduler,NULL);
//	pthread_create(&sub_cluster_heart_beat_daemon_tid,&attr,sub_cluster_heart_beat_daemon,NULL);

	ret = pthread_create(&sub_scheduler_server_tid,&attr,sub_scheduler_server,NULL);
	if(ret!=0)
	{
		perror("p_c sub_scheduler_server\n");
		log_error("pthread_create sub_scheduler_server error\n");
		exit(1);
	}
	ret = pthread_create(&sub_scheduler_tid,NULL,sub_scheduler,NULL);
	if(ret!=0)
	{
		perror("p_c sub_scheduler\n");
		log_error("pthread_create sub_scheduler_server error\n");
		exit(1);
	}
	ret = pthread_create(&sub_cluster_heart_beat_daemon_tid,NULL,sub_cluster_heart_beat_daemon,NULL);
	if(ret!=0)
	{
		perror("p_c sub_cluster_heart_beat\n");
		log_error("pthread_create sub_cluster_heart_beat error\n");
		exit(1);
	}

	pthread_attr_destroy(&attr);

	send_ack_msg(comm_source,ack_tag,"");

	free(t_arg);
}

void sub_scheduler_init(char *t_arg)
{
	char *parameter;
	char *save_ptr;
	int i;

	sub_scheduler_on = 1;

	parameter = strtok_r(t_arg,",",&save_ptr);
	sub_cluster_id = atoi(parameter);

	parameter = strtok_r(NULL,",",&save_ptr);
	sub_machine_num = atoi(parameter);


	//      coresponding send     need  to be modified

	sub_machine_array = (struct machine_status_array_element *)malloc(sub_machine_num*sizeof(struct machine_status_array_element));

	for(i=0;i<sub_machine_num;i++)
	{
		sub_machine_array[i].machine_id = i+1;

		parameter = strtok_r(NULL,",",&save_ptr);

		sub_machine_array[i].comm_id = atoi(parameter);

		if(sub_machine_array[i].comm_id==0)
		{
			printf("error in sub machine array comm id\n");
			log_error("error in sub machine array comm id\n");
			exit(1);
		}

		sub_machine_array[i].sub_master_id = 0;
		memset(&sub_machine_array[i].machine_description,0,sizeof(struct machine_description_element));
		sub_machine_array[i].machine_status = 0;
	}
}

void *sub_scheduler_server(void *tt_arg)
{
	struct server_arg_element *server_arg;
	pthread_t tid;
	pthread_attr_t attr;
	MPI_Status status;
	int comm_source;
	int comm_tag;
	int ack_tag;
	int length;
	int source;
	int ret;

	char arg[64];
	char *t_arg;
	char *type_c;
	char *parameter;
	char *save_ptr;

	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr,PTHREAD_CREATE_DETACHED);

	while(1)
	{
		MPI_Recv(arg,20,MPI_CHAR,MPI_ANY_SOURCE,1,MPI_COMM_WORLD,&status);

		if(sub_scheduler_on==1)
		{

			server_arg = NULL;
			server_arg = (struct server_arg_element *)malloc(sizeof(struct server_arg_element));
			server_arg->msg = strdup(arg);
			server_arg->status = status;

			ret = pthread_create(&tid,&attr,sub_scheduler_server_handler,(void *)server_arg);
			if(ret!=0)
			{
				perror("p_c computation_server\n");
				log_error("pthread_create computation_server\n");
				exit(1);
			}
		}
		else
		{
			t_arg = strdup(arg);
			printf("shut down arg = %s\n",arg);

			parameter = strtok_r(t_arg,";",&save_ptr);	//comm_tag;length;ack_tag
			comm_tag = atoi(parameter);

			parameter = strtok_r(NULL,";",&save_ptr);
			length = atoi(parameter);

			parameter = strtok_r(NULL,";",&save_ptr);
			ack_tag = atoi(parameter);

			free(t_arg);

			comm_source = status.MPI_SOURCE;


			if(comm_source<0)
			{
				printf("comm_source<0 !  danger!\n");
				log_error("phthread_create comm_source<0 ! danger!\n");
				exit(1);
			}
			MPI_Recv(arg,64,MPI_CHAR,comm_source,comm_tag,MPI_COMM_WORLD,&status);

			type_c = strtok_r(arg,";",&save_ptr);

			if(strcmp(type_c,"TYPE:SUB_CLUSTER_SHUT_DOWN"))
			{
				send_ack_msg(comm_source,ack_tag,"");
				continue;
			}



			printf("quit final = %s\n",arg);

			pthread_join(sub_scheduler_tid,NULL);
			pthread_join(sub_cluster_heart_beat_daemon_tid,NULL);
			send_ack_msg(comm_source,ack_tag,"");
			break;
		}
	}

	pthread_attr_destroy(&attr);

	printf("sub cluster closed\n");
	return NULL;
}

msg_t msg_type_computation(char *msg)
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

	if(!strcmp(type,"SUB_SCHEDULER_ASSIGN"))
	{
		return SUB_SCHEDULER_ASSIGN;
	}
	else if(!strcmp(type,"COMPUTATION_NODE_ASSIGN"))
	{
		return COMPUTATION_NODE_ASSIGN;
	}
	else if(!strcmp(type,"SUB_TASK_ASSIGN"))
	{
		return SUB_TASK_ASSIGN;
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
	else if(!strcmp(type,"BACK_TO_MAIN_MASTER"))
	{
		return BACK_TO_MAIN_MASTER;
	}
	else
	{
		return UNKNOWN;
	}
}

