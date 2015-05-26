#include "stdio.h"
#include "unistd.h"
#include "pthread.h"
#include "sys/socket.h"
#include "arpa/inet.h"
#include "strings.h"
#include "stdlib.h"
#include "string.h"
#include "../data.h"
#include "../common/communication.h"
#include "assert.h"
#include "signal.h"
#include "sys/msg.h"
#include "../common/api.h"
#include "mpi_pframe.h"

int API_o_mpisub_task_finish(char *arg[],char *ret_msg);
int API_o_mpichild_create(char *arg[],int child_num,char*child_arg[]);
int API_o_mpichild_wait_all(char *arg[],int child_num,char ***child_arg);
//int API_o_mpiget_parent_id(char *arg[],char *ip);
//int API_o_mpiget_child_id(char *arg[],int child_id,char *ip);
void fill_local_msg_type(msg_t type,char *type_c);
void mpi_send_local_msg(msg_t msg_type,char *send_msg);
int API_o_mpi_child_wait_all(char *arg[],int child_num,char ***child_arg);
int API_o_mpi_child_create(char *arg[],int child_num,char*child_arg[]);
int is_to_me(char *msg,char **arg);

//char *itoa(int num);
void sigusr1_handler(int signo);
long int get_msg_type(char **arg);

int sigusr1_flag;

int mpipframe_sub_task_finish(char *arg[],char *ret_msg)
{
	char null_msg[5];

	if(strlen(ret_msg)>64)
	{
		printf("ret_msg is longer than 64(has been trunked)\n");
		ret_msg[63] = '\0';
	}

	printf("ret msg is %s\n",ret_msg);

	if(ret_msg!=NULL)
	{
		API_o_mpisub_task_finish(arg,ret_msg);
	}
	else
	{
		strcpy(null_msg,"NULL");
		API_o_mpisub_task_finish(arg,null_msg);
	}

	return 1;
}

int API_o_mpisub_task_finish(char *arg[],char *exit_msg)
{
	char send_msg[72+60];
	char *ret_msg;
	char *final_send_msg;
	char ip[16];		//local deamo ip will added as the last argv when exec.
	int type;
	int job_id;
	int top_id;
	int id[10];
	int i;

	strcpy(ip,arg[5]);

	strcpy(send_msg,arg[0]);
	strcat(send_msg,",");

	for(i=1;i<4;i++)
	{
		strcat(send_msg,arg[i]);
		strcat(send_msg,",");
	}

	strcat(send_msg,exit_msg);
	strcat(send_msg,",");

	mpi_send_local_msg(SUB_TASK_FINISH,send_msg);

//	send_recv_msg(ip,2,SUB_TASK_FINISH,send_msg,&ret_msg);

//	free(ret_msg);

	return 1;
}

void mpi_send_local_msg(msg_t msg_type,char *send_msg)
{
	char *final_send_msg;
	char *t_send_msg;
	char msg_type_c[32];
	long int ipc_msg_type;
	int t_send_msg_len;
	int msg_queue_id;


	msg_queue_id = msgget(MSG_QUEUE_KEY,IPC_CREAT|0666);

	fill_local_msg_type(msg_type,msg_type_c);

	t_send_msg_len = strlen(msg_type_c)+strlen(send_msg);
	t_send_msg = (char *)malloc((t_send_msg_len+1)*sizeof(char));

	strcpy(t_send_msg,msg_type_c);
	strcat(t_send_msg,send_msg);

	final_send_msg = (void *)malloc(sizeof(long int)+(strlen(t_send_msg)+1)*sizeof(char));

	ipc_msg_type = MPI_LOCAL_DEAMON_MSG_TYPE;

	memcpy(final_send_msg,(void *)&ipc_msg_type,sizeof(long int));
	memcpy(final_send_msg+sizeof(long int),t_send_msg,(strlen(t_send_msg)+1)*sizeof(char));

	msgsnd(msg_queue_id,final_send_msg,sizeof(long int)+(strlen(t_send_msg)+1)*sizeof(char),0);
	free(final_send_msg);
}

/*
int mpipframe_get_child_id(char *arg[],int child_id,char **ip)
{
	char t_ip[16];

	API_o_get_child_ip(arg,child_id,t_ip);

	if(strcmp(t_ip,"0.0.0.0"))
	{
		*ip = strdup(t_ip);
		return 1;			//success
	}
	else
	{
		return 0;
	}
}

int mpipframe_get_parent_id(char *arg[],char **ip)
{
	char t_ip[16];

	API_o_get_parent_ip(arg,t_ip);

	if(strcmp(t_ip,"0.0.0.0"))
	{
		*ip = strdup(t_ip);
		return 1;			//success
	}
	else
	{
		return 0;
	}
}
*/

 
int mpipframe_child_wait_all(char *arg[],int child_num,char ***child_arg)
{
	API_o_mpi_child_wait_all(arg,child_num,child_arg);

	return 1;
}

int mpipframe_child_create(char *arg[],int child_num,char *child_arg[])
{
	int i;

	if(child_num==0)
	{
		printf("pframe_chind_create error:child num is 0\n");
		exit(1);
	}

	for(i=0;i<child_num;i++)
	{
		if(strlen(child_arg[i])>64)
		{
			printf("child_arg[%d] is longer than 64(has been trunked)\n",i);
			child_arg[i][63] = '\0';
		}
	}

	printf("before o api child create\n");

	API_o_mpi_child_create(arg,child_num,child_arg);

	return 1;
}

int API_o_mpi_child_wait_all(char *arg[],int child_num,char ***child_arg)
{
	void *queue_recv_msg;
	void *final_send_msg;
	char send_msg[34];
	char ip[16];
	char *ret_msg;
	char *final_recv_msg;
	char *parameter;
	char *t_arg;
	char *save_ptr;
	int msg_queue_id;
	long int msg_type;
	int my_pid;
	int ret;
	int i;

	strcpy(send_msg,arg[0]);
	strcat(send_msg,",");

	for(i=1;i<4;i++)
	{
		strcat(send_msg,arg[i]);
		strcat(send_msg,",");
	}

	my_pid = getpid();

	t_arg = itoa(my_pid);
	strcat(send_msg,t_arg);
	free(t_arg);

	strcpy(ip,arg[5]);

//	send_recv_msg(ip,2,CHILD_WAIT_ALL,send_msg,&ret_msg);
	mpi_send_local_msg(CHILD_WAIT_ALL,send_msg);

//	printf("child wait all ret_msg = %s\n");
	signal(SIGUSR1,sigusr1_handler);		//should save previous handler here.

	sigusr1_flag = 0;

	while(1)
	{
		pause();
		if(sigusr1_flag==1)
		{
			sigusr1_flag = 0;
			break;
		}
	}


	msg_type = get_msg_type(arg);
	msg_queue_id = msgget(WAKE_UP_MSG_QUEUE_KEY,IPC_CREAT|0666);

	queue_recv_msg = (void *)malloc(sizeof(long int)+(18+60+child_num*64)*sizeof(char));
	final_send_msg = (void *)malloc(sizeof(long int)+(18+60+child_num*64)*sizeof(char));

	while(1)
	{
		ret = msgrcv(msg_queue_id,queue_recv_msg,18+60+child_num*64,msg_type,0);
		if(ret==-1)
		{
			perror("msgrecv error");
			exit(1);
		}

		final_recv_msg = strdup(queue_recv_msg+sizeof(long int));
		break;

		if(is_to_me(final_recv_msg,arg))
		{
			break;
		}
		else
		{
			printf("2\n");
			memcpy(final_send_msg,(void *)&msg_type,sizeof(long int));
			memcpy(final_send_msg+sizeof(long int),final_recv_msg,(strlen(final_recv_msg)+1)*sizeof(char));
			msgsnd(msg_queue_id,final_send_msg,sizeof(long int)+(strlen(final_recv_msg)+1)*sizeof(char),0);
			usleep(200000);
		}
	}

	printf("final_recv_msg = %s\n",final_recv_msg);
	*child_arg = (char **)malloc(child_num*sizeof(char *));

	parameter = strtok_r(final_recv_msg,"|",&save_ptr);

//	*(child_arg) = (char **)malloc(child_num*sizeof(char *));
	for(i=0;i<child_num;i++)
	{
		parameter = strtok_r(NULL,"_",&save_ptr);
		printf("parameter = %s\n",parameter);
		(*(child_arg))[i] = strdup(parameter);
	}

	free(queue_recv_msg);
	free(final_send_msg);
	free(final_recv_msg);

	return 1;
}

int is_to_me(char *msg,char **arg)
{
	int type1,type2;
	int job_id1,job_id2;
	int top_id1,top_id2;
	int id1[10],id2[10];
	char *t_arg;
	char *tt_arg;
	char *parameter;
	char *save_ptr;
	int i;

	tt_arg = strdup(msg);

	t_arg = strdup(strtok_r(tt_arg,"|",&save_ptr));


	parameter = strtok_r(t_arg,",",&save_ptr);
	type1 = atoi(parameter);

	parameter = strtok_r(NULL,",",&save_ptr);
	job_id1 = atoi(parameter);

	parameter = strtok_r(NULL,",",&save_ptr);
	top_id1 = atoi(parameter);

	parameter = strtok_r(NULL,"_",&save_ptr);
	id1[0] = atoi(parameter);

	for(i=1;i<10;i++)
	{
		parameter = strtok_r(NULL,"_",&save_ptr);
		id1[i] = atoi(parameter);
	}

	free(t_arg);

	type2 = atoi(arg[0]);
	job_id2 = atoi(arg[1]);
	top_id2 = atoi(arg[2]);
	t_arg = strdup(arg[3]);

	parameter = strtok_r(t_arg,"_",&save_ptr);
	id2[0] = atoi(parameter);

	for(i=1;i<10;i++)
	{
		parameter = strtok_r(NULL,"_",&save_ptr);
		id2[i] = atoi(parameter);
	}

	free(t_arg);
	free(tt_arg);

	if(type1!=type2)
	{
		return 0;
	}

	if(type1==0)
	{
		if(job_id1!=job_id2)
		{
			return 0;
		}
		if(top_id1!=top_id2)
		{
			return 0;
		}
	}
	else
	{
		if(job_id1!=job_id2)
		{
			return 0;
		}
		for(i=0;i<10;i++)
		{
			if(id1[i]!=id2[i])
			{
				return 0;
			}
		}
	}

	return 1;
}

long int get_msg_type(char **arg)
{
	int type;
	int job_id;
	int top_id;
	int id[10];
	long int sum;
	char *t_arg;
	char *parameter;
	char *save_ptr;
	int i;

	type = atoi(arg[0]);
	job_id = atoi(arg[1]);
	top_id = atoi(arg[2]);
	t_arg = strdup(arg[3]);

	parameter = strtok_r(t_arg,"_",&save_ptr);
	id[0] = atoi(parameter);
	for(i=1;i<10;i++)
	{
		parameter = strtok_r(NULL,"_",&save_ptr);
		id[i] = atoi(parameter);
	}

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

void sigusr1_handler(int signo)
{
	if(signo==SIGUSR1)
	{
		sigusr1_flag = 1;
	}
	else
	{
		printf("sigusr1_handler:signo = %d,not SIGUSR1",signo);
	}
}

int API_o_mpi_child_create(char *arg[],int child_num,char*child_arg[])
{
	char send_msg[38+6+child_num*64];
	char ip[16];				//deamo ip
	char *t_msg;
//	char *ret_msg;
	char null_msg[5];
	int i;

	printf("argv:\n");
	for(i=0;i<6;i++)
	{
		printf("%d:%s\n",i,arg[i]);
	}

	strcpy(ip,arg[5]);

	printf("child_arg: num = %d\n",child_num);
	for(i=0;i<child_num;i++)
	{
		printf("%d:%s\n",i,child_arg[i]);
	}

	strcpy(send_msg,arg[0]);
	strcat(send_msg,",");

	for(i=1;i<4;i++)
	{
		strcat(send_msg,arg[i]);
		strcat(send_msg,",");
	}

	t_msg = itoa(child_num);
	strcat(send_msg,t_msg);
	free(t_msg);
	strcat(send_msg,",");

	strcpy(null_msg,"NULL");

	for(i=0;i<child_num;i++)
	{
		if(child_arg[i]!=NULL)
		{
			strcat(send_msg,child_arg[i]);
		}
		else
		{
			strcat(send_msg,null_msg);
		}
		strcat(send_msg,",");
	}

//	send_recv_msg(ip,2,CHILD_CREATE,send_msg,&ret_msg);
	mpi_send_local_msg(CHILD_CREATE,send_msg);
//	free(ret_msg);

	return 1;
}
/*
char *itoa(int num)
{
	char *num_c;
	char t[6];
	int index;
	int i;

	if(num>65536||num<0)
	{
		printf("itoa:num invalid %d,(0-65535)\n",num);
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
*/

/*
int API_o_get_parent_ip(char *arg[],char *ip)
{
	char send_msg[18+60];
	char t_id[21];
	char t_ip[16];
	char local_ip[16];
	char *ret_msg;
	int type;
	int i;

	type = atoi(arg[0]);

	strcpy(send_msg,arg[0]);
	strcat(send_msg,",");

	strcat(send_msg,arg[1]);
	strcat(send_msg,",");
	strcat(send_msg,arg[2]);
	strcat(send_msg,",");

	if(type==1)
	{
		strcpy(t_id,arg[3]);
		for(i=18;i>=0;i=i-2)
		{
			if(send_msg[i]!='0')
			{
				t_id[i] = '0';
				break;
			}
		}

		if(i==2)
		{
			memset(send_msg,'0',sizeof(char));
		}

		strcat(send_msg,t_id);
	}
	else
	{
		strcat(send_msg,arg[3]);
	}

	strcpy(local_ip,arg[5]);

	send_recv_msg(local_ip,2,GET_SUB_TASK_IP,send_msg,&ret_msg);

	strcpy(ip,ret_msg+4);
	free(ret_msg);

	return 1;
}

int API_o_get_child_ip(char *arg[],int child_id,char *ip)
{
	char send_msg[18+60];
	char t_id[21];
	char t_ip[16];
	char local_ip[16];
	char *ret_msg;
	char *t_arg;
	int type;
	int i;

	type = atoi(arg[0]);

	strcpy(send_msg,arg[0]);
	strcat(send_msg,",");

	strcat(send_msg,arg[1]);
	strcat(send_msg,",");
	strcat(send_msg,arg[2]);
	strcat(send_msg,",");

	if(type==1)
	{
		strcpy(t_id,arg[3]);
		for(i=0;i<=16;i=i+2)
		{
			if(send_msg[i]=='0')
			{
				t_id[i] = '0'+child_id;
				break;
			}
		}

		if(i>16)
		{
			printf("API_o_get_child_ip:too many sub task level!");
			exit(1);
		}

		strcat(send_msg,t_id);
	}
	else
	{
		memset(send_msg,'1',sizeof(char));

		strcat(send_msg,arg[2]);
		strcat(send_msg,"_");

		for(i=1;i<10;i++)
		{
			strcat(send_msg,"0_");
		}
	}

	strcpy(local_ip,arg[5]);

	send_recv_msg(local_ip,2,GET_SUB_TASK_IP,send_msg,&ret_msg);

	strcpy(ip,ret_msg+4);
	free(ret_msg);

	return 1;
}
*/

void fill_local_msg_type(msg_t type,char *type_c)
{
	switch(type)
	{
		case SUB_TASK_FINISH:
			strcpy(type_c,"TYPE:SUB_TASK_FINISH;");
			break;
		case CHILD_CREATE:
			strcpy(type_c,"TYPE:CHILD_CREATE;");
			break;
		case CHILD_WAIT_ALL:
			strcpy(type_c,"TYPE:CHILD_WAIT_ALL;");
			break;
		case CHILD_WAKE_UP_ALL:
			strcpy(type_c,"TYPE:CHILD_WAKE_UP_ALL;");
			break;
		case GET_SUB_TASK_IP:
			strcpy(type_c,"TYPE:GET_SUB_TASK_IP;");
			break;
		default:
			printf("fill_msg_type:unknown msg type!\n");
			exit(1);
	}
}

