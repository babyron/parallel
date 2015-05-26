#include "stdio.h"
#include "unistd.h"
#include "pthread.h"
#include "sys/socket.h"
#include "arpa/inet.h"
#include "strings.h"
#include "stdlib.h"
#include "string.h"
#include "../data.h"
#include "poll.h"
#include "fcntl.h"
#include "assert.h"
#include "api.h"
#include "mpi.h"

size_t advanced_write(int fd,void *buf,size_t count);
size_t advanced_read(int fd,void *buf,size_t count,msg_t msg_type);
void fill_msg_type(msg_t type,char *type_c);
void fd_set_nonblocking(int fd);
int get_a_tag();

/**
 * 进程通信函数，用于给指定的目的地传输消息并接受该消息源回传的信号
 * @comm_source:指定的消息源
 * @init_tag:约定的标识符，根据信号类型不同而不同
 * @msg_type:指定的消息传递类型
 * @send_msg:需要传送的消息，在第二步传输时发送
 * @recv_msg:用于存储接受到的消息
 */
void send_recv_msg(int comm_source, int init_tag, msg_t msg_type, char *send_msg, char **recv_msg)
{
	char pre_msg[20];
//	char *final_msg;
	char final_msg[strlen(send_msg)+50];
	char *ret_msg;
	char msg_type_c[32];
	char *parameter;
	time_t start,finish;
	int comm_tag;
	int ack_tag;
	int length;
	int pid;

	MPI_Status status;

	ret_msg = (char *)malloc(32 * sizeof(char));
	if(ret_msg == NULL)
	{
		printf("ret_msg alloc faild\n");
		log_error("ret_msg alloc faild\n");
		exit(1);
	}

	fill_msg_type(msg_type,msg_type_c);

	comm_tag = get_a_tag();
	parameter = itoa(comm_tag);
	strcpy(pre_msg,parameter);
	free(parameter);

	strcat(pre_msg,";");

	length = strlen(send_msg) + strlen(msg_type_c) + 1;
	parameter = itoa(length);
	strcat(pre_msg, parameter);
	free(parameter);

	strcat(pre_msg, ";");

	ack_tag = get_a_tag();
	parameter = itoa(ack_tag);
	strcat(pre_msg, parameter);
	free(parameter);
	//每次发送之前都要预先通知发送的类型以及发送数据的长度
	MPI_Send(pre_msg, strlen(pre_msg) + 1, MPI_CHAR, comm_source, init_tag, MPI_COMM_WORLD);

	strcpy(final_msg, msg_type_c);
	strcat(final_msg, send_msg);

	start = time(NULL);

	MPI_Sendrecv(final_msg, strlen(final_msg) + 1, MPI_CHAR, comm_source, comm_tag, ret_msg, 32, MPI_CHAR, comm_source, ack_tag, MPI_COMM_WORLD, &status);

	finish = time(NULL);
	log_API(final_msg, msg_type, start, finish);

	*recv_msg = strdup(ret_msg);
	free(ret_msg);
}

int get_a_tag()
{
	pthread_mutex_lock(&t_tag_m_lock);
	t_tag = (t_tag+1)%49999;
	pthread_mutex_unlock(&t_tag_m_lock);

	return t_tag+100;
}

/*
void advanced_send(int socket_fd,char *msg)
{
	int msg_len;

	msg_len = strlen(msg)+1;

	advanced_write(socket_fd,&msg_len,sizeof(msg_len));
	advanced_write(socket_fd,msg,msg_len);
}

void advanced_recv(int socket_fd,char **msg,msg_t msg_type)
{
	int msg_len;

	advanced_read(socket_fd,&msg_len,sizeof(msg_len),msg_type);

	*msg = (char *)malloc(msg_len*sizeof(char));

	advanced_read(socket_fd,*msg,msg_len,msg_type);
}
*/

/**
 * 用于填充完整的的消息类型
 */
void fill_msg_type(msg_t type,char *type_c)
{
	switch(type)
	{
		case SUB_CLUSTER_HEART_BEAT:
			strcpy(type_c,"TYPE:SUB_CLUSTER_HEART_BEAT;");
			break;
		case JOB_SUBMIT:
			strcpy(type_c,"TYPE:JOB_SUBMIT;");
			break;
		case SCHEDULE_UNIT_ASSIGN:
			strcpy(type_c,"TYPE:SCHEDULE_UNIT_ASSIGN;");
			break;
		case SCHEDULE_UNIT_FINISH:
			strcpy(type_c,"TYPE:SCHEDULE_UNIT_FINISH;");
			break;
		case SUB_SCHEDULER_ASSIGN:
			strcpy(type_c,"TYPE:SUB_SCHEDULER_ASSIGN;");
			break;
		case COMPUTATION_NODE_ASSIGN:
			strcpy(type_c,"TYPE:COMPUTATION_NODE_ASSIGN;");
			break;
		case SUB_TASK_ASSIGN:
			strcpy(type_c,"TYPE:SUB_TASK_ASSIGN;");
			break;
		case MACHINE_HEART_BEAT:
			strcpy(type_c,"TYPE:MACHINE_HEART_BEAT;");
			break;
		case SUB_TASK_FINISH:
			strcpy(type_c,"TYPE:SUB_TASK_FINISH;");
			break;
		case REGISTRATION_M:
			strcpy(type_c,"TYPE:REGISTRATION_M;");
			break;
		case REGISTRATION_S:
			strcpy(type_c,"TYPE:REGISTRATION_S;");
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
		case SUB_CLUSTER_DESTROY:
			strcpy(type_c,"TYPE:SUB_CLUSTER_DESTROY;");
			break;
		case BACK_TO_MAIN_MASTER:
			strcpy(type_c,"TYPE:BACK_TO_MAIN_MASTER;");
			break;
		case SUB_CLUSTER_SHUT_DOWN:
			strcpy(type_c,"TYPE:SUB_CLUSTER_SHUT_DOWN;");
			break;
		default:
			printf("fill_msg_type:unknown msg type!\n");
			log_error("fill_msg_type:unknown msg type!\n");
			exit(1);
	}
}

void send_ack_msg(int comm_source,int ack_tag,char *ret)
{
	char ack[32];
	int ret_v;
	int len;
	char *parameter;
	int flag;
	MPI_Request request;
	MPI_Status status;
/*
	ack = (char *)malloc(32*sizeof(char));

	if(strlen(ret)>27)
	{
		printf("ret msg too long!\n");
		exit(1);
	}
*/
	strcpy(ack,"ACK:");
	strcat(ack,ret);

	ret_v = MPI_Send(ack,5,MPI_CHAR,comm_source,ack_tag,MPI_COMM_WORLD);

/*
	ret_v = MPI_Isend(ack,strlen(ack)+1,MPI_CHAR,comm_source,ack_tag,MPI_COMM_WORLD,&request);
	while(1)
	{
		MPI_Test(&request,&flag,&status);
		if(flag==1)
		{
			break;
		}
		else
		{
			usleep(100);
		}
	}
	MPI_Wait(&request,&status);
*/
}

/*
size_t send_ret_msg(int fd,char *ret)
{
	char ack[32];
	int len;

	if(strlen(ret)>27)
	{
		printf("ret msg too long!\n");
		exit(1);
	}

	strcpy(ack,"ACK:");
	strcat(ack,ret);

	advanced_send(fd,ack);

	close(fd);
}

size_t advanced_write(int fd,void *buf,size_t count)
{
	size_t rc;

	rc = write(fd,buf,count);

	if(rc==-1)
	{
		printf("fd = %d\n",fd);
		perror("advanced write error");
		while(1);
		exit(1);
	}

	if(rc!=count)
	{
		printf("advanced write error! Write function did not send all data in one msg!:rc = %d,count = %d\n",rc,count);
		exit(1);
	}

	return rc;
}

size_t advanced_read(int fd,void *buf,size_t count,msg_t msg_type)
{
	struct pollfd pfd;
	size_t rc;
	int ret;
	int time_out_count;
	int total_rc;
	char type_c[32];


	pfd.fd = fd;
	pfd.events = POLLIN;

	ret = poll(&pfd,1,4000);

	if(ret==0)
	{
		fill_msg_type(msg_type,type_c);
		printf("d = %d",msg_type);
		printf("recv msg timeout!!!#  %s!\n",type_c);
		exit(1);
	}
	else if(ret==-1)
	{
		perror("advanced read error 1");
		exit(1);
	}

	rc = read(fd,buf,count);

	if(rc==-1)
	{
		perror("advanced read error 2");
		exit(1);
	}

	if(rc!=count)
	{
		printf("fd = %d\n",fd);
		printf("advanced read error! Read function did not recv all data in one msg!:rc = %d,count = %d\n",rc,count);
		exit(1);
	}

	return rc;
}

int open_connection(char *ip,int port)
{
	struct sockaddr_in serv_addr;
	int socket_fd;
	int ret;

	socket_fd = socket(AF_INET,SOCK_STREAM,0);

	if(socket_fd==-1)
	{
		perror("open connection error,create socket fail");
		exit(1);
	}

	bzero(&serv_addr,sizeof(serv_addr));

	serv_addr.sin_family = AF_INET;
	serv_addr.sin_addr.s_addr = inet_addr(ip);
	serv_addr.sin_port = htons(port);

	ret = connect(socket_fd,(struct sockaddr *)&serv_addr,sizeof(serv_addr));

	if(ret==-1)
	{
		printf("%s:%d fd = %d\n",ip,port,socket_fd);
		perror("open connection error");
		exit(1);
	}

	return socket_fd;
}
*/
