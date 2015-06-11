/*
 * computing_machine.c
 *
 *  Created on: 2015年5月29日
 *      Author: ron
 */
#include <stdio.h>
#include <stdlib.h>
#include <sys/msg.h>
#include <pthread.h>
#include <string.h>
#include "computation_server_handler.h"
#include "computing_machine.h"
#include "data_computation.h"
#include "machine_status.h"
#include "./common/api.h"
#include "computation_server_handler.h"
#include "dynamic_info.h"

/*=====================private declaration====================*/
static char queue_recv_msg[38 + 6 + 1000 * 64 + 16];		//max child_num 1000

static void lists_init(void);
static void read_conf_file();
static void *computation_server(void *t_arg);
static void *dynamic_info_get_daemon(void *arg);
static void *machine_heart_beat_daemon(void *arg);
static void *local_msg_daemon(void *arg);

/**
 * 各种链表初始化
 */
static void lists_init(void) {
	waiting_schedule_list = NULL;
	schedule_list = NULL;
	schedule_unit_status_list = NULL;
	child_wait_all_list_s = NULL;
	child_wait_all_list_c = NULL;
	sub_task_running_list = NULL;

	pthread_mutex_init(&waiting_schedule_list_m_lock, NULL);
	pthread_mutex_init(&schedule_list_m_lock, NULL);
	pthread_mutex_init(&schedule_unit_status_list_m_lock, NULL);
	pthread_mutex_init(&child_wait_all_list_s_m_lock, NULL);
	pthread_mutex_init(&child_wait_all_list_c_m_lock, NULL);
	pthread_mutex_init(&sub_task_running_list_m_lock, NULL);
	pthread_mutex_init(&debug_m_lock, NULL);
	pthread_mutex_init(&waiting_schedule_array_m_lock, NULL);
	pthread_mutex_init(&sub_scheduler_count_m_lock, NULL);
	pthread_mutex_init(&local_machine_role_m_lock, NULL);
	pthread_mutex_init(&log_m_lock, NULL);
}

static void read_conf_file() {
	FILE *fp;
	int sub_cluster_num;
	int sub_cluster_machine_num;
}

static void *computation_server(void *t_arg) {
	struct server_arg_element *server_arg = (server_arg_element *) malloc(sizeof(server_arg_element));
	//TODO check malloc fail
	server_arg->msg = (char *)malloc(21);
	pthread_t tid;
	pthread_attr_t attr;
	MPI_Status status;
	int ret_v;
	int length;
	int source;

	char arg[20];

	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

	while (1) {
		//接受消息后进行处理
		MPI_Recv(arg, 20, MPI_CHAR, MPI_ANY_SOURCE, 2, MPI_COMM_WORLD, &status);

		strcpy(server_arg->msg, arg);
		server_arg->status = status;

		pthread_create(&tid, &attr, computation_server_handler, (void *) server_arg);
	}
	free(server_arg->msg);
	free(server_arg);
	pthread_attr_destroy(&attr);
}

static void *dynamic_info_get_daemon(void *arg)
{
	while(1)
	{
		get_cpu_and_network_load();
		get_memory_load();
	}

	return NULL;
}

static void *machine_heart_beat_daemon(void *arg)
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

/**
 * 从消息队列中获取消息来处理本地进程的状态，这是在一个节点内的操作
 */
static void *local_msg_daemon(void *arg)
{
	char *final;
	int msg_queue_id;
	int msg_type;
	int ret;

	msg_type = MPI_LOCAL_DEAMON_MSG_TYPE;
	msg_queue_id = msgget(MSG_QUEUE_KEY,IPC_CREAT | 0666);//设置消息队列

	while(1)
	{
		//接受消息队列中的消息？？msgsnd
		ret = msgrcv(msg_queue_id, queue_recv_msg, sizeof(long int) + (72 + 60) * sizeof(char), msg_type, 0);

		if(ret == -1)
		{
			perror("msgrecv error!\n");
			log_error("msgrecv error! local_msg_daemon\n");
			exit(1);
		}

		final = strdup(queue_recv_msg + sizeof(long int));
		printf("final_recv_msg = %s\n", final);

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

void computation_init(pthread_t tid[4]) {
	printf("computation_init and pid = %d\n", getpid());

	long int msg_queue_id;

	read_conf_file(); 					//目前没有做任何操作
	init_local_machine_status();		//初始化机器各个数据，包括带宽、内存等

	sub_cluster_id = 0;

	pthread_mutex_lock(&local_machine_role_m_lock);
	local_machine_role = UNAVAILABLE_MACHINE;
	pthread_mutex_unlock(&local_machine_role_m_lock);

	sub_scheduler_on = 0;
	additional_sub_task_count = 0;

	lists_init();

	/**
	 * 得到消息队列标识符或创建一个消息队列对象并返回消息队列标识符
	 */
	msg_queue_id = msgget(MSG_QUEUE_KEY, IPC_CREAT | 0666);

	if (msg_queue_id == -1) {
		perror("init msg queue error");
		log_error("init msg queue error\n");
		exit(1);
	}

	msgctl(msg_queue_id, IPC_RMID, 0); //将消息队列删除？？
	//sleep(8);

	//send computing machine status to master
	//local_machine_status is a global variable
	API_registration_m(local_machine_status);
	pthread_mutex_lock(&local_machine_role_m_lock);

	local_machine_role = FREE_MACHINE;

	pthread_mutex_unlock(&local_machine_role_m_lock);

	pthread_create(&tid[0], NULL, computation_server, NULL);
	puts("dynamic_info_get_daemon");
	pthread_create(&tid[1], NULL, dynamic_info_get_daemon, NULL); 	//用于动态获取信息（暂时不重要）
	puts("machine_heart_beat_daemon");
	pthread_create(&tid[2], NULL, machine_heart_beat_daemon, NULL); //心跳函数
	puts("local_msg_daemon");
	pthread_create(&tid[3], NULL, local_msg_daemon, NULL);          //暂且不管这个问题


	pthread_join(tid[0], NULL);
	pthread_join(tid[1], NULL);
	pthread_join(tid[2], NULL);
	pthread_join(tid[3], NULL);
}

