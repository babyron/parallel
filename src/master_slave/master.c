/*
 * master.c
 *
 *  Created on: 2015年6月5日
 *      Author: ron
 */

#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>
#include <string.h>
#include "./structure/data.h"
#include "master.h"
#include "master_scheduler.h"

/*=======================private declaration=======================*/
static void *master_server(void *null_arg);
static void sub_cluster_init();

/*=======================private implementation=======================*/
/*初始化master_machine_num - 1 个 machine_status_array_element
 * 并置每个machine_status_array_element的machine_status为0*/
static void sub_cluster_init() {
	int i;

	master_machine_array = NULL;

	master_machine_array = (machine_status_array_element *) malloc((master_machine_num - 1) * sizeof(machine_status_array_element));

	for (i = 0; i < master_machine_num - 1; i++) {
		master_machine_array[i].machine_status = 0;
	}
}

/*
 * 该函数用来监听slave节点的数据传输请求,这个线程只用来接受传输数据的描述信息,具体对传输数据的信息
 * 的接受与处理操作新建一个新的线程来处理
 */
static void *master_server(void *null_arg) {
	printf("master_server start and process id is %d\n", getpid());
	struct server_arg_element *server_arg;
	pthread_t tid;
	pthread_attr_t attr;
	MPI_Status status;
	int ret_v;
	int source;
	int length;
	int ret;

	char arg[20];
	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
	while (1) {
		/*receive cmd message*/
		MPI_Recv(arg, 20, MPI_CHAR, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
		server_arg = (struct server_arg_element *) malloc(sizeof(struct server_arg_element));
		server_arg->msg = strdup(arg);
		server_arg->status = status;

		ret = pthread_create(&tid, &attr, master_server_handler, (void *) server_arg);
		//		ret = pthread_create(&tid, &attr, master_server_handler, (void *) server_arg);
		//
		//		if (ret != 0) {
		//			perror("master_server pthread create error");
		//			log_error("master_server pthraed create error\n");
		//			exit(1);
		//		}
		if (ret != 0) {
			perror("master_server pthread create error");
			log_error("master_server pthraed create error\n");
			exit(1);
		}
	}
	pthread_attr_destroy(&attr);
}

//*=======================API implementation=======================*/

void master_init(pthread_t tid[4]) {

	printf("parent pid = %d\n", getpid());

	sub_cluster_init();

	//使用链表来控制各个状态的作业
	pre_job_list = NULL;
	running_job_list = NULL;
	finished_job_list = NULL;
	sub_cluster_list = NULL;

	all_job_finish = 0;

	pthread_mutex_init(&pre_job_list_m_lock, NULL);
	pthread_mutex_init(&running_job_list_m_lock, NULL);
	pthread_mutex_init(&sub_cluster_list_m_lock, NULL);
	pthread_mutex_init(&master_machine_array_m_lock, NULL);
	pthread_mutex_init(&running_job_num_m_lock, NULL);
	pthread_mutex_init(&log_m_lock, NULL);

	pthread_create(&tid[0], NULL, master_scheduler, NULL); //创建一线程负责调度
	pthread_create(&tid[1], NULL, master_server, NULL);

	pthread_join(tid[0], NULL);
	pthread_join(tid[1], NULL);
}


