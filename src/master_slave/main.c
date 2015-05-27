#define _GNU_SOURCE

#include <stdio.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <strings.h>
#include <stdlib.h>
#include <string.h>
#include <sys/msg.h>
#include <sys/types.h>
#include <errno.h>
#include <mpi.h>
#include "./structure/data.h"
#include "./common/api.h"
#include "./common/debug.h"
#include "data_computation.h"
#include "log.h"

void master_init();
void *master_server(void *null_arg);
void *master_scheduler(void *arg);
void sub_cluster_init();
extern void *master_server_handler(void *arg);
//extern void *load_generater(void *);
extern void *local_msg_daemon(void *arg);

void *computation_server(void *arg);
void computation_init();
void read_conf_file();
void init_local_machine_status();
void lists_init(void);
int get_local_machine_CPU_core_num();
int get_version1();
int get_local_machine_CPU_core_num_red_hat();
int get_local_machine_memory_total_red_hat();
void factors_init();
int get_local_machine_network_capacity();
int get_local_machine_memory_total();

int main(int argc, char *argv[]) {
	int p, id;
	int t;

	int provided;

	pthread_t tid[4];

	exit_debug();

	log_before_start_up();

	MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);//从这里开始MPI可以正确运行

	printf("%d machines are provided.", provided);

	if (provided < MPI_THREAD_MULTIPLE) {
		printf("MPI cannot support mutiple\n");
		/**
		 * 终止MPI运行环境
		 */
		MPI_Abort(MPI_COMM_WORLD, 0);
	}

	log_after_start_up();
	MPI_Comm_size(MPI_COMM_WORLD, &master_machine_num);
	MPI_Comm_rank(MPI_COMM_WORLD, &id);

	log_record_start_up();

	/*进程组中进程数过少*/
	if (master_machine_num < 2) {
		printf("too few machines");
		MPI_Finalize();
		return 1;
	}

	global_machine_id = id;//设定主机id
	//pthread_mutex_init(&t_tag_m_lock, NULL);//初始化

	if (id == 0) {
		master_init();

		printf("parent pid = %d\n", getpid());
		pthread_create(&tid[0], NULL, master_scheduler, NULL);//创建一线程负责调度
		pthread_create(&tid[1], NULL, master_server, NULL);

		pthread_join(tid[0], NULL);
		pthread_join(tid[1], NULL);
	} else {
		computation_init();

		printf("child pid = %d\n", getpid());
//		sleep(8);

		API_registration_m(local_machine_status);//local_machine_status is a global variable
		//由于使用MPI在每个MPI进程互不影响

		pthread_mutex_lock(&local_machine_role_m_lock);

		local_machine_role = FREE_MACHINE;

		pthread_mutex_unlock(&local_machine_role_m_lock);

		/**
		 * 0. 计算进程
		 * 1.
		 */
		//在非主节点中发起四个线程，一个计算服务进程，三个守护进程。
		pthread_create(&tid[0], NULL, computation_server, NULL);
		pthread_create(&tid[1], NULL, dynamic_info_get_daemon, NULL);//用于动态获取信息（暂时不重要）
		pthread_create(&tid[2], NULL, machine_heart_beat_daemon, NULL);//心跳函数
		pthread_create(&tid[3], NULL, local_msg_daemon, NULL);//暂且不管这个问题

		pthread_join(tid[0], NULL);
		pthread_join(tid[1], NULL);
		pthread_join(tid[2], NULL);
		pthread_join(tid[3], NULL);
	}

	MPI_Finalize();

	return 0;
}

void master_init() {
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
}


/*初始化master_machine_num - 1 个 machine_status_array_element
 * 并置每个machine_status_array_element的machine_status为0*/
void sub_cluster_init() {
	int i;

	master_machine_array = NULL;

	master_machine_array = (struct machine_status_array_element *) malloc((master_machine_num - 1) * sizeof(struct machine_status_array_element));

	for (i = 0; i < master_machine_num - 1; i++) {
		master_machine_array[i].machine_status = 0;
	}
}

/*
 * 该函数用来监听slave节点的数据传输请求,这个线程只用来接受传输数据的描述信息,具体对传输数据的信息
 * 的接受与处理操作新建一个新的线程来处理
 */
void * master_server(void * null_arg) {
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
		//循环接收slave节点所发出的消息
		MPI_Recv(arg, 20, MPI_CHAR, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
		//printf("master receive msg === %s\n", arg);
		server_arg = (struct server_arg_element *) malloc(sizeof(struct server_arg_element));
		server_arg->msg = strdup(arg);
		server_arg->status = status;

		ret = pthread_create(&tid, &attr, master_server_handler, (void *) server_arg);

		if (ret != 0) {
			perror("master_server pthread create error");
			log_error("master_server pthraed create error\n");
			exit(1);
		}

	}

	pthread_attr_destroy(&attr);
}

void computation_init() {
	long int msg_queue_id;

	read_conf_file();//目前没有做任何操作
	init_local_machine_status();//初始化机器各个数据，包括带宽、内存等

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

	msgctl(msg_queue_id, IPC_RMID, 0);//将消息队列删除？？
}
/**
 * 各种链表初始化
 */
void lists_init(void) {
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

void init_local_machine_status() {
	local_machine_status.CPU_core_num = get_local_machine_CPU_core_num();
	local_machine_status.GPU_core_num = 5;
	local_machine_status.CPU_free = 1000;
	local_machine_status.GPU_load = 0;
	local_machine_status.memory_free = 1000;
	local_machine_status.network_free = 0;
	local_machine_status.IO_bus_capacity = 100;
	local_machine_status.network_capacity = get_local_machine_network_capacity();
	local_machine_status.memory_total = get_local_machine_memory_total();
	local_machine_status.memory_swap = 0;

	factors_init();
}

void factors_init() {
	CPU_free_factor = local_machine_status.CPU_core_num;
	memory_free_factor = (float) local_machine_status.memory_total
			/ (1024.0 * 1.5 / 10.0);	//1.5G
	network_free_factor = (float) local_machine_status.network_capacity
			/ (100.0 * 1024.0 / 8.0);	//100mb
	if (network_free_factor == 0) {
		printf("unknown network capacity! default factor = 1.0\n");
		network_free_factor = 1.0;
	}
}

int get_local_machine_network_capacity() {
	FILE *fp;
	char cmd[40];
	char *line;
	char *save_ptr;
	char *parameter;
	size_t len;
	int speed;
	int ret;

	speed = 100;
	return (speed * 1024) / 8;
}

int get_local_machine_memory_total() {
	int version;

	version = get_version1();

	if (version == 0) {
		return get_local_machine_memory_total_red_hat();
	} else {
		printf("unknown version");
		log_error("unknown version");
		exit(1);
		return -1;
	}
}

int get_local_machine_memory_total_red_hat() {
	FILE *fp;
	char *line;
	char sub[20];
	unsigned long int memtotal;
	int memtotal_int;
	size_t len;
	int ret;

	fp = fopen("/proc/meminfo", "r");

	while (1) {
		line = NULL;
		ret = getline(&line, &len, fp);
		if (ret == -1) {
			free(line);
			break;
		} else {
			if (strstr(line, "MemTotal")) {
				sscanf(line, "%s %ld", sub, &memtotal);
				free(line);
				break;
			}
			free(line);
		}
	}

	memtotal_int = memtotal / 1024 / 10;
	fclose(fp);

	return memtotal_int;
}

int get_local_machine_CPU_core_num() {
	int version;

	version = get_version1();//??何用
	if (version == 0) {
		return get_local_machine_CPU_core_num_red_hat();
	} else {
		printf("unknown version");
		log_error("unknown version");
		exit(1);
		return -1;
	}
}

int get_local_machine_CPU_core_num_red_hat() {
	FILE *fp;
	char *line;
	size_t len;
	int core_num;
	size_t ret;

	fp = fopen("/proc/stat", "r");

	core_num = 0;

	while (1) {
		line = NULL;
		ret = getline(&line, &len, fp);
		if (ret == -1) {
			free(line);
			break;
		} else {
			if (strstr(line, "cpu")) {
				core_num++;
			}
			free(line);
		}
	}

	core_num--;
	fclose(fp);

	return core_num;
}

int get_version1() {
	return 0;
}

void read_conf_file() {
	FILE *fp;
	int sub_cluster_num;
	int sub_cluster_machine_num;
}

void *computation_server(void *t_arg) {
	struct server_arg_element *server_arg;
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

		server_arg = NULL;
		server_arg = (struct server_arg_element *) malloc(sizeof(struct server_arg_element));
		server_arg->msg = strdup(arg);
		server_arg->status = status;

		pthread_create(&tid, &attr, computation_server_handler, (void *) server_arg);
	}

	pthread_attr_destroy(&attr);
}
