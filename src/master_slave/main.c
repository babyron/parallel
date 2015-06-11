#include <stdio.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <strings.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <errno.h>
#include <mpi.h>
#include "./common/api.h"
#include "./common/debug.h"
#include "data_computation.h"
#include "master_scheduler.h"
#include "log.h"
#include "machine_status.h"
#include "computing_machine.h"
#include "master.h"

int main(int argc, char *argv[]) {
	int p, id;
	int t;
	int provided;
	pthread_t tid[4];
	exit_debug();

	log_before_start_up();

	MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);//从这里开始MPI可以正确运行

	printf("%d machines are provided.\n", provided);

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

	global_machine_id = id; //设定主机id
	//pthread_mutex_init(&t_tag_m_lock, NULL);//初始化

	if (id == 0) {
		master_init(tid);
	} else {
		computation_init(tid);
	}

	MPI_Finalize();
	return 0;
}

