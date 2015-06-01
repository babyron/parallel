/*
 * computing_machine.c
 *
 *  Created on: 2015年5月29日
 *      Author: ron
 */



/*
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
*/
