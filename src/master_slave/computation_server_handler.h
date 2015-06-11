/*
 * computation_server_handler.h
 *
 *  Created on: 2015年5月27日
 *      Author: ron
 */

#include "./structure/data.h"
#ifndef SRC_MASTER_SLAVE_COMPUTATION_SERVER_HANDLER_H_
#define SRC_MASTER_SLAVE_COMPUTATION_SERVER_HANDLER_H_

msg_t msg_type_computation(const char *msg);
void o_mpi_sub_task_finish_handler(char *arg);
void o_mpi_child_create_handler(char *arg);
void o_mpi_child_wait_all_handler(char *arg);
#endif /* SRC_MASTER_SLAVE_COMPUTATION_SERVER_HANDLER_H_ */
