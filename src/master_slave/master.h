/*
 * master.h
 *
 *  Created on: 2015年6月5日
 *      Author: ron
 */

#ifndef SRC_MASTER_SLAVE_MASTER_H_
#define SRC_MASTER_SLAVE_MASTER_H_
#include <pthread.h>

void master_init(pthread_t tid[4]);

extern void *master_server_handler(void * arg);

#endif /* SRC_MASTER_SLAVE_MASTER_H_ */
