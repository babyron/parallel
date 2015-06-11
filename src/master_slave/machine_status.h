/*
 * machine_status.h
 *
 *  Created on: 2015年5月28日
 *      Author: ron
 */

#include "./structure/data.h"

#ifndef SRC_MASTER_SLAVE_MACHINE_STATUS_H_
#define SRC_MASTER_SLAVE_MACHINE_STATUS_H_
struct machine_description_element local_machine_status;

void init_local_machine_status();
int get_version1();
int get_local_machine_CPU_core_num();
int get_local_machine_network_capacity();
int get_local_machine_memory_total();
void get_cpu_and_network_load();
void get_cpu_and_network_load_red_hat();
float calc_cpu_load(struct cpu_stat_element t1, struct cpu_stat_element t2);
void get_cpu_stat(struct cpu_stat_element *t);
void get_memory_load();
void get_memory_load_red_hat();

#endif /* SRC_MASTER_SLAVE_MACHINE_STATUS_H_ */
