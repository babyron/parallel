/*
 * machine_status.h
 *
 *  Created on: 2015年5月28日
 *      Author: ron
 */

#ifndef SRC_MASTER_SLAVE_MACHINE_STATUS_H_
#define SRC_MASTER_SLAVE_MACHINE_STATUS_H_
struct machine_description_element local_machine_status;

void init_local_machine_status();
int get_version1();
int get_local_machine_CPU_core_num();
int get_local_machine_CPU_core_num_red_hat();
int get_local_machine_memory_total_red_hat();
int get_local_machine_network_capacity();
int get_local_machine_memory_total();

#endif /* SRC_MASTER_SLAVE_MACHINE_STATUS_H_ */
