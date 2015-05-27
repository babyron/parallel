/*
 * dynamic_info.h
 *
 *  Created on: 2015年5月27日
 *      Author: ron
 */

#ifndef SRC_MASTER_SLAVE_DYNAMIC_INFO_H_
#define SRC_MASTER_SLAVE_DYNAMIC_INFO_H_

int version;	//0: my machine  red hat

void send_machine_heart_beat();
void send_sub_cluster_heart_beat();

void *dynamic_info_get_daemon(void *arg);
int calc_network_load(unsigned long int total_num1[16],unsigned long int total_num2[16]);
void *machine_heart_beat_daemon(void *arg);
void *sub_cluster_heart_beat_daemon(void *arg);

#endif /* SRC_MASTER_SLAVE_DYNAMIC_INFO_H_ */
