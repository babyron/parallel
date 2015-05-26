//#include <stdio.h>
//#include "unistd.h"
//#include "pthread.h"
//#include "sys/socket.h"
//#include "arpa/inet.h"
//#include "strings.h"
//#include "stdlib.h"
//#include "string.h"
#include "../data.h"
//#include "communication.h"

#ifndef	_API_H
#define	_API_H

void fill_schedule_unit_assign_msg(struct schedule_unit_description_element schedule_unit,char *send_msg,int num,char *priority_modified_msg);
long int get_msg_type(int type, int job_id, int top_id, int id[10]);
struct child_wait_all_list_element *find_element_in_child_wait_all_list_c(int type,int job_id,int top_id,int id[10]);
void delete_element_from_child_wait_all_list_c(struct child_wait_all_list_element *t);
struct child_wait_all_list_element *find_element_in_child_wait_all_list_s(int type,int job_id,int top_id,int id[10]);
struct child_wait_all_list_element *find_element_in_child_wait_all_list_c(int type,int job_id,int top_id,int id[10]);
void delete_element_from_child_wait_all_list_s(struct child_wait_all_list_element *t);
struct sub_cluster_status_list_element *get_sub_cluster_element(int sub_cluster_id);
void check_modified_priority(struct sub_cluster_status_list_element *list,int *num,char **ret_char);
void check_modified_priority_without_lock(struct sub_cluster_status_list_element *list,int *num,char **ret_char);
void fill_modified_priority_msg(struct schedule_unit_priority_list_element *t,int new_priority,char *tt);
int master_get_sub_task_priority(int type,int job_id,int top_id,int *parent_id);
struct sub_cluster_status_list_element *get_sub_cluster_element_without_lock(int sub_cluster_id);
int sub_cluster_heart_beat_data_available();
int sub_find_machine_comm_id(int sub_machine_id);

extern void API_sub_scheduler_assign(struct sub_cluster_status_list_element *t);
extern void API_computation_node_assign(int machine_id);
extern int API_registration_m(struct machine_description_element local_machine_status);
extern int API_job_submit(char *master_ip,char *job_path);
extern int API_schedule_unit_assign(struct schedule_unit_description_element schedule_unit,int sub_cluster_id);
extern int API_sub_task_assign(char *path,struct sub_task_exe_arg_element exe_arg,int best_node_id);
extern int API_registration_s(int sub_master_id,struct machine_description_element local_machine_status);
extern int API_sub_task_finish_c_to_s(char *arg);
extern void API_schedule_unit_finish(int type,int schedule_unit_num,int job_id,int top_id,char *arg,int **ids,char **args);
extern int API_child_create_c_to_s(char *arg);
extern int API_child_create_s_to_m(char *arg);
extern int API_child_wake_up_all_m_to_s(struct child_wait_all_list_element *t,int child_num,char **args);
extern int API_child_wake_up_all_c_to_p(int type,int job_id,int top_id,int id[10],char *ret_arg);
extern int API_child_wait_all_c_to_s(char *arg);
extern int API_get_sub_task_ip_s_to_m(char *send_msg,char *parent_ip);
extern int API_get_sub_task_ip_c_to_s(char *send_msg,char *parent_ip);
extern int API_get_sub_task_ip_m_to_s(char *send_msg,char *sub_task_ip);
extern int API_sub_cluster_destroy(int sub_master_id);
extern int API_sub_cluster_shut_down(int sub_master_id);
extern int API_machine_heart_beat();
extern int API_sub_cluster_heart_beat();

int master_find_machine_id(int comm_source);
//extern int master_find_machine_id(struct sockaddr client_addr);
extern int sub_find_machine_id(int comm_id);
//extern long int get_msg_type(char **arg);
extern struct sub_cluster_status_list_element *get_sub_cluster_element(int sub_cluster_id);
extern struct sub_cluster_status_list_element *get_sub_cluster_element_through_sub_master_id(int sub_master_id);
extern int master_get_sub_task_priority(int type,int job_id,int top_id,int *parent_id);
extern int master_get_sub_task_priority_without_lock(int type,int job_id,int top_id,int *parent_id);
extern struct sub_cluster_status_list_element *get_sub_cluster_element_through_sub_master_id(int sub_master_id);
extern int API_back_to_main_master(int comm_source);
extern int API_child_wake_up_all_s_to_c(int type,int job_id,int top_id,int id[10],char *ret_arg);
extern int API_child_wait_all_s_to_m(char *arg);

extern void itoa(char *str, int num);
extern char *ltoa(int num);
#endif
