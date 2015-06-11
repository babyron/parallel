#include "time.h"
#include "./structure/data.h"

#ifndef	_DATA_COMPUTATION_H
#define _DATA_COMPUTATION_H

char master_ip[16];
volatile int sub_master_comm_id; //设置成全局变量，多个线程可以进行访问
char local_ip[16];

struct waiting_schedule_list_element *waiting_schedule_list;
struct schedule_list_element *schedule_list;
struct schedule_unit_status_list_element *schedule_unit_status_list;
struct child_wait_all_list_element *child_wait_all_list_s;
struct child_wait_all_list_element *child_wait_all_list_c;
struct sub_task_running_list_element *sub_task_running_list;
//struct machine_status_list_element *sub_machine_list;
struct machine_status_array_element *sub_machine_array;

volatile int sub_machine_num;
volatile int sub_cluster_id;
volatile int global_machine_id;
volatile int sub_machine_id;
volatile machine_role_t local_machine_role;		//0 unavailible 1 computation_node 2 sub_master
volatile int sub_scheduler_on;
volatile int additional_sub_task_count;

unsigned long int max_byte_per_prob_interval;

struct waiting_schedule_list_element *waiting_schedule_array;
int waiting_schedule_array_num;

pthread_t sub_scheduler_server_tid;
pthread_t sub_scheduler_tid;
pthread_t sub_cluster_heart_beat_daemon_tid;

pthread_mutex_t waiting_schedule_list_m_lock;
pthread_mutex_t schedule_list_m_lock;
pthread_mutex_t schedule_unit_status_list_m_lock;
pthread_mutex_t child_wait_all_list_s_m_lock;
pthread_mutex_t child_wait_all_list_c_m_lock;
pthread_mutex_t sub_task_running_list_m_lock;
pthread_mutex_t waiting_schedule_array_m_lock;
pthread_mutex_t sub_machine_array_m_lock;
pthread_mutex_t sub_scheduler_count_m_lock;

struct timeval last_machine_heart_beat_time;
struct timeval last_sub_cluster_heart_beat_time;

extern void *computation_server_handler(void *arg);
extern void *sub_scheduler(void *arg);
extern void *sub_scheduler_server_handler(void *arg);
extern void *sub_cluster_heart_beat_daemon(void *arg);
extern void send_sub_cluster_heart_beat();
extern void send_machine_heart_beat();

#endif
