#include "stdio.h"
#include "unistd.h"
#include "pthread.h"
#include "sys/socket.h"
#include "arpa/inet.h"
#include "strings.h"
#include "stdlib.h"
#include "string.h"
#include "../data.h"
#include "data_computation.h"
#include "../common/api.h"

struct machine_rank_array_element *machine_rank_array;
int machine_rank_array_num;

void construct_waiting_schedule_array(void);
void reset_waiting_schedule_array(void);
void array_rank(void);
void assign_sub_task(void);
void add_to_schedule_list(int index,int best_node_id);
void init_machine_rank_array();
float additional_rank_score(int waiting_schedule_array_index,int machine_id);
int select_best_node(int waiting_schedule_array_index);

/**
 * 子节点调度算法，与主节点的调度算法逻辑上基本一致
 */
void *sub_scheduler(void *arg)
{
	static count = 0;
	int i;

	pthread_setcanceltype(PTHREAD_CANCEL_DEFERRED,NULL);

	pthread_mutex_lock(&sub_scheduler_count_m_lock);
	count ++;
	pthread_mutex_unlock(&sub_scheduler_count_m_lock);

	if(count>=2)
	{
		pthread_mutex_lock(&sub_scheduler_count_m_lock);
		count --;
		pthread_mutex_unlock(&sub_scheduler_count_m_lock);

		printf("sub scheduler logic error 2 sub scheduler!\n");
		log_error("sub scheduler logic error 2 sub scheduler!\n");

		exit(1);

		return NULL;
	}

	while(1)
	{

		pthread_mutex_lock(&waiting_schedule_list_m_lock);
		pthread_mutex_lock(&waiting_schedule_array_m_lock);
		pthread_mutex_lock(&schedule_list_m_lock);

		construct_waiting_schedule_array();
		array_rank();

		assign_sub_task();
		reset_waiting_schedule_array();
		pthread_mutex_unlock(&waiting_schedule_list_m_lock);
		pthread_mutex_unlock(&waiting_schedule_array_m_lock);
		pthread_mutex_unlock(&schedule_list_m_lock);

		log_sub_master();
		sleep(1);			// do not consider scheduling time   should consider
		if(sub_scheduler_on==0)
		{
			break;
		}
	}

	pthread_mutex_lock(&sub_scheduler_count_m_lock);
	count --;
	pthread_mutex_unlock(&sub_scheduler_count_m_lock);

	return NULL;
}

void assign_sub_task(void)
{
	int best_node_id;
	int i;

	int t_flag;

	t_flag = 0;

	machine_rank_array = NULL;

	init_machine_rank_array();

	for(i=0;i<waiting_schedule_array_num;i++)
	{
		best_node_id = select_best_node(i);
//		best_node_id = 1;
		add_to_schedule_list(i,best_node_id);
	}

/*
	if(waiting_schedule_array_num>=5)
	{
		send_sub_cluster_heart_beat();
	}
*/

	free(machine_rank_array);
	machine_rank_array_num = 0;
}

void init_machine_rank_array()
{
	struct schedule_list_element *t_schedule_list;
	int i,j;

	machine_rank_array_num = sub_machine_num;
	machine_rank_array = (struct machine_rank_array_element *)malloc(machine_rank_array_num*sizeof(struct machine_rank_array_element));

	for(i=0;i<machine_rank_array_num;i++)
	{
		machine_rank_array[i].machine_id = sub_machine_array[i].machine_id;
		machine_rank_array[i].pridict_status = sub_machine_array[i].machine_description;
		machine_rank_array[i].total_sub_task_num = 0;
		for(j=0;j<2;j++)
		{
			machine_rank_array[i].priority_task_num[j] = 0;
		}
	}

//	pthread_mutex_lock(&schedule_list_m_lock);

	t_schedule_list = schedule_list;
	while(t_schedule_list!=NULL)
	{
		machine_rank_array[t_schedule_list->exe_machine_id-1].total_sub_task_num++;
		machine_rank_array[t_schedule_list->exe_machine_id-1].priority_task_num[t_schedule_list->priority-1]++;

		t_schedule_list = t_schedule_list->next;
	}

//	pthread_mutex_unlock(&schedule_list_m_lock);
}

void add_to_schedule_list(int index,int best_node_id)
{
	struct schedule_list_element *t;
	struct sub_task_exe_arg_element exe_arg;
	int i;

	t = (struct schedule_list_element *)malloc(sizeof(struct schedule_list_element));

//	pthread_mutex_lock(&waiting_schedule_array_m_lock);

	t->type = waiting_schedule_array[index].type;
	t->prime_sub_task_description = waiting_schedule_array[index].prime_sub_task_description;
	t->job_id = waiting_schedule_array[index].job_id;
	t->top_id = waiting_schedule_array[index].top_id;

	for(i=0;i<10;i++)
	{
		t->id[i] = waiting_schedule_array[index].id[i];
	}

	t->exe_machine_id = best_node_id;
	t->status = RUNNING;
	t->start_time = time(NULL);
	t->priority = waiting_schedule_array[index].priority;

//	pthread_mutex_unlock(&waiting_schedule_array_m_lock);

//	pthread_mutex_lock(&schedule_list_m_lock);

	t->next = schedule_list;
	schedule_list = t;

//	pthread_mutex_unlock(&schedule_list_m_lock);

	exe_arg.type = t->type;
	exe_arg.job_id = t->job_id;
	exe_arg.top_id = t->top_id;
	strcpy(exe_arg.arg,t->prime_sub_task_description.arg);

	for(i=0;i<10;i++)
	{
		exe_arg.id[i] = t->id[i];
	}

	API_sub_task_assign(t->prime_sub_task_description.sub_task_path,exe_arg,best_node_id);
}

int select_best_node(int waiting_schedule_array_index)
{
	struct machine_rank_array_element t;
	int weight_int[3];
	float weight[3];
	float factor;
	int i,j;

//	pthread_mutex_lock(&waiting_schedule_array_m_lock);

	weight_int[0] = waiting_schedule_array[waiting_schedule_array_index].prime_sub_task_description.weight[0];
	weight_int[1] = waiting_schedule_array[waiting_schedule_array_index].prime_sub_task_description.weight[1];
	weight_int[2] = waiting_schedule_array[waiting_schedule_array_index].prime_sub_task_description.weight[2];

	weight[0] = (float)weight_int[0]/(float)(weight_int[0]+weight_int[1]+weight_int[2]);
	weight[1] = (float)weight_int[1]/(float)(weight_int[0]+weight_int[1]+weight_int[2]);
	weight[2] = (float)weight_int[2]/(float)(weight_int[0]+weight_int[1]+weight_int[2]);

	for(i=0;i<machine_rank_array_num;i++)
	{
		machine_rank_array[i].score = (float)machine_rank_array[i].pridict_status.CPU_free*weight[0];
		machine_rank_array[i].score += (float)machine_rank_array[i].pridict_status.memory_free*weight[1];
		machine_rank_array[i].score += (float)machine_rank_array[i].pridict_status.network_free*weight[2];
		factor = additional_rank_score(waiting_schedule_array_index,machine_rank_array[i].machine_id);
		machine_rank_array[i].score *=factor;
	}

	for(i=0;i<machine_rank_array_num-1;i++)
	{
		for(j=0;j<machine_rank_array_num-1-1-i;j++)
		{
			if(machine_rank_array[j].score<machine_rank_array[j+1].score)
			{
				t = machine_rank_array[j];
				machine_rank_array[j] = machine_rank_array[j+1];
				machine_rank_array[j+1] = t;
			}
		}
	}

	machine_rank_array[0].pridict_status.CPU_free -=waiting_schedule_array[waiting_schedule_array_index].prime_sub_task_description.exe_density;
	machine_rank_array[0].pridict_status.memory_free -=waiting_schedule_array[waiting_schedule_array_index].prime_sub_task_description.memory_demand;
	machine_rank_array[0].pridict_status.network_free -=waiting_schedule_array[waiting_schedule_array_index].prime_sub_task_description.network_density;
	machine_rank_array[0].total_sub_task_num ++;
	machine_rank_array[0].priority_task_num[waiting_schedule_array[waiting_schedule_array_index].priority-1]++;

//	pthread_mutex_unlock(&waiting_schedule_array_m_lock);

	return machine_rank_array[0].machine_id;
}

float additional_rank_score(int waiting_schedule_array_index,int machine_id)
{
	struct schedule_list_element *t_schedule_list;
	float factor;
	int waiting_schedule_array_parent_id[10];
	int schedule_list_parent_id[10];
	int i;

	if(waiting_schedule_array[waiting_schedule_array_index].type==0)
	{
		return 1.0;
	}
	else
	{
		factor = 1.0;

		t_schedule_list = schedule_list;
		while(t_schedule_list!=NULL)
		{
			if((t_schedule_list->exe_machine_id==machine_id)&&(t_schedule_list->type==1))
			{
				for(i=0;i<10;i++)
				{
					schedule_list_parent_id[i] = 0;
					waiting_schedule_array_parent_id[i] = 0;
				}

				for(i=0;i<10;i++)
				{
					schedule_list_parent_id[i] = t_schedule_list->id[i];
					if(t_schedule_list->id[i]==0)
					{
						schedule_list_parent_id[i-1] = 0;
						break;
					}
				}

				for(i=0;i<10;i++)
				{
					waiting_schedule_array_parent_id[i] = waiting_schedule_array[waiting_schedule_array_index].id[i];
					if(waiting_schedule_array[waiting_schedule_array_index].id[i]==0)
					{
						waiting_schedule_array_parent_id[i-1] = 0;
						break;
					}
				}

				for(i=0;i<10;i++)
				{
					if(schedule_list_parent_id[i]!=waiting_schedule_array_parent_id[i])
					{
						break;
					}
				}

				if(i==10)
				{
					factor += 0.1;
				}
			}

			t_schedule_list = t_schedule_list->next;
		}

		return factor;
	}
}

void array_rank(void)
{
	struct waiting_schedule_list_element t;
	int i,j;

//	pthread_mutex_lock(&waiting_schedule_array_m_lock);

	for(i=0;i<waiting_schedule_array_num-1;i++)
	{
		for(j=0;j<waiting_schedule_array_num-1-1-i;j++)
		{
			if(waiting_schedule_array[j].prime_sub_task_description.exe_time<waiting_schedule_array[j+1].prime_sub_task_description.exe_time)
			{
				t = waiting_schedule_array[j];
				waiting_schedule_array[j] = waiting_schedule_array[j+1];
				waiting_schedule_array[j+1] = t;
			}
		}
	}

//	pthread_mutex_unlock(&waiting_schedule_array_m_lock);

	return;
}

void reset_waiting_schedule_array(void)
{
//	pthread_mutex_lock(&waiting_schedule_array_m_lock);

	free(waiting_schedule_array);
	waiting_schedule_array_num = 0;

//	pthread_mutex_unlock(&waiting_schedule_array_m_lock);
}

void construct_waiting_schedule_array(void)
{
	struct waiting_schedule_list_element *t_waiting_schedule_list,*t;
	int num;

	num = 0;

//	pthread_mutex_lock(&waiting_schedule_list_m_lock);
//	pthread_mutex_lock(&waiting_schedule_array_m_lock);

	waiting_schedule_array = NULL;

	t_waiting_schedule_list = waiting_schedule_list;

	while(t_waiting_schedule_list!=NULL)
	{
		num++;
		waiting_schedule_array = (struct waiting_schedule_list_element *)realloc(waiting_schedule_array,num*sizeof(struct waiting_schedule_list_element));
		waiting_schedule_array[num-1] = *t_waiting_schedule_list;
		waiting_schedule_array[num-1].next = NULL;

		t = t_waiting_schedule_list;

		t_waiting_schedule_list = t_waiting_schedule_list->next;

		free(t);
	}

	waiting_schedule_list = NULL;

	waiting_schedule_array_num = num;

//	pthread_mutex_unlock(&waiting_schedule_array_m_lock);
//	pthread_mutex_unlock(&waiting_schedule_list_m_lock);
}
