#include <stdio.h>
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <strings.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <pthread.h>
#include "./structure/data.h"
#include "./common/api.h"

msg_t msg_type(char *msg);
void sub_cluster_heart_beat_handler(int comm_source,int ack_tag,char *arg);
void registration_m_handler(int comm_source,int ack_tag,char *arg);
void job_submit_handler(int comm_source,int ack_tag,char *arg);
void auto_job_submit(char *arg);
struct prime_job_description_element *load_job(char *path);
void load_normal_sub_task_description_element(FILE *fp,struct normal_sub_task_description_element *arg,int normal_sub_task_id);
void delay_policy();
void schedule_unit_finish_handler(int comm_source,int ack_tag,char *arg);
void move_to_finished_job_list(struct job_description_element *t);
void fill_next_arg(int job_id,int top_id,char *ret_arg);
void child_create_s_to_m_handler(int comm_source,int ack_tag,char *arg);
void child_wait_all_s_to_m_handler(int comm_source,int ack_tag,char *arg);
void child_create(int type,int job_id,int top_id,int id[10],int child_num,char **child_arg);
struct sub_pack_description_tree_element *find_root(int type,int job_id,int top_id,int id[10]);
struct parallel_sub_task_abstract_element *find_abstract(int job_id,int top_id);
void fill_root_data(struct sub_pack_description_tree_element *root,int child_num,char **child_arg);
void fill_tree_data(struct sub_pack_description_tree_element *root,struct parallel_sub_task_abstract_element * abstract_array,int parent_id[10],int sub_id,int b_level,int child_num,char *child_arg,int priority);
void check_waiting_sub_task(int job_id);
int try_wake_up_job(struct child_wait_all_list_element *t,int job_id);
int try_wake_up_normal_sub_task(struct job_description_element *t,int top_id,int *child_num,char ***ret_args);
int try_wake_up_sub_pack(struct job_description_element *t,int id[10],int *child_num,char ***ret_args);
void change_sub_task_status_to_finish_fill_ret_args(int type,int job_id,int top_id,char *ret_arg,int id[10],char *ret_args);
void delete_element_from_child_wait_all_list_m(struct child_wait_all_list_element *t);
//void get_sub_task_ip_s_to_m_handler(int comm_source,int ack_tag,char *arg);
int get_sub_cluster_id(int type,int job_id,int top_id,int id[10]);
void destory_sub_cluster(struct sub_cluster_status_list_element *t);
int can_destory_sub_cluster(struct sub_cluster_status_list_element *t);
void fill_sub_machine_status(char *arg,struct sub_cluster_status_list_element *t);
void machine_heart_beat_m_handler(int comm_source,int ack_tag,char *arg);
void delete_element_from_schedule_unit_priority_list(struct sub_cluster_status_list_element *list,int type,int num,int job_id,int top_id,int *first_ids);
int is_job_finished(struct job_description_element *t);

/*
 * 对于不同的的请求分配给不同的响应
 */
void *master_server_handler(void * arg)
{
	struct server_arg_element *server_arg;
	int comm_source, length,comm_tag, ack_tag;
	char *parameter;
	char *save_ptr;
	char *final;

	MPI_Status status;

	server_arg = (struct server_arg_element *)arg;

	parameter = strtok_r(server_arg->msg, ";", &save_ptr);	//comm_tag;length;ack_tag
	comm_tag = atoi(parameter);

	parameter = strtok_r(NULL, ";", &save_ptr);
	length = atoi(parameter);

	parameter = strtok_r(NULL, ";", &save_ptr);
	ack_tag = atoi(parameter);

	comm_source = server_arg->status.MPI_SOURCE;

	final = (char *)malloc(length);

	MPI_Recv(final, length, MPI_CHAR, comm_source, comm_tag, MPI_COMM_WORLD, &status);

	printf("master recv %s!\n", final);

	switch(msg_type(final))
	{
		case SUB_CLUSTER_HEART_BEAT:
			printf("msg : sub cluster heart beat\n");
			sub_cluster_heart_beat_handler(comm_source, ack_tag, final);
			break;
		case JOB_SUBMIT:
			printf("msg : job submit\n");
			job_submit_handler(comm_source, ack_tag, final);
			break;
		case SCHEDULE_UNIT_FINISH:
			printf("msg : schedule unit finish\n");
			schedule_unit_finish_handler(comm_source, ack_tag, final);
			break;
		case REGISTRATION_M://机器注册，读取作业信息，这是进行作业的第一步
			printf("msg : registration_m\n");
			registration_m_handler(comm_source, ack_tag, final);
			break;
		case CHILD_CREATE:
			printf("msg : child create s_to_m\n");
			child_create_s_to_m_handler(comm_source, ack_tag, final);
			break;
		case CHILD_WAIT_ALL:
			printf("msg : child wait all s_to_m\n");
			child_wait_all_s_to_m_handler(comm_source, ack_tag, final);
			break;
/*			
		case GET_SUB_TASK_IP:
			printf("msg : get sub_task ip s_to_m\n");
			get_sub_task_ip_s_to_m_handler(comm_source,ack_tag,final);
			break;
*/
		case MACHINE_HEART_BEAT:
			printf("msg : machine heart beat_m\n");
			machine_heart_beat_m_handler(comm_source, ack_tag, final);
			break;
		default:
			printf("unknown msg\n");
			printf("msg is %s!\n", final);
			log_error("master unknown msg\n");
			exit(1);
	}

	free(final);
	free(server_arg->msg);
	free(server_arg);

	return NULL;
}

void machine_heart_beat_m_handler(int comm_source,int ack_tag,char *arg)
{
	char *t_arg;
	char *parameter;
	char *save_ptr;
	int machine_id;
	int i;

	i = 0;
	while(arg[i]!=';')
	{
		i++;
	}
	i++;

	t_arg = strdup(arg+i);

	send_ack_msg(comm_source,ack_tag,"");

	machine_id = comm_source;

	parameter = strtok_r(t_arg,",",&save_ptr);
	master_machine_array[machine_id-1].machine_description.CPU_free;

	parameter = strtok_r(NULL,",",&save_ptr);
	master_machine_array[machine_id-1].machine_description.GPU_load;

	parameter = strtok_r(NULL,",",&save_ptr);
	master_machine_array[machine_id-1].machine_description.memory_free;

	parameter = strtok_r(NULL,",",&save_ptr);
	master_machine_array[machine_id-1].machine_description.network_free;

	free(t_arg);
}

/*
void get_sub_task_ip_s_to_m_handler(int comm_source,int ack_tag,char *arg)
{
	struct sub_cluster_status_list_element *t;
	char *t_arg;
	char *tt_arg;
	char *reply_msg;
	char *save_ptr;
	char *parameter;
	char ret_ip[16];
	char sub_master_ip[16];
	int type;
	int job_id;
	int top_id;
	int id[10];
	int sub_cluster_id;
	int i;

	i = 0;
	while(arg[i]!=';')
	{
		i++;
	}
	i++;

	t_arg = strdup(arg+i);


	parameter = strtok_r(t_arg,",",&save_ptr);
	type = atoi(parameter);

	parameter = strtok_r(NULL,",",&save_ptr);
	job_id = atoi(parameter);

	parameter = strtok_r(NULL,",",&save_ptr);
	top_id = atoi(parameter);

	for(i=0;i<10;i++)
	{
		parameter = strtok_r(NULL,"_",&save_ptr);
		id[i] = atoi(parameter);
	}

	sub_cluster_id = get_sub_cluster_id(type,job_id,top_id,id);
	if(sub_cluster_id!=0)
	{
		tt_arg = strdup(t_arg);
		t = get_sub_cluster_element(sub_cluster_id);
		master_get_machine_ip(t->sub_master_id,sub_master_ip);
		API_get_sub_task_ip_m_to_s(tt_arg,ret_ip);
		reply_msg = strdup(ret_ip);
		free(tt_arg);
	}
	else
	{
		reply_msg = strdup("0.0.0.0");
	}

	send_ack_msg(comm_source,ack_tag,reply_msg);

	free(reply_msg);
	free(t_arg);
}
*/

int get_sub_cluster_id(int type,int job_id,int top_id,int id[10])
{
	struct job_description_element *t_running_job_list;
	struct sub_pack_description_tree_element *root;
	int ret;
	int p;

	ret = 0;

	pthread_mutex_lock(&running_job_list_m_lock);

	t_running_job_list = running_job_list;
	while(t_running_job_list!=NULL)
	{
		if(t_running_job_list->job_id==job_id)
		{
			if(type==0)
			{
				ret = t_running_job_list->job.normal_sub_task_description_array[top_id-1].sub_cluster_id;
				break;
			}
			else
			{
				root = t_running_job_list->job.normal_sub_task_description_array[id[0]-1].root;
				p = 1;
				while(id[p]!=0)
				{
					if(root->parallel_sub_task_description.child_num<=id[p])
					{
						root = &(root->sub_pack_description_tree[id[p]-1]);
						p++;
					}
					else
					{
						ret = 0;
						break;
					}
				}
				ret = root->parallel_sub_task_description.sub_cluster_id;
				break;
			}
		}

		t_running_job_list = t_running_job_list->next;
	}

	pthread_mutex_unlock(&running_job_list_m_lock);

	return ret;
}

void child_wait_all_s_to_m_handler(int comm_source,int ack_tag,char *arg)
{
	struct child_wait_all_list_element *t;
	char *t_arg;
	char *parameter;
	char *save_ptr;
	int i;

	t = (struct child_wait_all_list_element *)malloc(sizeof(struct child_wait_all_list_element));
	i = 0;
	while(arg[i]!=';')
	{
		i++;
	}
	i++;

	t_arg = strdup(arg+i);

	send_ack_msg(comm_source,ack_tag,"");

	parameter = strtok_r(t_arg,",",&save_ptr);
	t->type = atoi(parameter);

	parameter = strtok_r(NULL,",",&save_ptr);
	t->job_id = atoi(parameter);

	parameter = strtok_r(NULL,",",&save_ptr);
	t->top_id = atoi(parameter);

	for(i=0;i<10;i++)
	{
		parameter = strtok_r(NULL,"_",&save_ptr);
		t->id[i] = atoi(parameter);
	}

	parameter = strtok_r(NULL,",",&save_ptr);
	t->sub_cluster_id = atoi(parameter);

	t->next = NULL;

	pthread_mutex_lock(&child_wait_all_list_m_m_lock);

	t->next = child_wait_all_list_m;
	child_wait_all_list_m = t;

	pthread_mutex_unlock(&child_wait_all_list_m_m_lock);

	free(t_arg);
}

void child_create_s_to_m_handler(int comm_source,int ack_tag,char *arg)
{
	int type;
	int job_id;
	int top_id;
	int id[10];
	int child_num;
	char **child_arg;
	char *t_arg;
	char *parameter;
	char *save_ptr;
	int i;

	i = 0;
	while(arg[i]!=';')
	{
		i++;
	}
	i++;

	t_arg = strdup(arg+i);

	send_ack_msg(comm_source,ack_tag,"");

	printf("child create t_arg = %s\n",t_arg);

	parameter = strtok_r(t_arg,",",&save_ptr);
	type = atoi(parameter);

	parameter = strtok_r(NULL,",",&save_ptr);
	job_id = atoi(parameter);

	parameter = strtok_r(NULL,",",&save_ptr);
	top_id = atoi(parameter);

	for(i=0;i<10;i++)
	{
		parameter = strtok_r(NULL,"_",&save_ptr);
		id[i] = atoi(parameter);
	}

	parameter = strtok_r(NULL,",",&save_ptr);
	child_num = atoi(parameter);

	assert(child_num!=0);

	child_arg = (char **)malloc(child_num*sizeof(char *));
	for(i=0;i<child_num;i++)
	{
		child_arg[i] = (char *)malloc(64*sizeof(char));
	}

	for(i=0;i<child_num;i++)
	{
		parameter = strtok_r(NULL,",",&save_ptr);
		strcpy(child_arg[i],parameter);
	}

	child_create(type,job_id,top_id,id,child_num,child_arg);

	for(i=0;i<child_num;i++)
	{
		free(child_arg[i]);
	}
	free(child_arg);

	free(t_arg);
}

void child_create(int type,int job_id,int top_id,int id[10],int child_num,char **child_arg)
{
	struct sub_pack_description_tree_element *root;
	struct parallel_sub_task_abstract_element *abstract;
	int b_level;
	int i;

	root = find_root(type,job_id,top_id,id);
	if(type==0)
	{
		abstract = find_abstract(job_id,top_id);
	}
	else
	{
		abstract = find_abstract(job_id,id[0]);
	}

	pthread_mutex_lock(&running_job_list_m_lock);

	fill_root_data(root,child_num,child_arg);

	b_level = root->sub_pack_b_level-1;

	assert(b_level>=0);
	root->sub_pack_description_tree = (struct sub_pack_description_tree_element *)malloc(child_num*sizeof(struct sub_pack_description_tree_element));

	if(type==0)
	{
		for(i=0;i<10;i++)
		{
			id[i] = 0;
		}
	}
	id[0] = top_id;

//		pack all sub task in one pack and cannot be splited into several parts
//		if want to modify:	add the start id arg
	for(i=0;i<child_num;i++)
	{
		fill_tree_data(&(root->sub_pack_description_tree[i]),abstract,id,i+1,b_level,child_num,child_arg[i],root->parallel_sub_task_description.priority);
	}

	pthread_mutex_unlock(&running_job_list_m_lock);
}

void fill_root_data(struct sub_pack_description_tree_element *root,int child_num,char **child_arg)
{
	int i;

	root->child_created = 1;
	root->parallel_sub_task_description.child_num = child_num;
	root->parallel_sub_task_description.child_args = (char **)malloc(child_num*sizeof(char *));
	root->parallel_sub_task_description.child_ret_args = (char **)malloc(child_num*sizeof(char *));
	for(i=0;i<child_num;i++)
	{
		root->parallel_sub_task_description.child_args[i] = (char *)malloc(64*sizeof(char));
	}

	for(i=0;i<child_num;i++)
	{
		strcpy(root->parallel_sub_task_description.child_args[i],child_arg[i]);
	}

}

void fill_tree_data(struct sub_pack_description_tree_element *root,struct parallel_sub_task_abstract_element * abstract_array,int parent_id[10],int sub_id,int b_level,int child_num,char *child_arg,int priority)
{
	int t_level;
	int i;

	t_level = 0;
	while(parent_id[t_level]!=0)
	{
		t_level++;
	}

	if(t_level>9)
	{
		printf("fill_tree_data error:only support sub-task at most 10 level!\n");
		log_error("fill_tree_date error:only support sub-task at most 10 level!\n");
		exit(1);
	}

	root->sub_pack_b_level = b_level;
	root->child_created = 0;
	root->parallel_sub_task_description.prime_sub_task_description = abstract_array[t_level].prime_sub_task_description;
	strcpy(root->parallel_sub_task_description.prime_sub_task_description.arg,child_arg);

	for(i=0;i<10;i++)
	{
		root->parallel_sub_task_description.id[i] = 0;
	}

	for(i=0;i<t_level;i++)
	{
		root->parallel_sub_task_description.id[i] = parent_id[i];
	}

	root->parallel_sub_task_description.id[i] = sub_id;
	root->parallel_sub_task_description.sibling_num = child_num;
	root->parallel_sub_task_description.child_num = 0;
	root->parallel_sub_task_description.child_args = NULL;
	root->parallel_sub_task_description.child_ret_args = NULL;

	root->parallel_sub_task_description.inclusive_data_transfer_amount = abstract_array[t_level].inclusive_data_transfer_amount;
	root->parallel_sub_task_description.data_out_amount = abstract_array[t_level].data_out_amount;
	root->parallel_sub_task_description.data_in_amount = abstract_array[t_level].data_in_amount;
	root->parallel_sub_task_description.data_out_type = abstract_array[t_level].data_out_type;
	root->parallel_sub_task_description.data_in_type = abstract_array[t_level].data_in_type;
	root->parallel_sub_task_description.data_back_amount = abstract_array[t_level].data_back_amount;
	root->parallel_sub_task_description.data_back_type = abstract_array[t_level].data_back_type;
	root->parallel_sub_task_description.machine_num = abstract_array[t_level].machine_num;
	root->parallel_sub_task_description.status = CREATED;
	root->parallel_sub_task_description.priority = priority;
}

struct parallel_sub_task_abstract_element *find_abstract(int job_id,int top_id)
{
	struct job_description_element *t_running_job_list;

	pthread_mutex_lock(&running_job_list_m_lock);

	t_running_job_list = running_job_list;
	while(t_running_job_list!=NULL)
	{
		if(t_running_job_list->job_id==job_id)
		{
			break;
		}
		t_running_job_list = t_running_job_list->next;
	}

	assert(t_running_job_list!=NULL);

	pthread_mutex_unlock(&running_job_list_m_lock);

	return t_running_job_list->job.normal_sub_task_description_array[top_id-1].parallel_sub_task_abstract_array;

}

struct sub_pack_description_tree_element *find_root(int type,int job_id,int top_id,int id[10])
{
	struct job_description_element *t_running_job_list;
	struct sub_pack_description_tree_element *root;
	int p;

	assert((type==0)||(type==1));
	if(type==0)
	{
		pthread_mutex_lock(&running_job_list_m_lock);

		t_running_job_list = running_job_list;
		while(t_running_job_list!=NULL)
		{
			if(t_running_job_list->job_id==job_id)
			{
				break;
			}
			t_running_job_list = t_running_job_list->next;
		}

		assert(t_running_job_list!=NULL);

		pthread_mutex_unlock(&running_job_list_m_lock);

		return t_running_job_list->job.normal_sub_task_description_array[top_id-1].root;
	}
	else
	{
		pthread_mutex_lock(&running_job_list_m_lock);

		t_running_job_list = running_job_list;
		while(t_running_job_list!=NULL)
		{
			if(t_running_job_list->job_id==job_id)
			{
				break;
			}
			t_running_job_list = t_running_job_list->next;
		}

		assert(t_running_job_list!=NULL);

		root = t_running_job_list->job.normal_sub_task_description_array[id[0]-1].root;

		p = 1;
		while(id[p]!=0)
		{
			root = &(root->sub_pack_description_tree[id[p]-1]);
		}

		pthread_mutex_unlock(&running_job_list_m_lock);

		return root;
	}
}

void schedule_unit_finish_handler(int comm_source,int ack_tag,char *arg)
{
	struct sub_cluster_status_list_element *t_sub_cluster_list;
	struct schedule_unit_priority_list_element *t_schedule_unit_priority_list;
	char *t_arg;
	char *tt_arg;
	char *parameter;
	int type;
	int schedule_unit_num;
	int job_id;
	int top_id;
	int **ids;
	int sub_master_id;
	int first_ids[10];
	char *ret_arg;
	char **ret_args;
	char *save_ptr;
	int i,j;

	i = 0;
	while(arg[i]!=';')
	{
		i++;
	}

	i++;

	t_arg = strdup(arg+i);

	send_ack_msg(comm_source,ack_tag,"");

	printf("recv arg = %s\n",t_arg);

	parameter = strtok_r(t_arg,",",&save_ptr);
	type = atoi(parameter);

	assert((type==0)||(type==1));

	parameter = strtok_r(NULL,",",&save_ptr);
	schedule_unit_num = atoi(parameter);

	parameter = strtok_r(NULL,",",&save_ptr);
	job_id = atoi(parameter);

	top_id = 0;
	ids = NULL;
	ret_arg = NULL;

	if(type==0)
	{
		parameter = strtok_r(NULL,",",&save_ptr);
		top_id = atoi(parameter);

		parameter = strtok_r(NULL,",",&save_ptr);
		ret_arg = strdup(parameter);
	}
	else
	{
		ids = (int **)malloc(schedule_unit_num*sizeof(int *));

		for(i=0;i<schedule_unit_num;i++)
		{
			ids[i] = (int *)malloc(10*sizeof(int));
		}

		for(i=0;i<schedule_unit_num;i++)
		{
			for(j=0;j<10;j++)
			{
				parameter = strtok_r(NULL,"_",&save_ptr);
				ids[i][j] = atoi(parameter);
			}
		}

		tt_arg = strdup(strtok_r(NULL,",",&save_ptr));

		ret_args = (char **)malloc(schedule_unit_num*sizeof(char *));

		ret_args[0] = strdup(strtok_r(tt_arg,"_",&save_ptr));
		for(i=1;i<schedule_unit_num;i++)
		{
			ret_args[1] = strdup(strtok_r(NULL,"_",&save_ptr));
		}
		free(tt_arg);
	}


	pthread_mutex_lock(&sub_cluster_list_m_lock);
	pthread_mutex_lock(&running_job_list_m_lock);

	if(type==0)
	{
		change_sub_task_status_to_finish_fill_ret_args(type,job_id,top_id,ret_arg,NULL,NULL);
		free(ret_arg);
	}
	else
	{
		for(i=0;i<10;i++)
		{
			first_ids[i] = ids[0][i];
		}

		for(i=0;i<schedule_unit_num;i++)
		{
			change_sub_task_status_to_finish_fill_ret_args(type,job_id,top_id,NULL,ids[i],ret_args[i]);
		}

		for(i=0;i<schedule_unit_num;i++)
		{
			free(ids[i]);
			free(ret_args[i]);
		}

		free(ids);
		free(ret_args);
	}


	check_waiting_sub_task(job_id);

	sub_master_id = comm_source;

	t_sub_cluster_list = sub_cluster_list;
	while(t_sub_cluster_list!=NULL)
	{
		if(t_sub_cluster_list->sub_master_id==sub_master_id)
		{
			break;
		}

		t_sub_cluster_list = t_sub_cluster_list->next;
	}

	assert(t_sub_cluster_list!=NULL);

	if(type==0)
	{
		delete_element_from_schedule_unit_priority_list(t_sub_cluster_list,0,1,job_id,top_id,NULL);
	}
	else
	{
		delete_element_from_schedule_unit_priority_list(t_sub_cluster_list,1,schedule_unit_num,job_id,0,first_ids);
	}

	t_sub_cluster_list->schedule_unit_count--;
	if(t_sub_cluster_list->schedule_unit_count==0)
	{
		if(can_destory_sub_cluster(t_sub_cluster_list)==1) //lock master_machine_array
		{
			destory_sub_cluster(t_sub_cluster_list);
		}
	}

	pthread_mutex_unlock(&sub_cluster_list_m_lock);
	pthread_mutex_unlock(&running_job_list_m_lock);

	free(t_arg);

/*
//	if(job_id==1&&top_id==4)
	{
		t_sub_cluster_list = sub_cluster_list;

		while(t_sub_cluster_list!=NULL)
		{
			t_schedule_unit_priority_list = t_sub_cluster_list->schedule_unit_priority_list;
			while(t_schedule_unit_priority_list!=NULL)
			{
				if((type==0)&&(type==t_schedule_unit_priority_list->schedule_unit_type)&&(t_schedule_unit_priority_list->job_id==job_id)&&(t_schedule_unit_priority_list->top_id==top_id))
				{
					printf(" 1 , 4 is not delete!!!!!!\n");
					exit(1);
				}
				t_schedule_unit_priority_list = t_schedule_unit_priority_list->next;
			}
			t_sub_cluster_list = t_sub_cluster_list->next;
		}
	}
	*/
}

void delete_element_from_schedule_unit_priority_list(struct sub_cluster_status_list_element *list,int type,int num,int job_id,int top_id,int *first_ids)
{
	struct schedule_unit_priority_list_element *t_schedule_unit_priority_list,*t;
	int parent_id[10];
	int i;

	t = NULL;

	t_schedule_unit_priority_list = list->schedule_unit_priority_list;

	while(t_schedule_unit_priority_list!=NULL)
	{
		if((t_schedule_unit_priority_list->schedule_unit_type==type)&&(t_schedule_unit_priority_list->job_id==job_id))
		{
			if(type==0)
			{
				if(t_schedule_unit_priority_list->top_id==top_id)
				{
					t = t_schedule_unit_priority_list;
					break;
				}
			}
			else
			{
				for(i=0;i<10;i++)
				{
					parent_id[i] = 0;
				}

				for(i=0;i<10;i++)
				{
					parent_id[i] = first_ids[i];
					if(first_ids[i]==0)
					{
						parent_id[i-1] = 0;
						break;
					}
				}

				for(i=0;i<10;i++)
				{
					if(parent_id[i]!=t_schedule_unit_priority_list->parent_id[i])
					{
						break;
					}
				}

				if(i==10)
				{
					t = t_schedule_unit_priority_list;
					break;
				}
			}
		}

		t_schedule_unit_priority_list = t_schedule_unit_priority_list->next;
	}

	assert(t!=NULL);

	if(t==list->schedule_unit_priority_list)
	{
		list->schedule_unit_priority_list = list->schedule_unit_priority_list->next;
	}
	else
	{
		t_schedule_unit_priority_list = list->schedule_unit_priority_list;
		while(t_schedule_unit_priority_list->next!=NULL)
		{
			if(t_schedule_unit_priority_list->next==t)
			{
				t_schedule_unit_priority_list->next = t->next;
				break;
			}

			t_schedule_unit_priority_list = t_schedule_unit_priority_list->next;
		}
	}

	free(t);
}

void destory_sub_cluster(struct sub_cluster_status_list_element *t)
{
	struct sub_cluster_status_list_element *t_sub_cluster_list;
	int i;

	pthread_mutex_lock(&master_machine_array_m_lock);

	for(i=0;i<t->sub_machine_num;i++)
	{
		master_machine_array[t->sub_machine_id_list[i]-1].machine_status = 0;
	}

	pthread_mutex_unlock(&master_machine_array_m_lock);

	API_sub_cluster_destroy(t->sub_master_id);
	API_sub_cluster_shut_down(t->sub_master_id);

	if(t==sub_cluster_list)
	{
		sub_cluster_list = sub_cluster_list->next;
		free(t->sub_machine_id_list);
		free(t);
	}
	else
	{
		t_sub_cluster_list = sub_cluster_list;
		while(t_sub_cluster_list->next!=NULL)
		{
			if(t_sub_cluster_list->next==t)
			{
				t_sub_cluster_list->next = t->next;
				break;
			}

			t_sub_cluster_list = t_sub_cluster_list->next;
		}
		free(t->sub_machine_id_list);
		free(t);
	}

}

int can_destory_sub_cluster(struct sub_cluster_status_list_element *t)
{
	return 1;
}

void check_waiting_sub_task(int job_id)
{
	struct child_wait_all_list_element *t_child_wait_all_list_m,*t;
	int ret;


	pthread_mutex_lock(&child_wait_all_list_m_m_lock);

	t_child_wait_all_list_m = child_wait_all_list_m;
	while(t_child_wait_all_list_m!=NULL)
	{
		if(t_child_wait_all_list_m->job_id==job_id)
		{
			ret = try_wake_up_job(t_child_wait_all_list_m,job_id);
		}
		else
		{
			ret = 0;
		}

		t = t_child_wait_all_list_m;
		t_child_wait_all_list_m = t_child_wait_all_list_m->next;

		if(ret==1)
		{
			delete_element_from_child_wait_all_list_m(t);
		}
	}

	pthread_mutex_unlock(&child_wait_all_list_m_m_lock);
}

void delete_element_from_child_wait_all_list_m(struct child_wait_all_list_element *t)
{
	struct child_wait_all_list_element *t_child_wait_all_list_m;

	assert(t!=NULL);
	assert(child_wait_all_list_m!=NULL);

	if(t==child_wait_all_list_m)
	{
		child_wait_all_list_m = child_wait_all_list_m->next;
		free(t);
	}
	else
	{
		t_child_wait_all_list_m = child_wait_all_list_m;
		while(t_child_wait_all_list_m->next!=NULL)
		{
			if(t_child_wait_all_list_m->next==t)
			{
				t_child_wait_all_list_m->next = t->next;
				free(t);
				break;
			}
			t_child_wait_all_list_m = t_child_wait_all_list_m->next;
		}
		assert(t_child_wait_all_list_m!=NULL);
	}
}

int try_wake_up_job(struct child_wait_all_list_element *t,int job_id)
{
	struct job_description_element *t_running_job_list;
	char **ret_args;
	int child_num;
	int i;

	t_running_job_list = running_job_list;

	while(t_running_job_list!=NULL)
	{
		if(t_running_job_list->job_id==job_id)
		{
			if(t->type==0)
			{
				if(1==try_wake_up_normal_sub_task(t_running_job_list,t->top_id,&child_num,&ret_args))
				{
					API_child_wake_up_all_m_to_s(t,child_num,ret_args);

					for(i=0;i<child_num;i++)
					{
						free(ret_args[i]);
					}
					free(ret_args);

					break;
				}
			}
			else
			{
				if(1==try_wake_up_sub_pack(t_running_job_list,t->id,&child_num,&ret_args))
				{
					API_child_wake_up_all_m_to_s(t,child_num,ret_args);
					break;
				}
			}
		}

		t_running_job_list = t_running_job_list->next;
	}

	if(t_running_job_list!=NULL)
	{
		return 1;
	}
	else
	{
		return 0;
	}
}

int try_wake_up_normal_sub_task(struct job_description_element *t,int top_id,int *child_num,char ***ret_args)
{
	int i,j;

	*ret_args = NULL;
	*child_num = 0;

	if(t->job.normal_sub_task_description_array[top_id-1].root->child_created==0)
	{
		printf("An idiot programmer waits all children before create them,hahaha.\n");
	}
	else
	{
		for(i=0;i<t->job.normal_sub_task_description_array[top_id-1].root->parallel_sub_task_description.child_num;i++)
		{
			if(t->job.normal_sub_task_description_array[top_id-1].root->sub_pack_description_tree[i].parallel_sub_task_description.status!=FINISHED)
			{
				break;
			}
		}

		if(i==t->job.normal_sub_task_description_array[top_id-1].root->parallel_sub_task_description.child_num)
		{
			*ret_args = (char **)malloc(i*sizeof(char *));

			for(j = 0;j<i;j++)
			{
				(*ret_args)[j] = strdup(t->job.normal_sub_task_description_array[top_id-1].root->parallel_sub_task_description.child_ret_args[j]);
			}
			*child_num = i;
			return 1;
		}

	}

	return 0;
}

int try_wake_up_sub_pack(struct job_description_element *t,int id[10],int *child_num,char ***ret_args)
{
	struct sub_pack_description_tree_element *parent;
	int p;
	int i,j;

	*ret_args = NULL;
	*child_num = 0;

	parent = t->job.normal_sub_task_description_array[id[0]-1].root;

	p = 1;
	while(id[p]!=0)
	{
		parent = &(parent->sub_pack_description_tree[id[p]-1]);
		p++;
	}

	if(parent->child_created==0)
	{
		printf("An idiot programmer waits all children before create them,hahaha.\n");
	}
	else
	{
		for(i=0;i<parent->parallel_sub_task_description.child_num;i++)
		{
			if(parent->sub_pack_description_tree[i].parallel_sub_task_description.status!=FINISHED)
			{
				break;
			}
		}

		if(i==parent->parallel_sub_task_description.child_num)
		{
			*ret_args = (char **)malloc(i*sizeof(char *));

			for(j = 0;j<i;j++)
			{
				(*ret_args)[j] = strdup(parent->parallel_sub_task_description.child_ret_args[j]);
			}
			*child_num = i;
			return 1;
		}
	}
	return 0;
}

void fill_next_arg(int job_id,int top_id,char *ret_arg)
{
	struct job_description_element *t_running_job_list;
	int i,j;

	pthread_mutex_lock(&running_job_list_m_lock);

	t_running_job_list = running_job_list;
	while(t_running_job_list!=NULL)
	{
		if(t_running_job_list->job_id==job_id)
		{
			for(i=top_id-1+1;i<t_running_job_list->job.sub_unit_num;i++)
			{
				if(t_running_job_list->job.sub_unit_DAG[top_id-1][i].rely_type==1)
				{
					if(t_running_job_list->job.normal_sub_task_description_array[i].prime_sub_task_description.arg_type==1)
					{
						strcpy(t_running_job_list->job.normal_sub_task_description_array[i].prime_sub_task_description.arg,ret_arg);
					}
				}
			}
			break;
		}
		t_running_job_list = t_running_job_list->next;
	}
	pthread_mutex_unlock(&running_job_list_m_lock);
}

void change_sub_task_status_to_finish_fill_ret_args(int type,int job_id,int top_id,char *ret_arg,int id[10],char *ret_args)
{
	struct job_description_element *t_running_job_list;
	struct sub_pack_description_tree_element *root,*parent;
	int p;


	t_running_job_list = running_job_list;
	while(t_running_job_list!=NULL)
	{
		if(t_running_job_list->job_id==job_id)
		{
			if(type==0)
			{
				t_running_job_list->job.normal_sub_task_description_array[top_id-1].status = FINISHED;
				strcpy(t_running_job_list->job.normal_sub_task_description_array[top_id-1].prime_sub_task_description.ret_arg,ret_arg);
				break;
			}
			else
			{
				root = t_running_job_list->job.normal_sub_task_description_array[id[0]-1].root;
				parent = NULL;
				p = 1;
				while(id[p]!=0)
				{
					parent = root;
					root = &(root->sub_pack_description_tree[id[p]-1]);
					p++;
				}
				assert(parent);

				parent->parallel_sub_task_description.child_ret_args[id[p-1]-1] = strdup(ret_args);

				root->parallel_sub_task_description.status = FINISHED;
				break;
			}
		}
		t_running_job_list = t_running_job_list->next;
	}

	if(is_job_finished(t_running_job_list))
	{
		printf("job %ld finished\n",t_running_job_list->job_id);
		move_to_finished_job_list(t_running_job_list);
		//	move to finished list
	}

}

void move_to_finished_job_list(struct job_description_element *t)
{
	struct job_description_element *t_running_job_list;

	assert(running_job_list);

	if( t== running_job_list)
	{
		running_job_list = running_job_list->next;
	}
	else
	{
		t_running_job_list = running_job_list;
		while(t_running_job_list->next != NULL)
		{
			if(t_running_job_list->next == t)
			{
				t_running_job_list->next = t->next;
				break;
			}
			t_running_job_list = t_running_job_list->next;
		}
	}

	t->next = finished_job_list;

	finished_job_list = t;

	pthread_mutex_lock(&running_job_num_m_lock);

	running_job_num--;

	if(running_job_num==0)
	{
		all_job_finish = 1;
	}

	pthread_mutex_unlock(&running_job_num_m_lock);
}

int is_job_finished(struct job_description_element *t)
{
	int i;

	for(i=0;i<t->job.sub_unit_num;i++)
	{
		if(t->job.normal_sub_task_description_array[i].status!=FINISHED)
		{
			break;
		}
	}

	if(i==t->job.sub_unit_num)
	{
		return 1;
	}
	else
	{
		return 0;
	}
}

/**
 * 把文件中的作业信息读取出来，同时把这些作业加入到预备执行的作业链表中
 */
void auto_job_submit(char *arg)
{
	struct job_description_element *t,*t_pre_job_list;
	struct prime_job_description_element *ret;
	static int job_id;
	char *t_arg;
	int i;

	t_arg = strdup(arg);

	printf("job path = %s!\n",t_arg);

	ret = load_job(t_arg);

	free(t_arg);

	t = (struct job_description_element *)malloc(sizeof(struct job_description_element));

	t->job = *ret;
	free(ret);
	t->submit_time = time(NULL);
	job_id = (job_id+1)%65536;
	t->job_id = job_id;
	t->next = NULL;

	pthread_mutex_lock(&pre_job_list_m_lock);
	if(pre_job_list==NULL)
	{
		pre_job_list = t;
	}
	else
	{
		t_pre_job_list = pre_job_list;
		while(t_pre_job_list->next!=NULL)
		{
			t_pre_job_list = t_pre_job_list->next;
		}
		t_pre_job_list->next = t;
	}
	pthread_mutex_unlock(&pre_job_list_m_lock);

	delay_policy();
}

void job_submit_handler(int comm_source, int ack_tag, char *arg)
{
	struct job_description_element *t, *t_pre_job_list;
	struct prime_job_description_element *ret;
	static int job_id;
	char *t_arg;
	int i = 0;

	//TODO while don't use a function handle this?
	while(arg[i++] != ';');

	t_arg = strdup(arg + i);

	send_ack_msg(comm_source, ack_tag, "");
	printf("job path = %s!\n", t_arg);
	ret = load_job(t_arg);
	free(t_arg);

	t = (struct job_description_element *)malloc(sizeof(struct job_description_element));

	t->job = *ret;
	free(ret);
	t->submit_time = time(NULL);
	job_id = (job_id+1)%65536;
	t->job_id = job_id;
	t->next = NULL;

	pthread_mutex_lock(&pre_job_list_m_lock);
	if(pre_job_list==NULL)
	{
		pre_job_list = t;
	}
	else
	{
		t_pre_job_list = pre_job_list;
		while(t_pre_job_list->next!=NULL)
		{
			t_pre_job_list = t_pre_job_list->next;
		}
		t_pre_job_list->next = t;
	}
	pthread_mutex_unlock(&pre_job_list_m_lock);

	delay_policy();
}

/**
 * 将预备执行的作业加入到正在执行的作业中
 */
void delay_policy()
{
	struct job_description_element *t,*t_pre_job_list,*t_running_job_list;

	pthread_mutex_lock(&pre_job_list_m_lock);
	pthread_mutex_lock(&running_job_list_m_lock);

	t = pre_job_list;
	if(t ==NULL)
	{
		printf("delay policy:no pre job!\n");
		log_error("delay policy:no pre job!\n");
		exit(1);
	}
	pre_job_list = pre_job_list->next;

	t->next = NULL;

	if(running_job_list==NULL)
	{
		running_job_list = t;
	}
	else
	{
		t_running_job_list = running_job_list;
		while(t_running_job_list->next!=NULL)
		{
			t_running_job_list = t_running_job_list->next;
		}

		t_running_job_list->next = t;
	}

	pthread_mutex_unlock(&running_job_list_m_lock);
	pthread_mutex_unlock(&pre_job_list_m_lock);
}

void registration_m_handler(int comm_source,int ack_tag,char *arg)
{
	char *t_arg;
	char *parameter;
	char ip[16];
	char ret[16];
	char *ret_msg;
	char *msg;
	char *save_ptr;
	int machine_id;
	int sub_master_id;
	int i;

	static int job_num = 0;

	i = 0;

	while(arg[i] != ';')
	{
		i++;
	}
	i++;

	t_arg = strdup(arg + i);

	send_ack_msg(comm_source,ack_tag, "");

	printf("t_arg = %s\n", t_arg);

	machine_id = comm_source;

	parameter = strtok_r(t_arg,",",&save_ptr);
	master_machine_array[machine_id-1].machine_description.CPU_core_num = atoi(parameter);
	printf("parameter = %s!\n",parameter);

	parameter = strtok_r(NULL,",",&save_ptr);
	master_machine_array[machine_id-1].machine_description.GPU_core_num = atoi(parameter);
	printf("parameter = %s!\n",parameter);

	parameter = strtok_r(NULL,",",&save_ptr);
	master_machine_array[machine_id-1].machine_description.IO_bus_capacity = atoi(parameter);
	printf("parameter = %s!\n",parameter);

	parameter = strtok_r(NULL,",",&save_ptr);
	master_machine_array[machine_id-1].machine_description.network_capacity = atoi(parameter);
	printf("parameter = %s! atoi = %d\n",parameter,atoi(parameter));

	free(t_arg);

	pthread_mutex_lock(&master_machine_array_m_lock);

	master_machine_array[machine_id-1].machine_status = 1;
	master_machine_array[machine_id-1].machine_id = machine_id;
	master_machine_array[machine_id-1].comm_id = machine_id;

	pthread_mutex_unlock(&master_machine_array_m_lock);

	printf("machine %d online \n",machine_id);

	for(i=0;i<master_machine_num-1;i++)
	{
		if(master_machine_array[i].machine_status==0)
		{
			break;
		}
	}

	if(i==master_machine_num-1)
	{
		printf("all machines online~~~\n");
		//submit task here
		if(job_num==0)
		{
			sleep(2);

			pthread_mutex_lock(&running_job_num_m_lock);

			running_job_num = 0;

			for(i=0;i<running_job_num;i++)
			{
				auto_job_submit("job");
			}

			pthread_mutex_unlock(&running_job_num_m_lock);

			job_num++;
		}
	}
}

void sub_cluster_heart_beat_handler(int comm_source, int ack_tag, char *arg)
{
	struct sub_cluster_status_list_element *t;
	char *t_arg;
	int sub_master_id;
	int sub_cluster_id;
	int i;

	i = 0;
	while(arg[i] != ';')
	{
		i++;
	}

	i++;

	t_arg = strdup(arg + i);

	send_ack_msg(comm_source, ack_tag, "");

	sub_master_id = comm_source;
	t = get_sub_cluster_element_through_sub_master_id(sub_master_id);

	if(t != NULL)
	{
		fill_sub_machine_status(t_arg, t);
	}

	free(t_arg);
}

void fill_sub_machine_status(char *arg, struct sub_cluster_status_list_element *t)
{
	char *t_arg;
	char *parameter;
	char *save_ptr;
	int sub_machine_num;
	int machine_id;
	int average_CPU_free;
	int average_GPU_load;
	int average_memory_free;
	int average_network_free;
	int i;

	t_arg = strdup(arg);
	//printf("arg = %s\n", arg);
	printf("t_arg = %s\n", t_arg);

	parameter = strtok_r(t_arg, ",", &save_ptr);
	sub_machine_num = atoi(parameter);
	if(sub_machine_num != t->sub_machine_num)
	{
		printf("fill_sub_machine_status fail,sub_machine_num wrong!,%d!=%d", sub_machine_num, t->sub_machine_num);
		log_error("fill_sub_machine_status fail\n");
		exit(1);
	}

	average_CPU_free = 0;
	average_GPU_load = 0;
	average_memory_free = 0;
	average_network_free = 0;

	for(i = 0; i < sub_machine_num; i++)
	{
		machine_id = t->sub_machine_id_list[i];

		parameter = strtok_r(NULL , "_", &save_ptr);
		master_machine_array[machine_id-1].machine_description.CPU_free = atoi(parameter);
		if(master_machine_array[machine_id-1].machine_description.CPU_free == 0)
		{
			printf("CPU = 0  error!\n");
			log_error("CPU = 0 error!\n");
			exit(1);
		}

		average_CPU_free += master_machine_array[machine_id-1].machine_description.CPU_free;

		parameter = strtok_r(NULL, "_", &save_ptr);
		master_machine_array[machine_id-1].machine_description.GPU_load = atoi(parameter);
		average_GPU_load += master_machine_array[machine_id-1].machine_description.GPU_load;

		parameter = strtok_r(NULL, "_", &save_ptr);
		master_machine_array[machine_id-1].machine_description.memory_free = atoi(parameter);
		average_memory_free += master_machine_array[machine_id-1].machine_description.memory_free;

		parameter = strtok_r(NULL, "_", &save_ptr);
		master_machine_array[machine_id-1].machine_description.network_free = atoi(parameter);
		average_network_free += master_machine_array[machine_id-1].machine_description.network_free;
	}

	t->sub_cluster_description.average_CPU_free = average_CPU_free/sub_machine_num;
	t->sub_cluster_description.average_GPU_load = 0;
	t->sub_cluster_description.average_memory_free = average_memory_free/sub_machine_num;
	t->sub_cluster_description.average_network_free = average_network_free/sub_machine_num;

	free(t_arg);
}

msg_t msg_type(char *msg)
{
	char type[64];
	int i;

	for(i=0;i<5;i++)
	{
		type[i] = msg[i];
	}

	type[i] = '\0';

	if(strcmp(type,"TYPE:"))
	{
		printf("msg_type:start of msg is not TYPE:\n");
		log_error("msg_type:start of msg is not TYPE:\n");
		exit(1);
		return UNKNOWN;
	}

	for(i=0;i<64;i++)
	{
		if(msg[i+5]!=';')
		{
			type[i] = msg[i+5];
		}
		else
		{
			type[i] = '\0';
			break;
		}
	}

	if(!strcmp(type,"SUB_CLUSTER_HEART_BEAT"))
	{
		return SUB_CLUSTER_HEART_BEAT;
	}
	else if(!strcmp(type,"JOB_SUBMIT"))
	{
		return JOB_SUBMIT;
	}
	else if(!strcmp(type,"SCHEDULE_UNIT_FINISH"))
	{
		return SCHEDULE_UNIT_FINISH;
	}
	else if(!strcmp(type,"REGISTRATION_M"))
	{
		return REGISTRATION_M;
	}
	else if(!strcmp(type,"CHILD_CREATE"))
	{
		return CHILD_CREATE;
	}
	else if(!strcmp(type,"CHILD_WAIT_ALL"))
	{
		return CHILD_WAIT_ALL;
	}
	else if(!strcmp(type,"GET_SUB_TASK_IP"))
	{
		return GET_SUB_TASK_IP;
	}
	else if(!strcmp(type,"MACHINE_HEART_BEAT"))
	{
		return MACHINE_HEART_BEAT;
	}
	else
	{
		return UNKNOWN;
	}
}

/**
 * 读取文件中的信息到数据结构中去，读取一个作业
 */
struct prime_job_description_element *load_job(char *path)
{
	FILE *fp;
	struct prime_job_description_element *ret;
	char t_path[128];
	int i,j;

	strcpy(t_path,"../jobs/");
	strcat(t_path,path);

	fp = fopen(t_path,"r");

	if(fp==NULL)
	{
		printf("cannot find job in %s!\n",t_path);
		perror("!\n");
		return 0;
	}

	ret = (struct prime_job_description_element *)malloc(sizeof(struct prime_job_description_element));

	memset(ret,0,sizeof(struct prime_job_description_element));

	fscanf(fp,"sub_unit_num:%d\n",&(ret->sub_unit_num));


	ret->normal_sub_task_description_array = (struct normal_sub_task_description_element *)malloc(ret->sub_unit_num*sizeof(struct normal_sub_task_description_element));

	memset(ret->normal_sub_task_description_array,0,ret->sub_unit_num*sizeof(struct normal_sub_task_description_element));

	for(i=0;i<ret->sub_unit_num;i++)
	{
		load_normal_sub_task_description_element(fp,&(ret->normal_sub_task_description_array[i]),i+1);
	}

	fscanf(fp,"total_resource++++++++++++++++++++\n");
	fscanf(fp,"CPU_cores:%d\n",&(ret->total_resource.CPU_cores));
	fscanf(fp,"GPU_cores:%d\n",&(ret->total_resource.GPU_cores));
	fscanf(fp,"memory:%d\n",&(ret->total_resource.memory));
	fscanf(fp,"band_width:%d\n",&(ret->total_resource.band_width));

	ret->sub_unit_DAG = (struct sub_unit_DAG_element **)malloc(ret->sub_unit_num*sizeof(struct sub_unit_DAG_element *));
	for(i=0;i<ret->sub_unit_num;i++)
	{
		ret->sub_unit_DAG[i] = (struct sub_unit_DAG_element *)malloc(ret->sub_unit_num*sizeof(struct sub_unit_DAG_element));
	}

	fscanf(fp,"DAG####################\n");
	for(i=0;i<ret->sub_unit_num;i++)
	{
		for(j=0;j<ret->sub_unit_num;j++)
		{
			fscanf(fp,"%d,%d,%d\n",&(ret->sub_unit_DAG[i][j].rely_type),&(ret->sub_unit_DAG[i][j].communication_type),&(ret->sub_unit_DAG[i][j].communication_amount));
		}
	}

	fscanf(fp,"job_priority:%d\n",&(ret->job_priority));

	fclose(fp);

	return ret;
}

void load_normal_sub_task_description_element(FILE *fp,struct normal_sub_task_description_element *arg,int normal_sub_task_id)
{
	int b_level;
	int i;
	int ret;

	fscanf(fp,"********************\n");
	fscanf(fp,"sub_task_path:%s\n",arg->prime_sub_task_description.sub_task_path);
	fscanf(fp,"CPU_prefer:%d\n",&(arg->prime_sub_task_description.CPU_prefer));
	fscanf(fp,"GPU_prefer:%d\n",&(arg->prime_sub_task_description.GPU_prefer));
	fscanf(fp,"exe_time:%d\n",&(arg->prime_sub_task_description.exe_time));
	fscanf(fp,"exe_density:%d\n",&(arg->prime_sub_task_description.exe_density));
	fscanf(fp,"memory_demand:%d\n",&(arg->prime_sub_task_description.memory_demand));
	fscanf(fp,"network_density:%d\n",&(arg->prime_sub_task_description.network_density));
	fscanf(fp,"CPU_memory_network:%d,%d,%d\n",&(arg->prime_sub_task_description.weight[0]),&(arg->prime_sub_task_description.weight[1]),&(arg->prime_sub_task_description.weight[2]));
	fscanf(fp,"arg_type:%d\n",&(arg->prime_sub_task_description.arg_type));
	fscanf(fp,"arg:%s\n",arg->prime_sub_task_description.arg);

	fscanf(fp,"sub_pack_level:%d\n",&(arg->sub_pack_level));

	if(arg->sub_pack_level>0)
	{
		b_level = arg->sub_pack_level;

		arg->parallel_sub_task_abstract_array = (struct parallel_sub_task_abstract_element *)malloc((arg->sub_pack_level+1)*sizeof(struct parallel_sub_task_abstract_element));

		for(i=0;i<arg->sub_pack_level+1;i++)//以不同层次的任务形成的数组
		{
			fscanf(fp,"--------------------\n");
			fscanf(fp,"sub_task_path:%s\n",arg->parallel_sub_task_abstract_array[i].prime_sub_task_description.sub_task_path);
			fscanf(fp,"CPU_prefer:%d\n",&(arg->parallel_sub_task_abstract_array[i].prime_sub_task_description.CPU_prefer));
			fscanf(fp,"GPU_prefer:%d\n",&(arg->parallel_sub_task_abstract_array[i].prime_sub_task_description.GPU_prefer));
			fscanf(fp,"exe_time:%d\n",&(arg->parallel_sub_task_abstract_array[i].prime_sub_task_description.exe_time));
			fscanf(fp,"exe_density:%d\n",&(arg->parallel_sub_task_abstract_array[i].prime_sub_task_description.exe_density));
			fscanf(fp,"memory_demand:%d\n",&(arg->parallel_sub_task_abstract_array[i].prime_sub_task_description.memory_demand));
			fscanf(fp,"network_density:%d\n",&(arg->parallel_sub_task_abstract_array[i].prime_sub_task_description.network_density));
			fscanf(fp,"CPU_memory_network:%d,%d,%d\n",&(arg->parallel_sub_task_abstract_array[i].prime_sub_task_description.weight[0]),&(arg->parallel_sub_task_abstract_array[i].prime_sub_task_description.weight[1]),&(arg->parallel_sub_task_abstract_array[i].prime_sub_task_description.weight[2]));
			fscanf(fp,"arg_type:%d\n",&(arg->parallel_sub_task_abstract_array[i].prime_sub_task_description.arg_type));
			fscanf(fp,"arg:%s\n",arg->parallel_sub_task_abstract_array[i].prime_sub_task_description.arg);

			arg->parallel_sub_task_abstract_array[i].sub_pack_b_level = b_level;
			b_level--;

			fscanf(fp,"inclusive_data_transfer_amount:%d\n",&(arg->parallel_sub_task_abstract_array[i].inclusive_data_transfer_amount));
			fscanf(fp,"data_out_amount:%d\n",&(arg->parallel_sub_task_abstract_array[i].data_out_amount));
			fscanf(fp,"data_out_type:%d\n",&(arg->parallel_sub_task_abstract_array[i].data_out_type));
			fscanf(fp,"data_in_amount:%d\n",&(arg->parallel_sub_task_abstract_array[i].data_in_amount));
			fscanf(fp,"data_in_type:%d\n",&(arg->parallel_sub_task_abstract_array[i].data_in_type));
			fscanf(fp,"data_back_amount:%d\n",&(arg->parallel_sub_task_abstract_array[i].data_back_amount));
			fscanf(fp,"data_back_type:%d\n",&(arg->parallel_sub_task_abstract_array[i].data_back_type));
			fscanf(fp,"machine_num:%d\n",&(arg->parallel_sub_task_abstract_array[i].machine_num));
		}

	}
	else
	{
		arg->parallel_sub_task_abstract_array = NULL;
	}
	arg->root = NULL;
	arg->normal_sub_task_id = normal_sub_task_id;
	arg->status = NOT_EMIT;//初始值为还没发出
	arg->sub_cluster_id = 0;
	arg->start_time = -1;
	arg->priority = 0;
}
