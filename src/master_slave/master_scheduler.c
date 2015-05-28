#include <stdio.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <strings.h>
#include <stdlib.h>
#include <string.h>
#include "./structure/data.h"
#include "./common/api.h"
#include "assert.h"
#include "master_scheduler.h"

/*=====================private declarations====================*/
static int **DAG;
static int DAG_node_num;
/**
 *
 */
//下面数组为大小可变数组，用于存储可供调度的单元信息（可供调度包含各个级别的作业和任务）
static struct schedule_unit_description_element *total_schedule_unit_array; //姑且称作无敌调度大数组
static int total_schedule_unit_num;
static struct sub_cluster_rank_array_element *sub_cluster_rank_array;//用于选取集群，临时一用，用于排序
static int sub_cluster_rank_array_num;

static void master_scheduler_init();
static void select_schedule_unit(void);
static void add_job_schedule_unit(struct job_description_element *job_description);
static int can_schedule_normal_sub_task(struct prime_job_description_element *prime_job_description,int sub_task_index);
static void try_add_schedule_unit_sub_pack(struct sub_pack_description_tree_element *root,int job_id,int n_id[10],int n);
static void add_schedule_unit_sub_pack(struct sub_pack_description_tree_element *root,int job_id,int n_id[10],int n);
static void add_schedule_unit_normal_sub_task(struct job_description_element *job_description,int normal_sub_task_index);
static void reset_total_schedule_unit(void);
static void assign_schedule_unit(void);
static void change_schedule_unit_status_to_running(int schedule_unit_index,int selected_sub_cluster_id);
static struct sub_pack_description_tree_element *find_parent_root(int schedule_unit_index,struct job_description_element *t_running_job_list,int *level);
static void init_tree_element(struct job_description_element *job,int schedule_unit_index);
static void rank_schedule_unit();
//void try_split_sub_pack();
//void split_one_sub_pack_to_two(int index,int max_num);
static void fill_sub_cluster_description(struct sub_cluster_status_list_element *t);
static void update_priority();
static void update_a_job_priority(struct job_description_element *t);
static void change_sub_pack_priority(struct sub_pack_description_tree_element *t,int priority);
static int construct_DAG(struct job_description_element *t);
static void select_start_node(int *start_node);
static int calc_longest_path(int start_node_index);
static void init_sub_cluster_rank_array();
static int select_sub_cluster(int schedule_unit_index);
static int try_to_create_sub_cluster(int need_machine_num,int schedule_unit_index);


/**
 * 初始化数据信息，用以之后任务调度
 */
static void master_scheduler_init()
{
	total_schedule_unit_num = 0;
	total_schedule_unit_array = NULL;
}

/**
 * 将running_job_list中状态为可以调度的作业加入到调度数组中
 */
static void select_schedule_unit(void)
{
	struct job_description_element *t_running_job_list;
	int i;

	pthread_mutex_lock(&running_job_list_m_lock);

	t_running_job_list = running_job_list;

	while(t_running_job_list!=NULL)
	{
		add_job_schedule_unit(t_running_job_list);
		t_running_job_list = t_running_job_list->next;
	}

	pthread_mutex_unlock(&running_job_list_m_lock);
}

static void add_job_schedule_unit(struct job_description_element *job_description)
{
	struct sub_unit_DAG_element **DAG;
	int sub_unit_num;
	int n_id[10];//id用于作为子任务的表示，10个表示总共有10层
	int n;
	int i;

	for(i=0;i<10;i++)
	{
		n_id[i] = 0;
	}

	sub_unit_num = job_description->job.sub_unit_num;
	for(i = 0; i < sub_unit_num; i++)//遍历所有子作业
	{
		//对于处于未提交状态的子作业
		if(job_description->job.normal_sub_task_description_array[i].status == NOT_EMIT)
		{
			if(can_schedule_normal_sub_task(&(job_description->job), i))
			{
				add_schedule_unit_normal_sub_task(job_description, i);
			}
		}
		//对于正在运行的任务，没创建子任务拉倒，创建的话就会把创建的子任务加入到这个数组中
		else if(job_description->job.normal_sub_task_description_array[i].status == RUNNING)
		{
			if((job_description->job.normal_sub_task_description_array[i].sub_pack_level!=0))
			{
				if(job_description->job.normal_sub_task_description_array[i].root->child_created==1)
				{
					n_id[0] = i+1;
					try_add_schedule_unit_sub_pack(job_description->job.normal_sub_task_description_array[i].root,job_description->job_id,n_id,1);
				}
			}
		}
	}
}

/**
 * 检验之前依赖的作业是否已经完成，只有当依赖的作业已经完成才能进行调度
 */
static int can_schedule_normal_sub_task(struct prime_job_description_element *prime_job_description,int sub_task_index)
{
	int i;

	for(i=0;i<sub_task_index;i++)
	{
		if(prime_job_description->sub_unit_DAG[i][sub_task_index].rely_type==1)
		{
			if(prime_job_description->normal_sub_task_description_array[i].status!=FINISHED)
			{
				return 0;
			}
		}
	}

	return 1;
}

/**
 * n刚传入时是1，n表示你n_id应该填写的位置，递归程序的终点是child_created==1，一堆子任务当做一个
 * 调度单元传入该数组。在这里用子任务中第一个任务的信息作为模板，将其赋值于任务数组中
 */
static void try_add_schedule_unit_sub_pack(struct sub_pack_description_tree_element *root,int job_id,int n_id[10],int n)
{
	int i;

	if(root->sub_pack_b_level==0)
	{
		return;
	}

	if(root->child_created==1)
	{
		if(root->sub_pack_description_tree[0].parallel_sub_task_description.status==CREATED)
		{
			add_schedule_unit_sub_pack(root,job_id,n_id,n);
		}
		else
		{
			for(i=0;i<root->parallel_sub_task_description.child_num;i++)
			{
				if(root->sub_pack_description_tree[i].parallel_sub_task_description.status==RUNNING)
				{
					n_id[n] = i;
					try_add_schedule_unit_sub_pack(&(root->sub_pack_description_tree[i]),job_id,n_id,n+1);
				}
			}
		}
	}
}

static void add_schedule_unit_sub_pack(struct sub_pack_description_tree_element *root,int job_id,int n_id[10],int n)
{
	int i,j;

//	debug_count++;

	total_schedule_unit_num++;
	total_schedule_unit_array = (struct schedule_unit_description_element *)realloc(total_schedule_unit_array,total_schedule_unit_num*sizeof(struct schedule_unit_description_element));

	total_schedule_unit_array[total_schedule_unit_num-1].schedule_unit_type = 1;//表示为子任务

	total_schedule_unit_array[total_schedule_unit_num-1].prime_sub_task_description = root->sub_pack_description_tree[0].parallel_sub_task_description.prime_sub_task_description;

	total_schedule_unit_array[total_schedule_unit_num-1].schedule_unit_num = root->parallel_sub_task_description.child_num;
	total_schedule_unit_array[total_schedule_unit_num-1].job_id = job_id;
	total_schedule_unit_array[total_schedule_unit_num-1].top_id = 0;
	total_schedule_unit_array[total_schedule_unit_num-1].priority = master_get_sub_task_priority_without_lock(1,job_id,0,n_id);
	total_schedule_unit_array[total_schedule_unit_num-1].machine_num = root->parallel_sub_task_description.machine_num;

	total_schedule_unit_array[total_schedule_unit_num-1].ids = (int **)malloc(root->parallel_sub_task_description.child_num*sizeof(int *));

	for(i=0;i<root->parallel_sub_task_description.child_num;i++)
	{
		total_schedule_unit_array[total_schedule_unit_num-1].ids[i] = (int *)malloc(10*sizeof(int));
	}

	for(i=0;i<root->parallel_sub_task_description.child_num;i++)
	{
		for(j=0;j<10;j++)
		{
			total_schedule_unit_array[total_schedule_unit_num-1].ids[i][j] = 0;
		}
	}

	for(i=0;i<root->parallel_sub_task_description.child_num;i++)
	{
		for(j=0;j<n;j++)
		{
			total_schedule_unit_array[total_schedule_unit_num-1].ids[i][j] = n_id[j];
		}
		total_schedule_unit_array[total_schedule_unit_num-1].ids[i][j] = i+1;
	}

	total_schedule_unit_array[total_schedule_unit_num-1].args = (char **)malloc(root->parallel_sub_task_description.child_num*sizeof(char *));

	for(i=0;i<root->parallel_sub_task_description.child_num;i++)
	{
		total_schedule_unit_array[total_schedule_unit_num-1].args[i] = strdup(root->parallel_sub_task_description.child_args[i]);
	}
}

static void add_schedule_unit_normal_sub_task(struct job_description_element *job_description,int normal_sub_task_index)
{
	total_schedule_unit_num++;
	//增加数组的大小，realloc
	total_schedule_unit_array = (struct schedule_unit_description_element *)realloc(total_schedule_unit_array,total_schedule_unit_num*sizeof(struct schedule_unit_description_element));
	//加入新增的子作业
	total_schedule_unit_array[total_schedule_unit_num-1].schedule_unit_type = 0;


	total_schedule_unit_array[total_schedule_unit_num-1].prime_sub_task_description = job_description->job.normal_sub_task_description_array[normal_sub_task_index].prime_sub_task_description;

	total_schedule_unit_array[total_schedule_unit_num-1].schedule_unit_num = 1;
	total_schedule_unit_array[total_schedule_unit_num-1].job_id = job_description->job_id;
	total_schedule_unit_array[total_schedule_unit_num-1].top_id = normal_sub_task_index+1;

	total_schedule_unit_array[total_schedule_unit_num-1].priority = master_get_sub_task_priority_without_lock(0,job_description->job_id,normal_sub_task_index+1,NULL);

	total_schedule_unit_array[total_schedule_unit_num-1].machine_num = 1;
	total_schedule_unit_array[total_schedule_unit_num-1].ids = NULL;
	total_schedule_unit_array[total_schedule_unit_num-1].args = NULL;
}

/**
 * 清空已经赋值的数据结构，准备下一次作业调度
 */
static void reset_total_schedule_unit(void)
{
	int i,j;

	for(i=0;i<total_schedule_unit_num;i++)
	{
		if(total_schedule_unit_array[i].schedule_unit_type==1)
		{
			for(j=0;j<total_schedule_unit_array[i].schedule_unit_num;j++)
			{
				free(total_schedule_unit_array[i].ids[j]);
				free(total_schedule_unit_array[i].args[j]);
			}
			free(total_schedule_unit_array[i].ids);
			free(total_schedule_unit_array[i].args);
		}
	}


	free(total_schedule_unit_array);

	total_schedule_unit_array = NULL;

	total_schedule_unit_num = 0;
}

/**
 * 在这里说一下什么叫做cluster，这个东西由好多机器组成，一个cluster可以运行多个任务，这些任务可以有不同
 * 的优先级，只要集群满足任务的需求，则可以拿来运行
 */
static void assign_schedule_unit(void)
{
	int selected_sub_cluster_id;
	int i;

	sub_cluster_rank_array = NULL;

	init_sub_cluster_rank_array();

	for(i=0;i<total_schedule_unit_num;i++)
	{
		selected_sub_cluster_id = select_sub_cluster(i);

		if(selected_sub_cluster_id>0)
		{
			API_schedule_unit_assign(total_schedule_unit_array[i],selected_sub_cluster_id);
			change_schedule_unit_status_to_running(i,selected_sub_cluster_id);
		}
	}

	free(sub_cluster_rank_array);
	sub_cluster_rank_array_num = 0;
}

static void change_schedule_unit_status_to_running(int schedule_unit_index,int selected_sub_cluster_id)
{
	struct sub_pack_description_tree_element *parent_root;
	struct job_description_element *t_running_job_list;
	int parent_id[10];
	int level;
	int i;

	pthread_mutex_lock(&running_job_list_m_lock);
	if(total_schedule_unit_array[schedule_unit_index].schedule_unit_type==0)
	{
		t_running_job_list = running_job_list;
		while(t_running_job_list!=NULL)
		{
			if(t_running_job_list->job_id==total_schedule_unit_array[schedule_unit_index].job_id)
			{
				t_running_job_list->job.normal_sub_task_description_array[total_schedule_unit_array[schedule_unit_index].top_id-1].status = RUNNING;
				t_running_job_list->job.normal_sub_task_description_array[total_schedule_unit_array[schedule_unit_index].top_id-1].start_time = time(NULL);
				t_running_job_list->job.normal_sub_task_description_array[total_schedule_unit_array[schedule_unit_index].top_id-1].sub_cluster_id = selected_sub_cluster_id;
				t_running_job_list->job.normal_sub_task_description_array[total_schedule_unit_array[schedule_unit_index].top_id-1].priority = total_schedule_unit_array[schedule_unit_index].priority;

				if(t_running_job_list->job.normal_sub_task_description_array[total_schedule_unit_array[schedule_unit_index].top_id-1].sub_pack_level>0)
				{
					init_tree_element(t_running_job_list,schedule_unit_index);
				}
				break;
			}
			t_running_job_list = t_running_job_list->next;
		}
		if(t_running_job_list==NULL)
		{
			printf("change_schedule_unit_status:cannot find job description!\n");
			log_error("change_schedule_unit_status:cannot find job description!\n");
			exit(1);
		}
	}
	//简直丧心病狂
	else if(total_schedule_unit_array[schedule_unit_index].schedule_unit_type==1)
	{
		t_running_job_list = running_job_list;
		while(t_running_job_list!=NULL)
		{
			if(t_running_job_list->job_id==total_schedule_unit_array[schedule_unit_index].job_id)
			{
				parent_root = find_parent_root(schedule_unit_index,t_running_job_list,&level);
				for(i=0;i<total_schedule_unit_array[schedule_unit_index].schedule_unit_num;i++)
				{
					parent_root->sub_pack_description_tree[total_schedule_unit_array[schedule_unit_index].ids[i][level]-1].parallel_sub_task_description.status = RUNNING;
					parent_root->sub_pack_description_tree[total_schedule_unit_array[schedule_unit_index].ids[i][level]-1].parallel_sub_task_description.start_time = time(NULL);
					parent_root->sub_pack_description_tree[total_schedule_unit_array[schedule_unit_index].ids[i][level]-1].parallel_sub_task_description.sub_cluster_id = selected_sub_cluster_id;
				}
				break;
			}
			t_running_job_list = t_running_job_list->next;
		}
		if(t_running_job_list==NULL)
		{
			printf("change_schedule_unit_status:cannot find job description 1 !\n");
			log_error("change_schedule_unit_status:cannot find job description 1 !\n");
			exit(1);
		}
	}
	pthread_mutex_unlock(&running_job_list_m_lock);
}

static struct sub_pack_description_tree_element *find_parent_root(int schedule_unit_index,struct job_description_element *t_running_job_list,int *level)
{
	struct sub_pack_description_tree_element *ret;
	int parent_id[10];
	int p;
	int i;

	//下面这一堆求了一个level
	for(i=0;i<10;i++)
	{
		parent_id[i] = total_schedule_unit_array[schedule_unit_index].ids[0][i];
	}
	for(i=0;i<10;i++)
	{
		if(parent_id[9-i]!=0)
		{
			parent_id[9-i] = 0;
			*level = 9-i;
			break;
		}
	}

	ret = t_running_job_list->job.normal_sub_task_description_array[parent_id[0]-1].root;
	p = 1;
	//这样终于求出来了！！！
	while(parent_id[p]!=0)
	{
		ret = &(ret->sub_pack_description_tree[parent_id[p]-1]);
		p++;
	}

	return ret;
}

static void init_tree_element(struct job_description_element *job,int schedule_unit_index)
{
	struct sub_pack_description_tree_element *root;
	int i;
	//下面一行超出了整个屏幕，实在。。。好吧，只是一个空间申请
	job->job.normal_sub_task_description_array[total_schedule_unit_array[schedule_unit_index].top_id-1].root = (struct sub_pack_description_tree_element *)malloc(sizeof(struct sub_pack_description_tree_element));
	root = job->job.normal_sub_task_description_array[total_schedule_unit_array[schedule_unit_index].top_id-1].root;

	root->sub_pack_b_level = job->job.normal_sub_task_description_array[total_schedule_unit_array[schedule_unit_index].top_id-1].sub_pack_level;

	root->child_created = 0;

	root->parallel_sub_task_description.prime_sub_task_description = job->job.normal_sub_task_description_array[total_schedule_unit_array[schedule_unit_index].top_id-1].prime_sub_task_description;
	//上面部分依葫芦画瓢，造了一个和原来作业相同的root
	for(i=0;i<10;i++)
	{
		root->parallel_sub_task_description.id[i] = 0;
	}
	//下面这些都是改变root节点的状态
	root->parallel_sub_task_description.id[0] = total_schedule_unit_array[schedule_unit_index].top_id;
	root->parallel_sub_task_description.sibling_num = 0;
	root->parallel_sub_task_description.child_num = 0;
	root->parallel_sub_task_description.child_args = NULL;
	root->parallel_sub_task_description.child_ret_args = NULL;

	root->parallel_sub_task_description.inclusive_data_transfer_amount = job->job.normal_sub_task_description_array[total_schedule_unit_array[schedule_unit_index].top_id-1].parallel_sub_task_abstract_array[0].inclusive_data_transfer_amount;
	root->parallel_sub_task_description.data_out_amount = job->job.normal_sub_task_description_array[total_schedule_unit_array[schedule_unit_index].top_id-1].parallel_sub_task_abstract_array[0].data_out_amount;
	root->parallel_sub_task_description.data_in_amount = job->job.normal_sub_task_description_array[total_schedule_unit_array[schedule_unit_index].top_id-1].parallel_sub_task_abstract_array[0].data_in_amount;
	root->parallel_sub_task_description.data_out_type = job->job.normal_sub_task_description_array[total_schedule_unit_array[schedule_unit_index].top_id-1].parallel_sub_task_abstract_array[0].data_out_type;
	root->parallel_sub_task_description.data_in_type = job->job.normal_sub_task_description_array[total_schedule_unit_array[schedule_unit_index].top_id-1].parallel_sub_task_abstract_array[0].data_in_type;
	root->parallel_sub_task_description.data_back_amount = job->job.normal_sub_task_description_array[total_schedule_unit_array[schedule_unit_index].top_id-1].parallel_sub_task_abstract_array[0].data_back_amount;
	root->parallel_sub_task_description.data_back_type = job->job.normal_sub_task_description_array[total_schedule_unit_array[schedule_unit_index].top_id-1].parallel_sub_task_abstract_array[0].data_back_type;
	root->parallel_sub_task_description.machine_num = job->job.normal_sub_task_description_array[total_schedule_unit_array[schedule_unit_index].top_id-1].parallel_sub_task_abstract_array[0].machine_num;//这里简单的用第一个代表整体？？

	root->parallel_sub_task_description.status = RUNNING;
	root->parallel_sub_task_description.sub_cluster_id = job->job.normal_sub_task_description_array[total_schedule_unit_array[schedule_unit_index].top_id-1].sub_cluster_id;
	root->parallel_sub_task_description.start_time = job->job.normal_sub_task_description_array[total_schedule_unit_array[schedule_unit_index].top_id-1].start_time;
	root->parallel_sub_task_description.priority = job->job.normal_sub_task_description_array[total_schedule_unit_array[schedule_unit_index].top_id-1].priority;
}

static void rank_schedule_unit()
{
	struct schedule_unit_description_element t;
	int i,j;
/*
	for(i=0;i<total_schedule_unit_num-1;i++)
	{
		for(j=0;j<total_schedule_unit_num-1-1-i;j++)
		{
			if(total_schedule_unit_array[j].priority<total_schedule_unit_array[j].priority)
			{
				t = total_schedule_unit_array[j];
				total_schedule_unit_array[j] = total_schedule_unit_array[j+1];
				total_schedule_unit_array[j+1] = t;
			}
		}
	}
*/
	return;
}

static void fill_sub_cluster_description(struct sub_cluster_status_list_element *t)
{
	int machine_id;
	int i;

	t->sub_cluster_description.total_CPU_core_num = 0;
	t->sub_cluster_description.total_GPU_core_num = 0;
	t->sub_cluster_description.average_CPU_free = 0;
	t->sub_cluster_description.average_GPU_load = 0;
	t->sub_cluster_description.average_memory_free = 0;
	t->sub_cluster_description.average_network_free = 0;
	t->sub_cluster_description.total_IO_bus_capacity = 0;
	t->sub_cluster_description.total_network_capacity = 0;
	t->sub_cluster_description.total_memory_total = 0;
	t->sub_cluster_description.total_memory_swap = 0;

	for(i=0;i<t->sub_machine_num;i++)
	{
		machine_id = t->sub_machine_id_list[i];

		t->sub_cluster_description.total_CPU_core_num += master_machine_array[machine_id-1].machine_description.CPU_core_num;
		t->sub_cluster_description.total_GPU_core_num += master_machine_array[machine_id-1].machine_description.GPU_core_num;
		t->sub_cluster_description.total_IO_bus_capacity += master_machine_array[machine_id-1].machine_description.IO_bus_capacity;
		t->sub_cluster_description.total_network_capacity += master_machine_array[machine_id-1].machine_description.network_capacity;
		t->sub_cluster_description.total_memory_swap += master_machine_array[machine_id-1].machine_description.memory_swap;
		t->sub_cluster_description.average_CPU_free += master_machine_array[machine_id-1].machine_description.CPU_free;
		t->sub_cluster_description.average_memory_free += master_machine_array[machine_id-1].machine_description.memory_free;
		t->sub_cluster_description.average_network_free += master_machine_array[machine_id-1].machine_description.network_free;
	}

	t->sub_cluster_description.average_CPU_free /=t->sub_machine_num;
	t->sub_cluster_description.average_memory_free /=t->sub_machine_num;
	t->sub_cluster_description.average_network_free /=t->sub_machine_num;

	return ;
}

/**
 * 根据作业执行的路径来计算和更新作业的优先级
 */
static void update_priority()
{
	//job list
	struct job_description_element *t_running_job_list;

	pthread_mutex_lock(&running_job_list_m_lock);//lock global variable

	t_running_job_list = running_job_list;//running_job_list为全局变量，经过注册后已经有值
	while(t_running_job_list != NULL)//循环更新所有作业的优先级
	{
		update_a_job_priority(t_running_job_list);
		t_running_job_list = t_running_job_list->next;
	}

	pthread_mutex_unlock(&running_job_list_m_lock);
}

/**
 * 更新作业优先级
 * 设置和更新一个作业的优先级，更新的信息存入running_job_list中的相应数据结构中
 */
static void update_a_job_priority(struct job_description_element *t)
{
	int *start_node;
	int *node_b_level;
	int longest;
	int i;

	if(1 == construct_DAG(t))
	{

		start_node = (int *)malloc(DAG_node_num * sizeof(int));
		node_b_level = (int *)malloc(DAG_node_num * sizeof(int));
		//0 and 1 can use memset to initialize
		memset(node_b_level, -1, DAG_node_num * sizeof(int));

		select_start_node(start_node);

		for(i = 0; i < DAG_node_num; i++)
		{
			if(start_node[i]==1)
			{
				node_b_level[i] = calc_longest_path(i);
			}
		}

		longest = -1;

		for(i = 0; i < DAG_node_num; i++)
		{
			if(longest < node_b_level[i])
			{
				longest = node_b_level[i];
			}
		}


		//根据生成的有向无环图图的数据和找到的起始节点来设置子作业的优先级
		for(i=0;i<DAG_node_num;i++)
		{
			if(start_node[i]==1)
			{
				if((float)node_b_level[i]<0.6*(float)longest)
				{
					t->job.normal_sub_task_description_array[i].priority = 1;
					if((t->job.normal_sub_task_description_array[i].sub_pack_level)!=0)
					{
						change_sub_pack_priority(t->job.normal_sub_task_description_array[i].root,1);
					}
				}
				else
				{
					t->job.normal_sub_task_description_array[i].priority = 2;
					if((t->job.normal_sub_task_description_array[i].sub_pack_level)!=0)
					{
						change_sub_pack_priority(t->job.normal_sub_task_description_array[i].root,2);
					}
				}
			}
		}


		for(i=0;i<DAG_node_num;i++)
		{
			free(DAG[i]);
		}
		free(DAG);
		free(start_node);
		free(node_b_level);
		DAG_node_num = 0;
	}
}

/**
 * 逐层递归设置优先级，优先级别与上层优先级别相同
 */
static void change_sub_pack_priority(struct sub_pack_description_tree_element *t,int priority)
{
	int i;

	if(t==NULL)
	{
		return;
	}

	t->parallel_sub_task_description.priority = priority;

	if(t->child_created==1)
	{
		for(i=0;i<t->parallel_sub_task_description.child_num;i++)
		{
			change_sub_pack_priority(&(t->sub_pack_description_tree[i]),priority);
		}
	}
}

/**
 * 构造了一个矩阵，用于存储作业的剩余执行时间，最后构造结果存储在DAG中，如果构造失败返回0
 * 成功则返回1
 */
static int construct_DAG(struct job_description_element *t)
{
	struct sub_unit_DAG_element **p_DAG;
	int left_time;
	int construct_fail_flag;//用于标志是否构造成功
	int n_time;
	int i, j;

	p_DAG = t->job.sub_unit_DAG;
	/*构造DAG矩阵，其大小与p_DAG相同，先初始化为0*/
	DAG_node_num = t->job.sub_unit_num;//表示子作业的数量

	DAG = (int **)malloc(DAG_node_num * sizeof(int *));

	for(i = 0; i < DAG_node_num; i++)
	{
		DAG[i] = (int *)malloc(DAG_node_num * sizeof(int));
		memset(DAG[i], 0, sizeof(int) * DAG_node_num);
		//TODO check malloc fail
	}

	construct_fail_flag = 0;

	for(i = 0; i < DAG_node_num; i++)//循环查看每个子作业的状态
	{
		if(t->job.normal_sub_task_description_array[i].status == FINISHED)
		{
		}
		else if(t->job.normal_sub_task_description_array[i].status == RUNNING)
		{
			n_time = time(NULL);

			if(t->job.normal_sub_task_description_array[i].prime_sub_task_description.exe_time == 0)
			{
				construct_fail_flag = 1;
				break;
			}
			//TODO start_time >= exe_time？？
			if(n_time - t->job.normal_sub_task_description_array[i].start_time >= t->job.normal_sub_task_description_array[i].prime_sub_task_description.exe_time)
			{
				left_time = t->job.normal_sub_task_description_array[i].prime_sub_task_description.exe_time;
			}
			else
			{
				left_time = n_time - t->job.normal_sub_task_description_array[i].start_time;
			}

			for(j = i + 1; j < DAG_node_num; j++)
			{
				if(p_DAG[i][j].rely_type == 1)//有依赖关系则将权值设为执行时间数
				{
					DAG[i][j] = left_time;
				}
			}
		}
		else if(t->job.normal_sub_task_description_array[i].status == NOT_EMIT)//作业还没有真正运行，即初始情况
		{
			if(t->job.normal_sub_task_description_array[i].prime_sub_task_description.exe_time == 0)
			{
				construct_fail_flag = 1;
				break;
			}
			/**这里将剩余执行时间设置成预设的执行时间**/
			left_time = t->job.normal_sub_task_description_array[i].prime_sub_task_description.exe_time;
			for(j = i + 1; j < DAG_node_num; j++)
			{
				if(p_DAG[i][j].rely_type == 1)
				{
					DAG[i][j] = left_time;
				}
			}
		}
	}

	if(construct_fail_flag == 1)
	{
		for(i = 0; i < DAG_node_num; i++)
		{
			free(DAG[i]);
		}

		free(DAG);
		DAG_node_num = 0;
		return 0;
	}

	return 1;
}

/**
 * 找到有向无环图的起点
 */
static void select_start_node(int *start_node)
{
	int i, j;

	/*TODO I have thought about this but I don't know whether this is logical right*/
	for(i = 0; i < DAG_node_num; i++){
		start_node[i] = 1;
	}

	for(i = 0; i < DAG_node_num; i++)
	{
		for(j = 0; j < i; j++)
		{
			if(DAG[j][i] != 0)
			{
				start_node[i] = 0;
				break;
			}
		}
	}
}

/**
 * 计算出从起始点出发到结束节所需最长的执行时间，该算法只适合与无向无环图，即把有向无环
 * 图的形式化简成无向无环图的形式才能用此方法
 */
static int calc_longest_path(int start_node_index)
{
	int path_len[DAG_node_num];
	int longest;
	int i;

	if(start_node_index==DAG_node_num-1)
	{
		return 0;
	}

	for(i = 0; i < DAG_node_num; i++)
	{
		path_len[i] = -1;
	}

	for(i = start_node_index + 1; i < DAG_node_num; i++)
	{
		if(DAG[start_node_index][i] != 0)
		{
			path_len[i] = DAG[start_node_index][i] + calc_longest_path(i);
		}
	}

	longest = -1;
	for(i=0;i<DAG_node_num;i++)
	{
		if(longest<path_len[i])
		{
			longest = path_len[i];
		}
	}

	assert(longest>-1);

	return longest;
}

static void init_sub_cluster_rank_array()
{
	struct sub_cluster_status_list_element *t_sub_cluster_list;
	struct schedule_unit_priority_list_element *t_sub_unit_priority_list;
	int i;

	sub_cluster_rank_array_num = 0;

	pthread_mutex_lock(&sub_cluster_list_m_lock);
			
	t_sub_cluster_list = sub_cluster_list;
	while(t_sub_cluster_list!=NULL)
	{
		sub_cluster_rank_array_num ++;
		sub_cluster_rank_array = (struct sub_cluster_rank_array_element *)realloc(sub_cluster_rank_array,sub_cluster_rank_array_num*sizeof(struct sub_cluster_rank_array_element));
		sub_cluster_rank_array[sub_cluster_rank_array_num-1].sub_cluster_id = t_sub_cluster_list->sub_cluster_id;
		sub_cluster_rank_array[sub_cluster_rank_array_num-1].sub_machine_num = t_sub_cluster_list->sub_machine_num;
		sub_cluster_rank_array[sub_cluster_rank_array_num-1].pridict_status = t_sub_cluster_list->sub_cluster_description;
		sub_cluster_rank_array[sub_cluster_rank_array_num-1].total_sub_task_num = 0;
		for(i=0;i<2;i++)
		{
			sub_cluster_rank_array[sub_cluster_rank_array_num-1].priority_task_num[i] = 0;
		}

		t_sub_unit_priority_list = t_sub_cluster_list->schedule_unit_priority_list;

		while(t_sub_unit_priority_list!=NULL)
		{
			sub_cluster_rank_array[sub_cluster_rank_array_num-1].total_sub_task_num += t_sub_unit_priority_list->schedule_unit_num;
			sub_cluster_rank_array[sub_cluster_rank_array_num-1].priority_task_num[t_sub_unit_priority_list->priority-1] += t_sub_unit_priority_list->schedule_unit_num;

			t_sub_unit_priority_list = t_sub_unit_priority_list->next;
		}
		sub_cluster_rank_array[sub_cluster_rank_array_num-1].score = 0;

		t_sub_cluster_list = t_sub_cluster_list->next;
	}

	pthread_mutex_unlock(&sub_cluster_list_m_lock);
}

static int select_sub_cluster(int schedule_unit_index)
{
	struct sub_cluster_status_list_element *t_sub_cluster_list;
	struct sub_cluster_rank_array_element t;
	int weight_int[3];
	float weight[3];
	int ret;
	int i,j;

	weight_int[0] = total_schedule_unit_array[schedule_unit_index].prime_sub_task_description.weight[0];
	weight_int[1] = total_schedule_unit_array[schedule_unit_index].prime_sub_task_description.weight[1];
	weight_int[2] = total_schedule_unit_array[schedule_unit_index].prime_sub_task_description.weight[2];

	weight[0] = (float)weight_int[0]/(float)(weight_int[0]+weight_int[1]+weight_int[2]);
	weight[1] = (float)weight_int[1]/(float)(weight_int[0]+weight_int[1]+weight_int[2]);
	weight[2] = (float)weight_int[2]/(float)(weight_int[0]+weight_int[1]+weight_int[2]);

	for(i=0;i<sub_cluster_rank_array_num;i++)
	{
		sub_cluster_rank_array[i].score = weight[0]*(float)sub_cluster_rank_array[i].pridict_status.average_CPU_free;
		sub_cluster_rank_array[i].score += weight[1]*(float)sub_cluster_rank_array[i].pridict_status.average_memory_free;
		sub_cluster_rank_array[i].score += weight[2]*(float)sub_cluster_rank_array[i].pridict_status.average_network_free;
//		printf("CPU: %d,mem: %d,net: %d\n",sub_cluster_rank_array[i].pridict_status.average_CPU_free,sub_cluster_rank_array[i].pridict_status.average_memory_free,sub_cluster_rank_array[i].pridict_status.average_network_free);
	}

	//冒泡排序。。。
	for(i=0;i<sub_cluster_rank_array_num-1;i++)
	{
		for(j=0;j<sub_cluster_rank_array_num-1-1-i;j++)
		{
			if(sub_cluster_rank_array[j].score<sub_cluster_rank_array[j+1].score)
			{
				t = sub_cluster_rank_array[j];
				sub_cluster_rank_array[j] = sub_cluster_rank_array[j+1];
				sub_cluster_rank_array[j+1] = t;
			}
		}
	}

	ret = 0;

	//选择一个sub_cluster来运行这个unit，是否根据机器的数量来判定？？
	for(i=0;i<sub_cluster_rank_array_num;i++)
	{
//		printf("score = %d!!!!!!!!\n",sub_cluster_rank_array[i].score);
		if(sub_cluster_rank_array[i].score<100)
		{
			break;
		}
		if((sub_cluster_rank_array[i].score<400)&&(total_schedule_unit_array[schedule_unit_index].priority<2))
		{
			break;
		}

		if(sub_cluster_rank_array[i].sub_machine_num<0.5*(float)total_schedule_unit_array[schedule_unit_index].machine_num)
		{
			continue;
		}
		else
		{
			ret = sub_cluster_rank_array[i].sub_cluster_id;
			break;
		}
	}
	//在这里新建一个sub_cluster，在没有合适的sub_cluster的时候
	if(ret==0)
	{
		ret = try_to_create_sub_cluster(total_schedule_unit_array[schedule_unit_index].machine_num,schedule_unit_index);
		//从sub_cluster_list加入到sub_cluster_rank_array
		if(ret>0)
		{
			sub_cluster_rank_array_num++;
			sub_cluster_rank_array = (struct sub_cluster_rank_array_element *)realloc(sub_cluster_rank_array,sub_cluster_rank_array_num*sizeof(struct sub_cluster_rank_array_element));

			t_sub_cluster_list = sub_cluster_list;
			while(t_sub_cluster_list!=NULL)
			{
				if(t_sub_cluster_list->sub_cluster_id==ret)
				{
					break;
				}

				t_sub_cluster_list = t_sub_cluster_list->next;
			}

			assert(t_sub_cluster_list!=NULL);

			sub_cluster_rank_array[sub_cluster_rank_array_num-1].sub_cluster_id = t_sub_cluster_list->sub_cluster_id;
			sub_cluster_rank_array[sub_cluster_rank_array_num-1].sub_machine_num = t_sub_cluster_list->sub_machine_num;
			sub_cluster_rank_array[sub_cluster_rank_array_num-1].pridict_status = t_sub_cluster_list->sub_cluster_description;
			sub_cluster_rank_array[sub_cluster_rank_array_num-1].total_sub_task_num = total_schedule_unit_array[schedule_unit_index].schedule_unit_num;

			for(i=0;i<2;i++)
			{
				sub_cluster_rank_array[sub_cluster_rank_array_num-1].priority_task_num[i] = 0;
			}

			sub_cluster_rank_array[sub_cluster_rank_array_num-1].priority_task_num[total_schedule_unit_array[schedule_unit_index].priority-1] = total_schedule_unit_array[schedule_unit_index].schedule_unit_num;
		}
		else
		{
			printf("cannot create a sub_cluster\n");
		}

		return ret;
	}
	else
	{
		//在这里居然找到了正确的i，这个变量节省的跨度好大
		if(ret!=0)
		{
			sub_cluster_rank_array[i].pridict_status.average_CPU_free -= (float)total_schedule_unit_array[schedule_unit_index].prime_sub_task_description.exe_density/(float)sub_cluster_rank_array[i].sub_machine_num;
			sub_cluster_rank_array[i].pridict_status.average_memory_free -= (float)total_schedule_unit_array[schedule_unit_index].prime_sub_task_description.memory_demand/(float)sub_cluster_rank_array[i].sub_machine_num;
			sub_cluster_rank_array[i].pridict_status.average_network_free -= (float)total_schedule_unit_array[schedule_unit_index].prime_sub_task_description.network_density/(float)sub_cluster_rank_array[i].sub_machine_num;
			sub_cluster_rank_array[i].total_sub_task_num += total_schedule_unit_array[schedule_unit_index].schedule_unit_num;
			sub_cluster_rank_array[i].priority_task_num[total_schedule_unit_array[schedule_unit_index].priority-1] += total_schedule_unit_array[schedule_unit_index].schedule_unit_num;
		}

		return ret;
	}

}

static int try_to_create_sub_cluster(int need_machine_num,int schedule_unit_index)
{
	struct sub_cluster_status_list_element *t;
	int *selected_machine_id_array;
	int sub_master_id;
	int selected_machine_num;
	int i;

	assert(need_machine_num>0);

	selected_machine_id_array = (int *)malloc(need_machine_num*sizeof(int));

	selected_machine_num = 0;
	//下面查看可用机器的个数
	pthread_mutex_lock(&master_machine_array_m_lock);

	for(i=0;i<master_machine_num-1;i++)
	{
		if(master_machine_array[i].machine_status==1)
		{
			selected_machine_id_array[selected_machine_num] = i+1;//好吧，这里machine_num也从1开始
			selected_machine_num++;
			if(selected_machine_num==need_machine_num)
			{
				break;
			}
		}
	}

	pthread_mutex_unlock(&master_machine_array_m_lock);

	if(selected_machine_num==need_machine_num)//在满足需求的情况下
	{
		sub_master_id = selected_machine_id_array[0];//选取第一个作为sub_master

		pthread_mutex_lock(&master_machine_array_m_lock);

		for(i=0;i<need_machine_num;i++)
		{
			master_machine_array[selected_machine_id_array[i]-1].sub_master_id = sub_master_id;//每个机器都找到自己的主节点
			master_machine_array[selected_machine_id_array[i]-1].machine_status = 2;
		}

		pthread_mutex_unlock(&master_machine_array_m_lock);

		t = (struct sub_cluster_status_list_element *)malloc(sizeof(struct sub_cluster_status_list_element));

		t_sub_cluster_id = (t_sub_cluster_id+1)%65536;

		t->sub_cluster_id = t_sub_cluster_id;
		t->sub_master_id = sub_master_id;
		t->sub_machine_num = need_machine_num;
		t->sub_machine_id_list = (int *)malloc(need_machine_num*sizeof(int));

		for(i=0;i<need_machine_num;i++)
		{
			t->sub_machine_id_list[i] = selected_machine_id_array[i];
		}

		t->schedule_unit_count = 0;

		t->schedule_unit_priority_list = NULL;

		fill_sub_cluster_description(t);//填充其余的描述信息

		pthread_mutex_lock(&sub_cluster_list_m_lock);

		t->next = sub_cluster_list;
		sub_cluster_list = t;

		pthread_mutex_unlock(&sub_cluster_list_m_lock);

		API_sub_scheduler_assign(t);//发送SUB_SCHEDULER_ASSIGN信号

		for(i=0;i<need_machine_num;i++)
		{
			API_computation_node_assign(selected_machine_id_array[i]);//发送COMPUTATION_NODE_ASSIGN信号
		}

		free(selected_machine_id_array);
		return t->sub_cluster_id;
	}
	else
	{
		free(selected_machine_id_array);
		return 0;
	}
}

/*
 * 该线程负责调度, 在所有线程执行完之前一直运行
 * 该程序每隔一秒更新一下优先级, 从而按照新的优先级来进行调度
 */
void *master_scheduler(void *arg)
{
	int i;
	/**
	 * 初始化调度
	 */
	master_scheduler_init();
	while(1)
	{
		update_priority();// 决定任务的优先级，数越高，优先级越大

		select_schedule_unit();//决定真正在调度周期内打算调度的任务，构建了那个无敌大数组

		rank_schedule_unit();//对打算调度的任务的优先级排序

		assign_schedule_unit();//给任务选择一个子群去执行,若没有子群,则建立子群.将调度单元分配出去，将分配出去的调度单元状态更改
		reset_total_schedule_unit();//把那个total_schedule_unit_array无敌大数组释放了！！！

		log_main_master(running_job_num);//好吧，写日志暂且不管了

		if(all_job_finish == 1)
		{
			printf("All tasks are finished!!!!!!!!!\n");
			MPI_Abort(MPI_COMM_WORLD, 0);
		}

		sleep(1);
	}
}
/*

void try_split_sub_pack()
{
	int i,j,k;

	i = 0;

	while(i<total_schedule_unit_num)
	{
		if(total_schedule_unit_array[i].schedule_unit_type==1)
		{
			split_one_sub_pack_to_two(i,2);
		}
		i++;
	}
}

void split_one_sub_pack_to_two(int index,int max_num)
{
	int new_num;
	int i,j;

	if(total_schedule_unit_array[index].schedule_unit_num<=max_num)
	{
		return;
	}
	else
	{
		new_num = total_schedule_unit_array[index].schedule_unit_num - max_num;

		total_schedule_unit_num++;
		total_schedule_unit_array = (struct schedule_unit_description_element *)realloc(total_schedule_unit_array,total_schedule_unit_num*sizeof(struct schedule_unit_description_element));
		total_schedule_unit_array[total_schedule_unit_num-1].schedule_unit_type = 1;

		printf("before  prefer = %s\n",total_schedule_unit_array[total_schedule_unit_num-1].prime_sub_task_description.sub_task_path);

		total_schedule_unit_array[total_schedule_unit_num-1].prime_sub_task_description = total_schedule_unit_array[index].prime_sub_task_description;

		printf("after  prefer = %s\n",total_schedule_unit_array[total_schedule_unit_num-1].prime_sub_task_description.sub_task_path);
		exit(21);

		total_schedule_unit_array[total_schedule_unit_num-1].schedule_unit_num = new_num;
		total_schedule_unit_array[total_schedule_unit_num-1].job_id = total_schedule_unit_array[index].job_id;
		total_schedule_unit_array[total_schedule_unit_num-1].top_id = total_schedule_unit_array[index].top_id;

		total_schedule_unit_array[total_schedule_unit_num-1].ids = (int **)malloc(new_num*sizeof(int *));
		for(i=0;i<new_num;i++)
		{
			total_schedule_unit_array[total_schedule_unit_num-1].ids[i] = (int *)malloc(10*sizeof(int));
		}

		for(i=0;i<new_num;i++)
		{
			for(j=0;j<10;j++)
			{
				total_schedule_unit_array[total_schedule_unit_num-1].ids[i][j] = total_schedule_unit_array[index].ids[i+max_num][j];
			}
		}

		total_schedule_unit_array[total_schedule_unit_num-1].args = (char **)malloc(new_num*sizeof(char *));

		for(i=0;i<new_num;i++)
		{
			total_schedule_unit_array[total_schedule_unit_num-1].args[i] = strdup(total_schedule_unit_array[index].args[i+max_num]);
		}

		total_schedule_unit_array[index].schedule_unit_num = max_num;

		for(i=max_num;i<total_schedule_unit_array[index].schedule_unit_num;i++)
		{
			free(total_schedule_unit_array[index].ids[i]);
			free(total_schedule_unit_array[index].args[i]);
		}

		total_schedule_unit_array[index].ids = (int **)realloc(total_schedule_unit_array[index].ids,max_num*sizeof(int *));
		total_schedule_unit_array[index].args = (char **)realloc(total_schedule_unit_array[index].args,max_num*sizeof(char *));
	}
}
*/

