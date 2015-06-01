#include <time.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <mpi.h>

#ifndef	_DATA_H
#define	_DATA_H

#define	MSG_QUEUE_KEY				22224
#define	WAKE_UP_MSG_QUEUE_KEY		22225
#define	APP_ETH						"eth0"
#define	PROB_INTERVAL				100000
#define	MPI_LOCAL_DEAMON_MSG_TYPE	1

/**
 * 信息传递类别
 */
typedef enum
{
	SUB_CLUSTER_HEART_BEAT,
	JOB_SUBMIT,
	SCHEDULE_UNIT_ASSIGN,
	SCHEDULE_UNIT_FINISH,
	SUB_SCHEDULER_ASSIGN,
	COMPUTATION_NODE_ASSIGN,
	SUB_TASK_ASSIGN,
	MACHINE_HEART_BEAT,
	SUB_TASK_FINISH,
	REGISTRATION_M,
	REGISTRATION_S,
	CHILD_CREATE,
	CHILD_WAIT_ALL,
	CHILD_WAKE_UP_ALL,
	GET_SUB_TASK_IP,
	SUB_CLUSTER_DESTROY,
	BACK_TO_MAIN_MASTER,
	SUB_CLUSTER_SHUT_DOWN,
	UNKNOWN
}msg_t;

/**
 * 描述一个任务或作业处于的状态
 */
typedef enum
{
	NOT_EMIT,
	RUNNING,
	SUSPEND,
	FINISHED,
	CREATED			//	sub task in sub pack  only. programmers use API to set, not in this frame
}status_t;

//子节点角色
typedef enum
{
	FREE_MACHINE,
	COMPUTATION_MACHINE,
	SUB_MASTER_MACHINE,
	HALF_SUB_MASTER_MACHINE,//当主节点刚任命一个子节点为sub_master时设为此状态，当将该节点设为计算节点时状态改为sub_master
	UNAVAILABLE_MACHINE
}machine_role_t;

struct task_pack_total_resource_element
{
	unsigned int CPU_cores;
	unsigned int GPU_cores;
	unsigned int memory;
	unsigned int band_width;
};

struct data_transfer_element
{
	int transfer_type;
	int transfer_unit_id;
	int transfer_data_amount;
};

/**
 * 对子任务对资源需求的信息
 * @arg_type:表示参数从命令行读取还是从上一个完成任务的结果中读取
 * @ret_arg:返回参数存储的位置
 */
struct prime_sub_task_description_element
{
	char sub_task_path[64];
	int CPU_prefer;
	int GPU_prefer;
	int exe_time;
	int exe_density;	//1000 means sub task needs 1 core
	int memory_demand;	//1000 means sub task needs 10000mB memory
	int network_density;	//(100*1024/8) means sub task needs 100mb band width
	int weight[3];

	int arg_type;		//0:from job file	1:from parent
	char arg[64];

	char ret_arg[64];
};

/**
 * 用来描述一个子任务的信息
 * @prime_sub_task_description: 子任务需求描述
 * @sub_pack_level: 拥有子任务包的层数，每个任务有可能分出多个子任务，这些子任务形成一个子任务包
 * @parallel_sub_task_abstract_array: 指向其下子任务包，只有当sub_pack_level不等于0时该值才有用
 * @root: 子任务树的根节点，即生成子任务的节点，如有子节点的话只好根据自己的信息生成一个root，使其构成树
 * @normal_sub_task_id: 子任务id号
 * @status: 子任务处于的状态
 * @priority: 子任务的优先级
 * @sub_cluster_id: 子集群id
 */
struct normal_sub_task_description_element
{
	struct prime_sub_task_description_element prime_sub_task_description;

	int sub_pack_level;		//0: no sub pack	1: 1 sub pack ...
	struct parallel_sub_task_abstract_element *parallel_sub_task_abstract_array; //从文件读取的子任务描述，包括自己的任务描述也包含
	struct sub_pack_description_tree_element *root;

	int normal_sub_task_id;

	status_t status;
	time_t start_time; //初始值为-1
	int priority;
	int sub_cluster_id;
};

/**
 * 多个相同任务的抽象结构
 * data_in data_out 用于记录和上下层节点交换的数据数量？？该数据结构简直没有用的地方
 */
struct parallel_sub_task_abstract_element
{
	struct prime_sub_task_description_element prime_sub_task_description;

	int sub_pack_b_level;

	int inclusive_data_transfer_amount;
	int data_out_amount;
	int data_in_amount;
	int data_out_type;
	int data_in_type;
	int data_back_amount;
	int data_back_type;
	int machine_num;
};

/**
 * @id:这里用10个数字记录id，表明最多可以用10层，每一个位置存储一个上层任务的id号
 */
struct parallel_sub_task_description_element
{
	struct prime_sub_task_description_element prime_sub_task_description;

//	dynamic

	int id[10];
	int sibling_num;//兄弟的个数
	int child_num;
	char **child_args;
	char **child_ret_args;
	int inclusive_data_transfer_amount;

	int data_out_amount;
	int data_in_amount;
	int data_out_type;
	int data_in_type;

	int data_back_amount;
	int data_back_type;

	int machine_num;

	time_t start_time;
	int priority;
	status_t status;
	int sub_cluster_id;
};

/**
 * 用于记录父子关系
 * @sub_pack_description_tree:是个数组，访问按下标访问（好吧，不是指针数组）
 * @parallel_sub_task_description:存放了这层任务的信息
 */
struct sub_pack_description_tree_element
{
	int sub_pack_b_level;		//b-level   0:is sub leaf 1:children are leaf
	int child_created;
	struct parallel_sub_task_description_element parallel_sub_task_description;

	struct sub_pack_description_tree_element *sub_pack_description_tree;
};

/**
 * DAG:Directed Acyclic Graph，有向无环图
 * 保存图的信息，rely表示与其他任务相连，即其他任务依赖于该任务
 * 剩余两个参数暂时不用
 */
struct sub_unit_DAG_element
{
	int rely_type;			//0:no rely 1:rely
	int communication_type;
	int communication_amount;
};

/**
 * 基本作业单元描述,最顶层的数据结构
 * @sub_unit_num: 子单元数目
 * @normal_sub_task_description_array: 解决这一作业的子任务描述数组。该程序会读取作业，由于一个作业
 * 可能有多个需要按序执行的任务，故形成这一数组来保存每个任务的状态
 * @total_resource: 子任务总共需要的资源
 * @sub_unit_DAG: 有向无环图(拓扑图)
 * @status: 作业处于的状态
 * @sub_unit_DAG: 用于表示任务执行先后顺序的一个图(拓扑图)
 * @job_priority: 作业优先级
 * @exe_time: 作业执行的时间
 */
struct prime_job_description_element
{
	int sub_unit_num;
//	int *sub_unit_type_array;	//0:normal 1:pack
	struct normal_sub_task_description_element *normal_sub_task_description_array;
	struct task_pack_total_resource_element total_resource;
	struct sub_unit_DAG_element **sub_unit_DAG;
	int job_priority;

	status_t status;
	int exe_time;
};

/**
 * 链表结构，该结构体表示链表中的元素
 * @job_id: 作业编号
 * @submit_time: 提交时间
 */
struct job_description_element
{
	struct prime_job_description_element job;
	time_t submit_time;
	unsigned long int job_id;
	struct job_description_element * next;
};

struct machine_description_element
{
	int CPU_core_num;
	int GPU_core_num;
	int CPU_free;		// 10000 average of all cores
	int GPU_load;		// 10000
	int memory_free;	// mB
	int network_free;	// mb
	int IO_bus_capacity;
	int network_capacity;
	int memory_total;	//10mB
	int memory_swap;
};

struct machine_status_array_element
{
	int machine_id;
	int comm_id;
	int sub_master_id;
	struct machine_description_element machine_description;
	int machine_status;	 //0:unavailable 1:available not in cluster 2.in sub cluster
	time_t last_heart_beat_time;
};

struct sub_cluster_description_element
{
	int total_CPU_core_num;
	int total_GPU_core_num;
	int average_CPU_free;		//computation resource left   1000 = 1 core = 1 unit
	int average_GPU_load;
	int average_memory_free;		//average mB			2G = 1 unit
	int average_network_free;
	int total_IO_bus_capacity;
	int total_network_capacity;	//				1000mb = 1 unit
	int total_memory_total;
	int total_memory_swap;
};

/**
 * 用于记录集群上调度单元的优先级
 */
struct schedule_unit_priority_list_element
{
	int schedule_unit_type;
	int job_id;
	int top_id;
	int parent_id[10];//父节点的id编号，最大为9位
	int schedule_unit_num;

	int priority;

	struct schedule_unit_priority_list_element *next;
};

struct sub_cluster_status_list_element
{
	int sub_cluster_id;
	int sub_master_id;
	struct sub_cluster_description_element sub_cluster_description;
//	int sub_cluster_status;		// 0:unavailable 1:available
	int sub_machine_num;
	int *sub_machine_id_list;//这里记录这个集群所管理的机器id
	int schedule_unit_count;
	struct schedule_unit_priority_list_element *schedule_unit_priority_list;
	//为什么不建立一个已有的数据结构链表去管理，而去创造一个新的数据结构来管理所属任务？？
	time_t last_heart_beat_time;

	struct sub_cluster_status_list_element *next;
};

struct p_sub_cluster_status_array_element
{
	int sub_cluster_id;
	int sub_machine_num;
	int *sub_machine_id_list;
};

typedef struct schedule_unit_description_element
{
	int schedule_unit_type;		//0:normal sub task  1:sub pack

	struct prime_sub_task_description_element prime_sub_task_description;

	int schedule_unit_num;

	int job_id;

	int top_id;//用来标记这个任务的id

	int priority;

	int machine_num;//需要机器的数量

	int **ids;//用来记录所有子任务的标识

	char **args;

}schedule_unit_description_element;
struct schedule_unit_status_list_element
{
	int schedule_unit_type;

	int schedule_unit_num;

	int job_id;

	int priority;

//	type:0
	int top_id;
	status_t status;

//	type:1	
	int **ids;
	status_t *status_array;

	char **ret_args;

	struct schedule_unit_status_list_element *next;
};

struct server_arg_element
{
	MPI_Status status;
	char *msg;
};

struct waiting_schedule_list_element
{
	int type;
	struct prime_sub_task_description_element prime_sub_task_description;
	int job_id;
	int top_id;
	int id[10];
	int priority;

	struct waiting_schedule_list_element *next;
};

struct schedule_list_element
{
	int type;
	struct prime_sub_task_description_element prime_sub_task_description;
	int job_id;
	int top_id;
	int id[10];
	int priority;

	int exe_machine_id;
	status_t status;
	time_t start_time;

	struct schedule_list_element *next;
};

struct sub_task_running_list_element
{
	int type;
	int job_id;
	int top_id;
	int id[10];

	status_t status;

	struct sub_task_running_list_element *next;
};

struct sub_task_exe_arg_element
{
	int type;
	int job_id;
	int top_id;
	int id[10];
	char arg[64];
//	char ip[16];	ip addr will be added as a parameter when execv is executed
};

struct child_wait_all_list_element
{
	int type;
	int job_id;
	int top_id;
	int id[10];
	int pid;	//this struct will be used in M S C node. pid is valid in C node only
	int machine_id;			//S node only
	int sub_cluster_id;		//M node only
	struct child_wait_all_list_element *next;
};

struct cpu_stat_element
{
	unsigned long int user;
	unsigned long int nice;
	unsigned long int system;
	unsigned long int idle;
	unsigned long int io_wait;
	unsigned long int irq;
	unsigned long int soft_irq;
};

typedef struct sub_cluster_rank_array_element
{
	int sub_cluster_id;
	int sub_machine_num;
	struct sub_cluster_description_element pridict_status;
	int total_sub_task_num;
	int priority_task_num[2];
	int score;
}sub_cluster_rank_array_element;

struct machine_rank_array_element {
	int machine_id;
	struct machine_description_element pridict_status;
	int total_sub_task_num;
	int priority_task_num[2];
	int score;
};

struct p_sub_cluster_status_array_element *p_sub_cluster_array;
int p_sub_cluster_num;

struct sub_cluster_status_list_element *sub_cluster_list;//这个链表存储了集群的状态信息
int t_sub_cluster_id;

struct machine_status_array_element *master_machine_array;//used to store each machine's status
int master_machine_num;

struct job_description_element *pre_job_list;//预备要执行的作业，直接从文件中接受作业信息
struct job_description_element *running_job_list;//已经准备要运行的作业
//预备要执行的作业与运行中的作业有区别，由于有多个作业，running_job_list会把预备作业加入，并将该作业
//从pre_job_list中删去，如此反复将预读取的作业添加至运行作业表中。
struct job_description_element *finished_job_list;
struct child_wait_all_list_element *child_wait_all_list_m;

int running_job_num;

int all_job_finish;

//以下均为互斥锁变量
pthread_mutex_t pre_job_list_m_lock;
pthread_mutex_t running_job_list_m_lock;
pthread_mutex_t child_wait_all_list_m_m_lock;
pthread_mutex_t debug_m_lock;
pthread_mutex_t sub_cluster_list_m_lock;
pthread_mutex_t master_machine_array_m_lock;
//pthread_mutex_t t_tag_m_lock;
pthread_mutex_t local_machine_role_m_lock;
pthread_mutex_t running_job_num_m_lock;
pthread_mutex_t log_m_lock;

#endif
