#ifndef	_MPIPFRAME_H
#define	_MPIPFRAME_H

extern int mpipframe_sub_task_finish(char *arg[],char *ret_msg);
extern int mpipframe_child_wait_all(char *arg[],int child_num,char ***child_arg);
extern int mpipframe_child_create(char *arg[],int child_num,char *child_arg[]);

#endif
