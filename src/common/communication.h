#include "stdio.h"
#include "unistd.h"
#include "pthread.h"
#include "sys/socket.h"
#include "arpa/inet.h"
#include "strings.h"
#include "stdlib.h"
#include "string.h"
#include "../data.h"

#ifndef	_COMMUNICATION_H
#define	_COMMUNICATION_H

extern int open_connection(char *ip,int port);
extern void send_recv_msg(int comm_source,int comm_tag,msg_t msg_type,char *send_msg,char **recv_msg);
extern size_t send_ret_msg(int fd,char *ret);
extern void advanced_send(int socket_fd,char *msg);
extern void advanced_recv(int socket_fd,char **msg);
extern void send_ack_msg(int comm_source,int ack_tag,char *ret);

#endif
