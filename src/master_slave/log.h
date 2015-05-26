#ifndef	_DATA_COMPUTATION_H
#define _DATA_COMPUTATION_H

void log_before_start_up();
void log_after_start_up();
void log_record_start_up();
void log_API(char *msg,msg_t type,time_t start,time_t finish);
void log_main_master();
void log_sub_master();
void log_error(char *msg);

#endif
