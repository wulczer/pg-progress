#ifndef PROGRESS_H
#define PROGRESS_H

#define PROGRESS_DOT_DUMP_SIZE (1024*1024)

typedef struct ProgressSharedState
{
	LWLockId	lock;
	double		estimate;
	char		dot_dump[PROGRESS_DOT_DUMP_SIZE];
} ProgressSharedState;

void		_PG_init(void);

Datum pg_progress_update(PG_FUNCTION_ARGS);
Datum pg_progress(PG_FUNCTION_ARGS);
Datum pg_progress_dot(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(pg_progress_update);
PG_FUNCTION_INFO_V1(pg_progress);
PG_FUNCTION_INFO_V1(pg_progress_dot);

#endif   /* PROGRESS_H */
