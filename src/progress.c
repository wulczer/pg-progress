/*------------------------------------------------------------------------
 *
 * progress.c
 *	   dynamic query progress calculation
 *
 * Copyright (c) 2013, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"
#include "nodes/bitmapset.h"
#include "storage/procsignal.h"
#include "storage/ipc.h"
#include "executor/executor.h"
#include "executor/hashjoin.h"
#include "utils/builtins.h"
#include "lib/stringinfo.h"

#include "progress.h"
#include "progress_util.h"
#include "progress_pipeline.h"

PG_MODULE_MAGIC;


/* Saved hook values to avoid stepping on other plugins' toes */
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static procsignal_handler_hook_type prev_procsignal_handler_hook = NULL;
static ExecutorStart_hook_type prev_ExecutorStart_hook = NULL;
static ExecutorRun_hook_type prev_ExecutorRun_hook = NULL;

/* global reference to the backend's currently executing query */
static volatile QueryDesc	*currentQueryDesc = NULL;

/* pointer to shared memory state */
static ProgressSharedState	*progress_state = NULL;


/***********************/
/* Progress estimation */
/***********************/

static double
processed_Hash(HashState *node)
{
	if (node->hashtable == NULL)
		return 0.0;

	return node->hashtable->totalTuples;
}


static double
node_tup_processed(PlanState *node)
{
	double	ret = 0.0;

	switch (nodeTag(node))
	{
		case T_HashState:
			ret = processed_Hash((HashState *) node);

		default:
			ret = node->instrument->ntuples + node->instrument->tuplecount;
	}

	return ret;
}


static double
dne_estimator(List *nodes)
{
	double		 tup_processed;
	double		 tup_estimated;
	ListCell	*lc;

	tup_estimated = tup_processed = 0.0;

	foreach(lc, nodes)
	{
		PlanState		*node = (PlanState *) lfirst(lc);
		ProgressInstr	*instr = PROGRESS_INSTR(node);

		if (tup_estimated == 0.0)
			tup_estimated = instr->tup_estimated;
		else
			tup_estimated = Min(tup_estimated, instr->tup_estimated);

		tup_processed += node_tup_processed(node);
	}

	tup_processed /= list_length(nodes);

	return tup_estimated / tup_processed;
}


static double
pipeline_to_process(PipelineData *pdata)
{
	bool		 finished = true;
	ListCell	*lc;

	Assert(pdata->driver_nodes != NIL);

	foreach(lc, pdata->driver_nodes)
	{
		PlanState		*node  = (PlanState *) lfirst(lc);
		ProgressInstr	*instr = PROGRESS_INSTR(node);

		if (!instr->finished)
			finished = false;
	}

	if (finished)
		return pdata->tup_processed;

	if (pdata->tup_processed == 0.0)
		return pdata->tup_estimated;

	return pdata->tup_processed * dne_estimator(pdata->driver_nodes);
}


static double
estimate_progress(PipelineData *pdata, int no_pipelines)
{
	int			i;
	double		total_processed	 = 0.0;
	double		total_to_process = 0.0;

	for (i = 0; i < no_pipelines; i++)
	{
		total_processed += pdata[i].tup_processed;
		total_to_process += pipeline_to_process(&pdata[i]);
	}

	if (total_to_process == 0.0)
		return 0.0;

	return total_processed / total_to_process;
}


static void
estimator_walker(PlanState *node, List *children, void *context)
{
	PipelineData		*pdata = context;
	ProgressInstr		*instr = PROGRESS_INSTR(node);
	PipelineData		*this_pdata;

	this_pdata = &pdata[instr->pipeline_id];

	this_pdata->tup_processed += node_tup_processed(node);
	this_pdata->tup_estimated += Max(node_tup_processed(node),
									 instr->tup_estimated);
	if (instr->is_driver)
		this_pdata->driver_nodes = lappend(this_pdata->driver_nodes, node);
}


/*****************/
/* DOT debugging */
/*****************/

static void
dot_dump_walker(PlanState *node, List *children, void *context)
{
	StringInfo			 si	   = context;
	ProgressInstr		*instr = PROGRESS_INSTR(node);
	ListCell			*lc;

	appendStringInfoSpaces(si, 4);
	appendStringInfo(si, "P%p [label=<(P%d)<br/>%s<br/>"
					 "%.0f/%.0f<br/>%.02f%% done>",
					 node, instr->pipeline_id, plan_node_name(node),
					 node_tup_processed(node), instr->tup_estimated,
					 node_tup_processed(node) / instr->tup_estimated * 100.0);
	if (instr->is_driver)
		appendStringInfo(si, ", fillcolor=\"#cdcdcd\", style=filled");
	appendStringInfo(si, "];\n");

	foreach(lc, children)
	{
		PlanState *child = (PlanState *) lfirst(lc);

		appendStringInfoSpaces(si, 4);
		appendStringInfo(si, "P%p -> P%p;\n", node, child);
	}
}


/********************/
/* Main entry point */
/********************/

static void
calculate_progress(volatile QueryDesc *queryDesc)
{
	EState				*estate	= queryDesc->estate;
	ProgressState		*pstate = estate->es_private;
	PipelineData		*pdata;
	double				 estimate;
	StringInfoData		 si;
	int					 i;

	pdata = palloc(sizeof(PipelineData) * pstate->no_pipelines);
	for (i = 0; i < pstate->no_pipelines; i++)
	{
		pdata[i].tup_processed = 0;
		pdata[i].tup_estimated = 0;
		pdata[i].driver_nodes = NIL;
	}
	plan_state_walker(queryDesc->planstate, estimator_walker, pdata);
	estimate = estimate_progress(pdata, pstate->no_pipelines);

	initStringInfo(&si);
	appendStringInfo(&si, "digraph progress {\n");
	plan_state_walker(queryDesc->planstate, dot_dump_walker, &si);
	appendStringInfo(&si, "}");

	LWLockAcquire(progress_state->lock, LW_EXCLUSIVE);
	progress_state->estimate = estimate;
	strncpy(progress_state->dot_dump, si.data, PROGRESS_DOT_DUMP_SIZE);
	progress_state->dot_dump[PROGRESS_DOT_DUMP_SIZE - 1] = '\0';
	LWLockRelease(progress_state->lock);

	pfree(si.data);
}


/*************************/
/* Instrumentation hooks */
/*************************/

static Instrumentation *
progress_InstrAlloc(int n, int instrument_options)
{
	int					 i;
	Instrumentation		*instr;
	ProgressInstr		*private;

	instr = standard_InstrAlloc(n, instrument_options);

	for (i = 0; i < n; i++)
	{
		private = palloc(sizeof(ProgressInstr));
		private->pipeline_id = 0;
		private->is_driver = false;
		private->finished = false;
		private->tup_estimated = 0.0;
		private->loops_estimated = 0.0;

		instr[i].private = private;
	}

	return instr;
}


static void
progress_InstrStopNode(Instrumentation *instr, double nTuples)
{
	ProgressInstr	*private = instr->private;

	standard_InstrStopNode(instr, nTuples);
	if (nTuples == 0.0)
		private->finished = true;
}


/******************/
/* Executor hooks */
/******************/

static void
teardown_progress(QueryDesc *queryDesc)
{
	EState				*estate	 = queryDesc->estate;

	pfree(estate->es_private);

	estate->es_private = NULL;
	currentQueryDesc = NULL;
}


static void
progress_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
	queryDesc->instrument_options |= INSTRUMENT_ROWS;

	if (prev_ExecutorStart_hook)
		prev_ExecutorStart_hook(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);

}


static void
progress_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction, long count)
{
	ProgressState		*pstate = palloc(sizeof(ProgressState));
	EState				*estate	 = queryDesc->estate;

	find_pipelines(queryDesc->planstate, pstate);
	find_planner_estimates(queryDesc->planstate, pstate);

	estate->es_private = (void *) pstate;

	currentQueryDesc = queryDesc;

	PG_TRY();
	{
		if (prev_ExecutorRun_hook)
			prev_ExecutorRun_hook(queryDesc, direction, count);
		else
			standard_ExecutorRun(queryDesc, direction, count);
		teardown_progress(queryDesc);
	}
	PG_CATCH();
	{
		teardown_progress(queryDesc);
		PG_RE_THROW();
	}
	PG_END_TRY();
}


/***********************/
/* Signal handler hook */
/***********************/

static void
progress_procsignal_handler_hook(void)
{
	if (prev_procsignal_handler_hook)
		prev_procsignal_handler_hook();

	if (currentQueryDesc == NULL)
		return;

	calculate_progress(currentQueryDesc);
}


/******************************/
/* Shared memory startup hook */
/******************************/

static void
progress_shmem_startup_hook(void)
{
	bool	found;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	progress_state = NULL;

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	progress_state = ShmemInitStruct("progress",
									 sizeof(ProgressSharedState), &found);
	if (!found)
	{
		progress_state->lock = LWLockAssign();
		progress_state->estimate = 0.0;
		memset(progress_state->dot_dump, 0, PROGRESS_DOT_DUMP_SIZE);
	}

	LWLockRelease(AddinShmemInitLock);
}


/***************************/
/* SQL interface functions */
/***************************/

Datum
pg_progress_update(PG_FUNCTION_ARGS)
{
	int ret;

	ret = SendProcSignal(PG_GETARG_INT32(0), PROCSIG_HOOK, InvalidBackendId);

	PG_RETURN_BOOL(ret == 0);
}


Datum
pg_progress(PG_FUNCTION_ARGS)
{
	double		val;

	if (progress_state == NULL)
		elog(ERROR, "progress.so should be preloaded");

	LWLockAcquire(progress_state->lock, LW_SHARED);
	/* XXX should val be volatile? will this actually copy memory? */
	val = progress_state->estimate;
	LWLockRelease(progress_state->lock);

	PG_RETURN_FLOAT8(val);
}


Datum
pg_progress_dot(PG_FUNCTION_ARGS)
{
	char		data[PROGRESS_DOT_DUMP_SIZE];

	if (progress_state == NULL)
		elog(ERROR, "progress.so should be preloaded");

	LWLockAcquire(progress_state->lock, LW_SHARED);
	strncpy(data, progress_state->dot_dump, PROGRESS_DOT_DUMP_SIZE);
	data[PROGRESS_DOT_DUMP_SIZE - 1] = '\0';
	LWLockRelease(progress_state->lock);

	PG_RETURN_TEXT_P(cstring_to_text(data));
}


void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		return;

	/* request shared memory */
	RequestAddinShmemSpace(sizeof(ProgressSharedState));
	RequestAddinLWLocks(1);

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = progress_shmem_startup_hook;

	/* setup executor start/end hooks */
	prev_ExecutorStart_hook = ExecutorStart_hook;
	ExecutorStart_hook = progress_ExecutorStart;
	prev_ExecutorRun_hook = ExecutorRun_hook;
	ExecutorRun_hook = progress_ExecutorRun;

	/* setup instrumentation hooks */
	InstrAlloc_hook = progress_InstrAlloc;
	InstrStopNode_hook = progress_InstrStopNode;

	/* setup signal hook */
	prev_procsignal_handler_hook = procsignal_handler_hook;
	procsignal_handler_hook = progress_procsignal_handler_hook;
}
