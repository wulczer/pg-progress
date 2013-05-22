/*------------------------------------------------------------------------
 *
 * progress_pipeline.c
 *	   pipelines handling for dynamic query progress calculation
 *
 * Copyright (c) 2013, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "progress_pipeline.h"
#include "progress_util.h"


typedef struct ChangePipelineContext {
	int from_id;
	int	to_id;
} ChangePipelineContext;


typedef struct RenumberPipelinesContext {
	int			*pipeline_ids;
	Bitmapset	*seen;
} RenumberPipelinesContext;


static void
change_pipeline_id_walker(PlanState *node, List *children, void *context)
{
	ProgressInstr				*instr = PROGRESS_INSTR(node);
	ChangePipelineContext		*ctx   = context;

	if (instr->pipeline_id == ctx->from_id)
		instr->pipeline_id = ctx->to_id;
}


static void
unmark_driver_nodes_walker(PlanState *node, List *children, void *context)
{
	ProgressInstr		*instr		 = PROGRESS_INSTR(node);
	int					 pipeline_id = *((int *) context);

	if (instr->pipeline_id == pipeline_id)
		instr->is_driver = false;
}


static void
change_pipeline_id(PlanState *node, int pipeline_id)
{
	ProgressInstr			*instr = PROGRESS_INSTR(node);
	ChangePipelineContext	ctx;

	ctx.from_id = instr->pipeline_id;
	ctx.to_id = pipeline_id;

	plan_state_walker(node, change_pipeline_id_walker, &ctx);
}


static void
unmark_driver_nodes(PlanState *node)
{
	ProgressInstr		*instr = PROGRESS_INSTR(node);
	int					 pipeline_id;

	pipeline_id = instr->pipeline_id;
	plan_state_walker(node, unmark_driver_nodes_walker, &pipeline_id);
}



static void
mark_NestLoop(NestLoopState *node, List *children, void *context)
{
	ProgressInstr	*instr = PROGRESS_INSTR(node);

	/* the NL is part of the outer child's pipeline */
	instr->pipeline_id = PROGRESS_INSTR(outerPlanState(node))->pipeline_id;
	/* the entire inner child subtree is also part of that pipeline */
	change_pipeline_id(innerPlanState(node), instr->pipeline_id);
	/* the inner subtree of a NL cannot contain driver nodes */
	unmark_driver_nodes(innerPlanState(node));
}


static void
mark_MergeJoin(MergeJoinState *node, List *children, void *context)
{
	ProgressInstr	*instr = PROGRESS_INSTR(node);

	/* merge the children's pipelines into one */
	instr->pipeline_id = PROGRESS_INSTR(outerPlanState(node))->pipeline_id;
	change_pipeline_id(innerPlanState(node), instr->pipeline_id);
}


static void
mark_HashJoin(HashJoinState *node, List *children, void *context)
{
	ProgressInstr	*instr = PROGRESS_INSTR(node);

	/* a HJ is part of the outer child's pipeline */
	instr->pipeline_id = PROGRESS_INSTR(outerPlanState(node))->pipeline_id;
}


static void
mark_Hash(HashState *node, List *children, void *context)
{
	ProgressInstr	*instr = PROGRESS_INSTR(node);

	/* a hash  is part of its child's pipeline */
	instr->pipeline_id = PROGRESS_INSTR(outerPlanState(node))->pipeline_id;
}


static void
mark_dummy(PlanState *node, List *children, void *context)
{
	ProgressInstr	*instr = PROGRESS_INSTR(node);
	int				*current = context;

	Assert(list_length(children) > 0);

	if (list_length(children) == 1)
	{
		/* if there's only one child, assume we're not blocking and are part of
		 * its pipeline */
		PlanState	*child = (PlanState *) linitial(children);
		instr->pipeline_id = PROGRESS_INSTR(child)->pipeline_id;
	}
	else
	{
		/* for more children, assume we're blocking and start a new pipeline */
		instr->pipeline_id = (*current)++;
		instr->is_driver = true;
	}
}


static void
find_pipelines_walker(PlanState *node, List *children, void *context)
{
	ProgressInstr	*instr = PROGRESS_INSTR(node);
	int				*current = context;

	/* leaf nodes start their own pipelines and are driver nodes by default */
	if (children == NIL)
	{
		instr->pipeline_id = (*current)++;
		instr->is_driver = true;
		return;
	}

	switch (nodeTag(node))
	{
		/* join nodes are handled specifically  */
		case T_NestLoopState:
			mark_NestLoop((NestLoopState *) node, children, context);
			break;

		case T_MergeJoinState:
			mark_MergeJoin((MergeJoinState *) node, children, context);
			break;

		case T_HashJoinState:
			mark_HashJoin((HashJoinState *) node, children, context);
			break;

		/* hash nodes are part of theit child's pipelines */
		case T_HashState:
			mark_Hash((HashState *) node, children, context);
			break;

		/* these nodes are blocking, and so they start their own pipeline */
		case T_SortState:
		case T_GroupState:
		case T_AggState:
		case T_WindowAggState:
		case T_UniqueState:
		case T_SetOpState:
		case T_LockRowsState:
		case T_LimitState:
			instr->pipeline_id = (*current)++;
			instr->is_driver = true;
			break;

		/* handle the remaining nodes somehow */
		default:
			mark_dummy(node, children, context);
			break;
	}
}


static void
renumber_pipelines_walker(PlanState *node, List *children, void *context)
{
	ProgressInstr				*instr = PROGRESS_INSTR(node);
	RenumberPipelinesContext	*ctx   = context;

	if (!bms_is_member(instr->pipeline_id, ctx->seen))
	{
		ctx->pipeline_ids[instr->pipeline_id] = bms_num_members(ctx->seen);
		ctx->seen = bms_add_member(ctx->seen, instr->pipeline_id);
	}

	instr->pipeline_id = ctx->pipeline_ids[instr->pipeline_id];
}



static void
planner_estimates_walker(PlanState *node, List *children, void *context)
{
	Plan				*plan  = node->plan;
	ProgressInstr		*instr = PROGRESS_INSTR(node);
	ListCell			*lc;

	instr->tup_estimated = plan->plan_rows * instr->loops_estimated;

	foreach(lc, children)
	{
		PlanState		*child = (PlanState *) lfirst(lc);
		ProgressInstr	*childinstr = PROGRESS_INSTR(child);

		childinstr->loops_estimated = instr->loops_estimated;
	}

	if (nodeTag(node) == T_NestLoopState)
	{
		NestLoopState	*nstate = (NestLoopState *) node;
		Plan			*outerplan;
		ProgressInstr	*innerinstr;

		outerplan = outerPlanState(nstate)->plan;
		innerinstr = PROGRESS_INSTR(innerPlanState(nstate));

		innerinstr->loops_estimated *= outerplan->plan_rows;
	}
}


void
find_planner_estimates(PlanState *top, ProgressState *pstate)
{
	ProgressInstr		*instr = PROGRESS_INSTR(top);

	instr->loops_estimated = 1.0;
	plan_state_walker_preorder(top, planner_estimates_walker, NULL);
}


void
find_pipelines(PlanState *top, ProgressState *pstate)
{
	int							 current;
	RenumberPipelinesContext	 ctx;

	plan_state_walker(top, find_pipelines_walker, &current);

	ctx.pipeline_ids = palloc((current + 1) * sizeof(int));
	ctx.seen = NULL;

	plan_state_walker(top, renumber_pipelines_walker, &ctx);

	pstate->no_pipelines = bms_num_members(ctx.seen);

	pfree(ctx.pipeline_ids);
	bms_free(ctx.seen);
}
