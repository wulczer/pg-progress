/*------------------------------------------------------------------------
 *
 * progress_util.c
 *	   utility functinons for dynamic query progress calculation
 *
 * Copyright (c) 2013, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "nodes/pg_list.h"
#include "nodes/execnodes.h"

#include "progress_util.h"


static List *
list_ps(PlanState **planstates, int n)
{
	int			 i;
	List		*result = NIL;

	for (i = 0; i < n; i++)
	{
		result = lappend(result, planstates[i]);
	}

	return result;
}


static List *
subplan_ps(List *plans)
{
	ListCell	*lc;
	List		*result = NIL;

	foreach(lc, plans)
	{
		SubPlanState	*state = (SubPlanState *) lfirst(lc);

		result = lappend(result, state->planstate);
	}

	return result;
}


/*
 * Walk a PlanState structure, calling the walker function on each node,
 * passing it a list of that node's PlanState children and an optional pointer
 * to a context structure.
 */
static PlanState *
plan_state_walker_common(PlanState *node, ps_walker_type walker,
						 void *context, bool postorder)
{
	Plan		*plan = node->plan;
	List		*children;
	List		*extra;
	ListCell	*lc;
	int			n;

	children = NIL;
	extra = NIL;

	if (node->initPlan)
		children = list_concat(children, subplan_ps(node->initPlan));

	if (node->subPlan)
		children = list_concat(children, subplan_ps(node->subPlan));

	if (outerPlanState(node))
		children = lappend(children, outerPlanState(node));

	if (innerPlanState(node))
		children = lappend(children, innerPlanState(node));

	switch (nodeTag(plan))
	{
		case T_ModifyTable:
			n = list_length(((ModifyTable *) plan)->plans);
			extra = list_ps(((ModifyTableState *) node)->mt_plans, n);
			break;
		case T_Append:
			n = list_length(((Append *) plan)->appendplans);
			extra = list_ps(((AppendState *) node)->appendplans, n);
			break;
		case T_MergeAppend:
			n = list_length(((MergeAppend *) plan)->mergeplans);
			extra = list_ps(((MergeAppendState *) node)->mergeplans, n);
			break;
		case T_BitmapAnd:
			n = list_length(((BitmapAnd *) plan)->bitmapplans);
			extra = list_ps(((BitmapAndState *) node)->bitmapplans, n);
			break;
		case T_BitmapOr:
			n = list_length(((BitmapOr *) plan)->bitmapplans);
			extra = list_ps(((BitmapOrState *) node)->bitmapplans, n);
			break;
		case T_SubqueryScan:
			children = lappend(children,
							   ((SubqueryScanState *) node)->subplan);
			break;
		default:
			break;
	}

	children = list_concat(children, extra);

	if (!postorder)
		(*walker)(node, children, context);

	foreach(lc, children)
	{
		PlanState		*child = (PlanState *) lfirst(lc);

		plan_state_walker_common(child, walker, context, postorder);
	}

	if (postorder)
		(*walker)(node, children, context);

	list_free(children);

	return node;
}


/* the common case is postorder traversal */
PlanState *
plan_state_walker(PlanState *node, ps_walker_type walker, void *context)
{
	return plan_state_walker_common(node, walker, context, true);
}


PlanState *
plan_state_walker_preorder(PlanState *node, ps_walker_type walker,
						   void *context)
{
	return plan_state_walker_common(node, walker, context, false);
}

/* PlanState node to human readable name */
char *
plan_node_name(PlanState *node)
{
	switch (nodeTag(node))
	{
		case T_ResultState:
			return "Result";
		case T_ModifyTableState:
			return "ModifyTable";
		case T_AppendState:
			return "Append";
		case T_MergeAppendState:
			return "MergeAppend";
		case T_RecursiveUnionState:
			return "RecursiveUnion";
		case T_BitmapAndState:
			return "BitmapAnd";
		case T_BitmapOrState:
			return "BitmapOr";
		case T_ScanState:
			return "Scan";
		case T_SeqScanState:
			return "SeqScan";
		case T_IndexScanState:
			return "IndexScan";
		case T_IndexOnlyScanState:
			return "IndexOnlyScan";
		case T_BitmapIndexScanState:
			return "BitmapIndexScan";
		case T_BitmapHeapScanState:
			return "BitmapHeapScan";
		case T_TidScanState:
			return "TidScan";
		case T_SubqueryScanState:
			return "SubqueryScan";
		case T_FunctionScanState:
			return "FunctionScan";
		case T_ValuesScanState:
			return "ValuesScan";
		case T_CteScanState:
			return "CteScan";
		case T_WorkTableScanState:
			return "WorkTableScan";
		case T_ForeignScanState:
			return "ForeignScan";
		case T_JoinState:
			return "Join";
		case T_NestLoopState:
			return "NestLoop";
		case T_MergeJoinState:
			return "MergeJoin";
		case T_HashJoinState:
			return "HashJoin";
		case T_MaterialState:
			return "Material";
		case T_SortState:
			return "Sort";
		case T_GroupState:
			return "Group";
		case T_AggState:
			return "Agg";
		case T_WindowAggState:
			return "WindowAgg";
		case T_UniqueState:
			return "Unique";
		case T_HashState:
			return "Hash";
		case T_SetOpState:
			return "SetOp";
		case T_LockRowsState:
			return "LockRows";
		case T_LimitState:
			return "Limit";
		default:
			return "???";
	}
}
