#ifndef PROGRESS_UTIL_H
#define PROGRESS_UTIL_H

#include "nodes/execnodes.h"

#define PROGRESS_INSTR(node) ((ProgressInstr *) ((PlanState *) (node))->instrument->private)

typedef struct ProgressState
{
	int	no_pipelines;
} ProgressState;

typedef struct ProgressInstr {
	int		pipeline_id;
	bool	is_driver;
	double	tup_estimated;
	double	loops_estimated;
	bool	finished;
} ProgressInstr;

typedef void (*ps_walker_type) (PlanState *node, List *children, void *context);

PlanState *plan_state_walker(PlanState *node, ps_walker_type walker, void *context);
PlanState *plan_state_walker_preorder(PlanState *node, ps_walker_type walker, void *context);

char *plan_node_name(PlanState *node);

#endif   /* PROGRESS_UTIL_H */
