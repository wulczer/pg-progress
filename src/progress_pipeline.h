#ifndef PROGRESS_PIPELINE_H
#define PROGRESS_PIPELINE_H

#include "nodes/pg_list.h"
#include "nodes/execnodes.h"

#include "progress_util.h"

typedef struct PipelineData {
	double		 tup_processed;
	double		 tup_estimated;
	List		*driver_nodes;
} PipelineData;

void find_pipelines(PlanState *top, ProgressState *pstate);
void find_planner_estimates(PlanState *top, ProgressState *pstate);

#endif   /* PROGRESS_PIPELINE_H */
