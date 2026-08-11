// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Basic custom worker-selection policy example.

use std::sync::Arc;

use dynamo_kv_router::services::selection::{
    WorkerSelectionPolicyFactory, WorkerSelectionPolicyParameters,
    WorkerSelectionPolicyProviderError, WorkerSelectionPolicyRegistry,
    WorkerSelectionPolicyRegistryError,
};
use dynamo_kv_router::{
    KvRouterConfig, WorkerCandidate, WorkerInputView, WorkerInputs, WorkerPicker, WorkerScorer,
    WorkerSelectionContext, WorkerSelectionInputTrigger, WorkerSelectionPolicy,
    WorkerSelectionPolicyError,
};

struct LeastBusyScorer;

impl WorkerScorer for LeastBusyScorer {
    fn required_worker_inputs(&self) -> WorkerInputs {
        WorkerInputs::LOAD
    }

    fn score(
        &mut self,
        _context: &WorkerSelectionContext<'_>,
        candidate: &WorkerCandidate,
    ) -> Result<f64, WorkerSelectionPolicyError> {
        let load = candidate
            .load()
            .ok_or_else(|| WorkerSelectionPolicyError::failed("load input unavailable"))?;
        Ok(load.active_requests() as f64)
    }
}

struct UncachedBlocksScorer;

impl WorkerScorer for UncachedBlocksScorer {
    fn required_worker_inputs(&self) -> WorkerInputs {
        WorkerInputs::CACHE
    }

    fn score(
        &mut self,
        context: &WorkerSelectionContext<'_>,
        candidate: &WorkerCandidate,
    ) -> Result<f64, WorkerSelectionPolicyError> {
        let cache = candidate
            .cache()
            .ok_or_else(|| WorkerSelectionPolicyError::failed("cache input unavailable"))?;
        Ok((context.request_blocks() as f64 - cache.device_overlap_blocks()).max(0.0))
    }
}

struct RequestAwarePicker;

impl WorkerPicker for RequestAwarePicker {
    fn required_worker_inputs(&self) -> WorkerInputs {
        WorkerInputs::CACHE
    }

    fn pick(
        &mut self,
        context: &WorkerSelectionContext<'_>,
        input: WorkerInputView<'_>,
    ) -> Result<usize, WorkerSelectionPolicyError> {
        if context
            .session_context()
            .and_then(|session| session.input_trigger())
            == Some(WorkerSelectionInputTrigger::ToolResult)
        {
            return input
                .cache()
                .ok_or_else(|| WorkerSelectionPolicyError::failed("cache input unavailable"))?
                .iter()
                .enumerate()
                .max_by(|(_, left), (_, right)| {
                    left.device_overlap_blocks()
                        .total_cmp(&right.device_overlap_blocks())
                })
                .map(|(row, _)| row)
                .ok_or_else(|| WorkerSelectionPolicyError::failed("no eligible worker"));
        }

        input
            .candidates()
            .iter()
            .enumerate()
            .min_by(|(_, left), (_, right)| left.cost().total_cmp(&right.cost()))
            .map(|(row, _)| row)
            .ok_or_else(|| WorkerSelectionPolicyError::failed("no eligible worker"))
    }
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Parameters {}

fn provider(
    parameters: &WorkerSelectionPolicyParameters,
) -> Result<WorkerSelectionPolicyFactory, WorkerSelectionPolicyProviderError> {
    let _: Parameters = parameters.deserialize()?;

    Ok(Arc::new(
        |config: &KvRouterConfig, worker_type, _partition| {
            WorkerSelectionPolicy::new(
                config.clone(),
                worker_type,
                vec![Box::new(LeastBusyScorer), Box::new(UncachedBlocksScorer)],
                Box::new(RequestAwarePicker),
            )
        },
    ))
}

pub fn register(
    registry: &mut WorkerSelectionPolicyRegistry,
) -> Result<(), WorkerSelectionPolicyRegistryError> {
    registry.register("least-busy", Arc::new(provider))
}
