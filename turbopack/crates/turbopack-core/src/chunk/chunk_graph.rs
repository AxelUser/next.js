use anyhow::{Context, Result};
use turbo_tasks::{FxIndexMap, FxIndexSet, ResolvedVc};

use super::{availability_info, ChunkContentResult, ChunkableModule, ChunkingType};
use crate::{
    module::Module,
    module_graph::{GraphTraversalAction, SingleModuleGraph},
};

#[turbo_tasks::value(serialization = "none", cell = "new", eq = "manual")]
pub struct ChunkGraph {
    graph: ResolvedVc<SingleModuleGraph>,
}

// impl PartialEq for ChunkGraph {
//     fn eq(&self, other: &Self) -> bool {
//         std::ptr::eq(&self.graph, &other.graph)
//     }
// }

impl ChunkGraph {
    pub fn new(graph: ResolvedVc<SingleModuleGraph>) -> Self {
        Self { graph }
    }

    pub async fn chunk_content(
        &self,
        chunk_group_entries: impl IntoIterator<Item = ResolvedVc<Box<dyn Module>>>,
        availability_info: availability_info::AvailabilityInfo,
        can_split_async: bool,
        should_trace: bool,
    ) -> Result<ChunkContentResult> {
        struct TraverseState {
            result: ChunkContentResult,
        }

        let mut state = TraverseState {
            result: ChunkContentResult {
                chunkable_modules: FxIndexSet::default(),
                async_modules: FxIndexSet::default(),
                traced_modules: FxIndexSet::default(),
                passthrough_modules: FxIndexSet::default(),
                forward_edges_inherit_async: FxIndexMap::default(),
                local_back_edges_inherit_async: FxIndexMap::default(),
                available_async_modules_back_edges_inherit_async: FxIndexMap::default(),
            },
        };

        let graph = self.graph.await?;

        for entry in chunk_group_entries {
            graph
                .traverse_edges_from_entry_topological_async(
                    entry,
                    &mut state,
                    async |parent_info, node, TraverseState { result }| {
                        let chunkable_module =
                            ResolvedVc::try_sidecast::<Box<dyn ChunkableModule>>(node.module)
                                .await?;

                        let Some(chunkable_module) = chunkable_module else {
                            return Ok(GraphTraversalAction::Skip);
                        };

                        let Some((parent_node, edge)) = parent_info else {
                            result.chunkable_modules.insert(chunkable_module);
                            return Ok(GraphTraversalAction::Continue);
                        };

                        let parent_module = ResolvedVc::try_sidecast::<Box<dyn ChunkableModule>>(
                            parent_node.module,
                        )
                        .await?
                        .context("Expected parent module to be chunkable")?;

                        if let Some(available_modules) = availability_info.available_modules() {
                            let info = *available_modules.get(*chunkable_module).await?;
                            if let Some(info) = info {
                                if info.is_async {
                                    result
                                        .forward_edges_inherit_async
                                        .entry(parent_module)
                                        .or_insert_with(|| vec![])
                                        .push(chunkable_module);

                                    result
                                        .available_async_modules_back_edges_inherit_async
                                        .entry(chunkable_module)
                                        .or_insert_with(|| vec![])
                                        .push(parent_module);
                                }
                                return Ok(GraphTraversalAction::Continue);
                            }
                        }

                        Ok(match edge {
                            ChunkingType::Passthrough => {
                                result.passthrough_modules.insert(chunkable_module);
                                GraphTraversalAction::Continue
                            }
                            ChunkingType::Parallel => {
                                result.chunkable_modules.insert(chunkable_module);
                                GraphTraversalAction::Continue
                            }
                            ChunkingType::ParallelInheritAsync => {
                                result.chunkable_modules.insert(chunkable_module);
                                result
                                    .local_back_edges_inherit_async
                                    .entry(chunkable_module)
                                    .or_insert_with(|| vec![])
                                    .push(parent_module);
                                result
                                    .forward_edges_inherit_async
                                    .entry(parent_module)
                                    .or_insert_with(|| vec![])
                                    .push(chunkable_module);

                                GraphTraversalAction::Continue
                            }
                            ChunkingType::Async => {
                                if can_split_async {
                                    result.async_modules.insert(chunkable_module);
                                } else {
                                    result.chunkable_modules.insert(chunkable_module);
                                }
                                GraphTraversalAction::Skip
                            }
                            ChunkingType::Traced => {
                                if should_trace {
                                    result.traced_modules.insert(node.module);
                                }
                                GraphTraversalAction::Skip
                            }
                            ChunkingType::Isolated { .. } => GraphTraversalAction::Skip,
                        })
                    },
                    |_, _, _| (),
                )
                .await?;
        }

        Ok(state.result)
    }
}
