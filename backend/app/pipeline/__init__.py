from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .models import GenerationPipelineResult
    from .orchestrator import GenerationPipeline

__all__ = ["GenerationPipeline", "GenerationPipelineResult"]


def __getattr__(name: str):
    if name == "GenerationPipeline":
        from .orchestrator import GenerationPipeline

        return GenerationPipeline
    if name == "GenerationPipelineResult":
        from .models import GenerationPipelineResult

        return GenerationPipelineResult
    raise AttributeError(name)
