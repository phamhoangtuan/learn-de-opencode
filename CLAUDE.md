# Project Instructions

## Subagent Policy

This project uses a two-tier model strategy:
- **Orchestration (this session)**: claude-opus-4-6 — complex planning, decision-making, synthesis
- **Subagents (Task tool)**: claude-sonnet-4-6 — research, execution, parallel work

**Rules:**
1. Whenever work can be parallelized or delegated, prefer spawning subagents via the Task tool over sequential execution.
2. ALWAYS specify `model: "claude-sonnet-4-6"` on every Task tool invocation.
3. Wait for all parallel subagents to complete before synthesizing their results.
