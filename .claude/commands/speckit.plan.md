---
description: Execute the implementation planning workflow using the plan template to generate design artifacts.
---

## User Input

```text
$ARGUMENTS
```

You **MUST** consider the user input before proceeding (if not empty).

## Outline

1. **Setup**: Run `.specify/scripts/bash/setup-plan.sh --json` from repo root and parse JSON for FEATURE_SPEC, IMPL_PLAN, SPECS_DIR, BRANCH. For single quotes in args like "I'm Groot", use escape syntax: e.g 'I'\''m Groot' (or double-quote if possible: "I'm Groot").

2. **Load context**: Read FEATURE_SPEC and `.specify/memory/constitution.md`. Load IMPL_PLAN template (already copied).

3. **Execute plan workflow**: Follow the structure in IMPL_PLAN template to:
   - Fill Technical Context (mark unknowns as "NEEDS CLARIFICATION")
   - Fill Constitution Check section from constitution
   - Evaluate gates (ERROR if violations unjustified)
   - Phase 0: Generate research.md (resolve all NEEDS CLARIFICATION)
   - Phase 1: Generate data-model.md, contracts/, quickstart.md
   - Phase 1: Update agent context by running the agent script
   - Re-evaluate Constitution Check post-design

4. **Stop and report**: Command ends after Phase 2 planning. Report branch, IMPL_PLAN path, and generated artifacts.

## Phases

### Phase 0: Outline & Research

1. **Extract unknowns from Technical Context** above:
   - For each NEEDS CLARIFICATION → research task
   - For each dependency → best practices task
   - For each integration → patterns task

2. **Generate and dispatch research agents**:

   Dispatch ALL research unknowns as **parallel** subagents in a **single message** using the Task tool. For each unknown or technology choice, invoke:

   ```
   Task tool:
     subagent_type: "general-purpose"
     model: "claude-sonnet-4-6"
     prompt: "Research {unknown} for {feature context}. Return findings as:
              - Decision: [what is recommended]
              - Rationale: [why]
              - Alternatives: [what else was evaluated]"
   ```

   Rules:
   - ALL unknowns must be dispatched in a single message (parallel, not sequential)
   - ALWAYS set `model: "claude-sonnet-4-6"` on every Task invocation
   - Do NOT run research sequentially inline — always use the Task tool
   - Wait for ALL subagents to complete before proceeding to Step 3

3. **Consolidate findings** in `research.md` using format:
   - Decision: [what was chosen]
   - Rationale: [why chosen]
   - Alternatives considered: [what else evaluated]

**Output**: research.md with all NEEDS CLARIFICATION resolved

### Phase 1: Design & Contracts

**Prerequisites:** `research.md` complete

1. **Extract entities from feature spec** → `data-model.md`:
   - Entity name, fields, relationships
   - Validation rules from requirements
   - State transitions if applicable

2. **Generate API contracts** from functional requirements:
   - For each user action → endpoint
   - Use standard REST/GraphQL patterns
   - Output OpenAPI/GraphQL schema to `/contracts/`

3. **Agent context update**:
   - Run `.specify/scripts/bash/update-agent-context.sh opencode`
   - These scripts detect which AI agent is in use
   - Update the appropriate agent-specific context file
   - Add only new technology from current plan
   - Preserve manual additions between markers

**Parallel dispatch**: `data-model.md` extraction and `contracts/` generation are independent — dispatch them as parallel Sonnet subagents in a single message once `research.md` is complete:

   ```
   Task tool (parallel, single message):
     [1] subagent_type: "general-purpose", model: "claude-sonnet-4-6"
         prompt: "Extract entities from {FEATURE_SPEC} and write data-model.md to {FEATURE_DIR}/data-model.md"
     [2] subagent_type: "general-purpose", model: "claude-sonnet-4-6"
         prompt: "Generate API contracts from {FEATURE_SPEC} functional requirements. Write OpenAPI/GraphQL schema to {FEATURE_DIR}/contracts/"
   ```

   Wait for both to complete before running the agent context update (Step 3 above).

**Output**: data-model.md, /contracts/*, quickstart.md, agent-specific file

## Key rules

- Use absolute paths
- ERROR on gate failures or unresolved clarifications
