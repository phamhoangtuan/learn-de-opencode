<!--
Sync Impact Report
- Version change: N/A → 1.0.0
- List of modified principles (old title → new title if renamed):
  - [PRINCIPLE_1_NAME] → I. Data Quality First (DAMA-DMBOK Area 11)
  - [PRINCIPLE_2_NAME] → II. Metadata & Lineage (DAMA-DMBOK Area 10)
  - [PRINCIPLE_3_NAME] → III. Security & Privacy by Design (DAMA-DMBOK Area 5)
  - [PRINCIPLE_4_NAME] → IV. Integration & Interoperability (DAMA-DMBOK Area 6)
  - [PRINCIPLE_5_NAME] → V. Architecture & Modeling Integrity (DAMA-DMBOK Area 2 & 3)
- Added sections: Data Storage and Operations, Data Governance and Ethics
- Removed sections: None
- Templates requiring updates (✅ updated / ⚠ pending):
  - .specify/templates/plan-template.md (✅ updated)
  - .specify/templates/spec-template.md (✅ updated)
  - .specify/templates/tasks-template.md (✅ updated)
- Follow-up TODOs if any placeholders intentionally deferred: None
-->

# Expert Data Engineering Constitution

## Core Principles

### I. Data Quality First (DAMA-DMBOK Area 11)
Data quality must be measured, monitored, and maintained throughout the lifecycle. Automated 
validation and profiling are non-negotiable before data enters any curated zone (Warehouse/Lake). 
Data quality metrics MUST be part of the pipeline success criteria.

### II. Metadata & Lineage (DAMA-DMBOK Area 10)
Every pipeline must emit metadata including lineage, schema, and business logic. Pipelines without 
clear ownership, documentation, and lineage are considered broken. Metadata should be treated as 
a first-class citizen of the data platform.

### III. Security & Privacy by Design (DAMA-DMBOK Area 5)
Data security is integrated into every component. Encryption at rest and in transit, RBAC, and 
PII masking are mandatory and MUST be verified by automated tests. Compliance with data protection 
regulations (e.g., GDPR, CCPA) is built-in, not bolted-on.

### IV. Integration & Interoperability (DAMA-DMBOK Area 6)
Favor standardized exchange formats (Parquet, Avro, JSON) and robust interface contracts. Decouple 
producers from consumers using messaging or well-defined APIs. Interoperability MUST be prioritized 
to prevent vendor lock-in and siloes.

### V. Architecture & Modeling Integrity (DAMA-DMBOK Area 2 & 3)
Align data physical structures with enterprise data architecture. Every dataset must follow a 
vetted model (Star Schema, Data Vault, etc.) and adhere to defined naming conventions. Schema 
evolution MUST be handled gracefully without breaking downstream consumers.

## Data Storage and Operations (DAMA-DMBOK Area 4)
Manage the data lifecycle from ingestion to disposal. Ensure high availability, disaster recovery, 
and cost optimization of storage systems. Database operations must be automated (Infrastructure as 
Code) to ensure consistency and repeatability.

## Data Governance and Ethics (DAMA-DMBOK Area 1)
Adhere to organizational data policies. Data ethics must guide all engineering decisions, 
ensuring transparency, fairness, and accountability in data handling. Data must be treated as a 
valuable enterprise asset.

## Governance
1. This constitution defines the engineering standards for the project.
2. All pull requests MUST be validated against these principles.
3. Amendments require a consensus review and a version bump.
4. Any deviation MUST be documented in the Implementation Plan under "Complexity Tracking".

**Version**: 1.0.0 | **Ratified**: 2026-02-17 | **Last Amended**: 2026-02-17
