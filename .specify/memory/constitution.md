<!--
  SYNC IMPACT REPORT
  ==================
  Version: 1.0.0 → 2.0.0 (MAJOR)
  Change Type: DMBOK FRAMEWORK INTEGRATION
  Amendment Date: 2026-02-15
  
  BREAKING CHANGES:
  - Restructured from 5 to 7 core principles
  - Added 8 DMBOK Knowledge Area Guidelines sections
  - Introduced convention-specific code quality standards
  - Expanded governance and technical constraints
  
  Principles Modified:
  - I. Modularity & Reusability → Enhanced with DMBOK Data Architecture practices
  - II. Data Quality First → Expanded to "Data Quality & Governance" (merged with DMBOK frameworks)
  - III. Test-First Development → No changes (NON-NEGOTIABLE status maintained)
  - IV. Observability & Reproducibility → Restructured to "Metadata & Lineage" (DMBOK Metadata Management focus)
  - VII. Simplicity & Performance → Enhanced with operational excellence practices
  
  Principles Added:
  - V. Data Security & Privacy (NEW) - DMBOK Data Security principles mandatory
  - VI. Data Architecture & Integration (NEW) - DMBOK-aligned pipeline design patterns
  
  Sections Added:
  - DMBOK Knowledge Area Guidelines (8 focused areas)
    * Data Governance
    * Data Architecture
    * Data Modeling & Design
    * Data Storage & Operations
    * Data Security
    * Data Integration & Interoperability
    * Metadata Management
    * Data Quality
  - Code Quality & Maintainability Standards (readability, documentation, maintainability, reusability)
  
  Sections Renamed/Expanded:
  - "Data Engineering Constraints" → "Technical Standards & Constraints" (DMBOK-aligned technology choices)
  - "Development Workflow" → Expanded with CI/CD, monitoring, DMBOK compliance verification
  
  Templates Requiring Updates:
  ⚠️ .specify/templates/plan-template.md - Constitution Check requires 7-principle + 8 DMBOK gates
  ⚠️ .specify/templates/spec-template.md - Add Metadata, Security, Lineage requirement sections
  ⚠️ .specify/templates/tasks-template.md - Add DMBOK-specific task categories
  ✨ .specify/templates/dmbok-checklist-template.md - New DMBOK compliance checklist (to be created)
  
  Backward Compatibility Impact:
  - Existing specs/plans must be reviewed against new 7 principles
  - Data Security & Privacy principle introduces new mandatory requirements
  - Code quality standards now convention-specific (Python PEP 8)
  - DMBOK compliance adds review overhead but maintains learning-focused flexibility
  
  Follow-up TODOs: Template updates in progress
-->

# Data Engineering Learning Constitution

**Framework Alignment**: DAMA-DMBOK 2.0  
**Focus**: Building robust data pipelines with emphasis on code readability, understandability, 
maintainability, and reusability

---

## Core Principles

### I. Modularity & Reusability

Every data processing capability MUST be built as a standalone, reusable component aligned 
with DMBOK Data Architecture principles:

**Component Design**:
- Each component MUST be self-contained with clear, documented interfaces (inputs/outputs)
- Components MUST be independently testable without external dependencies
- Each component MUST have a single, well-defined purpose (Single Responsibility Principle)
- Components MUST be framework-agnostic where possible to enable reuse across contexts

**Interface Standards**:
- CLI interfaces MUST be provided for all components (stdin/args → stdout, errors → stderr)
- Components MUST support both JSON (machine-readable) and human-readable output formats
- APIs MUST follow consistent patterns (REST principles, versioned endpoints)

**Reusability Practices**:
- No organizational-only libraries; every library must provide concrete, reusable functionality
- Common transformations MUST be extracted into shared utility libraries (`src/lib/`)
- Configuration MUST be externalized (YAML/JSON files, environment variables)
- Components MUST be parameterizable for different execution contexts

**Architecture Patterns**:
- Design for scalability: components should handle growing data volumes gracefully
- Use layered architecture: ingestion → transformation → quality → output
- Document integration points and dependencies explicitly
- Follow DMBOK reference architecture patterns for pipeline design

**Rationale**: Modularity enables learning through composition, makes components portable 
across different data engineering contexts, and facilitates incremental understanding of 
complex data pipelines. Reusable components accelerate future development and reduce 
technical debt.

---

### II. Data Quality & Governance

Data validation, quality management, and governance MUST be first-class concerns throughout 
the data lifecycle, aligned with DMBOK Data Quality and Data Governance knowledge areas:

**Data Quality (6 Quality Dimensions)**:
- **Accuracy**: Data correctly represents real-world entities and values
- **Completeness**: All required data present, no unexpected nulls, all expected records exist
- **Consistency**: Same data across systems matches, referential integrity maintained
- **Timeliness**: Data available when needed, freshness tracked with SLAs
- **Validity**: Data conforms to defined formats, ranges, business rules
- **Uniqueness**: No unintended duplicates, primary keys properly enforced

**Schema & Validation**:
- All data pipelines MUST define explicit schemas for inputs and outputs
- Schema validation MUST occur at component boundaries (fail fast on bad data)
- Data contracts MUST be documented, versioned, and published alongside code
- Quality checks MUST be explicit in pipeline logic, not implicit assumptions

**Quality Monitoring**:
- Invalid data MUST be logged with clear error messages indicating which rule failed
- Data quality metrics MUST be captured and made visible (records processed, validation 
  failures, data drift indicators, pass rates)
- Quality SLAs MUST be defined and tracked (e.g., "99% pass rate", "< 1hr latency")
- Quality dashboards SHOULD visualize trends over time

**Data Governance**:
- Data ownership MUST be clearly defined (who is accountable for quality)
- Data policies MUST be documented (retention, privacy, access control)
- Business glossary SHOULD define key terms and metrics consistently
- Data stewardship roles SHOULD be assigned for critical datasets
- Compliance requirements (GDPR, CCPA concepts) MUST be understood even in learning context

**Rationale**: Data engineering systems fail silently when quality is not enforced. Explicit 
validation surfaces issues early, builds confidence in data outputs, and establishes good 
habits for production systems. Governance ensures accountability and regulatory compliance.

---

### III. Test-First Development (NON-NEGOTIABLE)

TDD is MANDATORY for all feature development - no exceptions:

**TDD Process**:
- Tests MUST be written before implementation code
- Test workflow: Write test → User/spec approval → Test fails (red) → Implement → Test 
  passes (green) → Refactor for quality
- Red-Green-Refactor cycle MUST be strictly followed for all new functionality
- No implementation code without corresponding failing tests first

**Test Coverage**:
- Minimum 80% code coverage for core pipeline logic
- Unit tests for individual functions and transformations
- Integration tests for component interactions and data flows
- Contract tests for external interfaces and APIs
- Data quality tests for validation rules

**Test Types Required**:
- **Unit Tests**: Functions, transformations, business logic
- **Integration Tests**: Component interactions, end-to-end pipeline flows
- **Contract Tests**: API endpoints, data schemas, external dependencies
- **Data Quality Tests**: Validation rules, quality dimensions, edge cases

**Test Standards**:
- Test naming: `test_<function>_<scenario>_<expected_outcome>()`
- Use fixtures for common test data and setup
- Tests MUST be isolated (no shared state between tests)
- Tests MUST be deterministic (same input = same result always)
- Commit messages SHOULD reference test coverage added

**Rationale**: TDD enforces clarity of requirements before coding, prevents scope creep, 
ensures testability by design, and builds regression safety as the project grows. Writing 
tests first forces clear thinking about interfaces and edge cases.

---

### IV. Metadata & Lineage

All data operations MUST capture comprehensive metadata and maintain complete lineage, 
aligned with DMBOK Metadata Management knowledge area:

**Metadata Types**:
- **Technical Metadata**: Schemas, data types, formats, file sizes, row counts
- **Business Metadata**: Definitions, ownership, business rules, quality dimensions
- **Operational Metadata**: Run timestamps, execution duration, parameters used, success/failure status
- **Lineage Metadata**: Source systems, transformations applied, target systems, dependencies

**Lineage Tracking**:
- Data lineage MUST be traceable end-to-end: source → transformations → output
- Every transformation MUST document: input datasets, logic applied, output datasets
- Lineage MUST include: dataset versions, code versions, execution timestamps
- Lineage visualization SHOULD be maintained (DAG diagrams, flow charts)

**Logging Standards**:
- Every pipeline run MUST log: start time, end time, input/output counts, parameters used, 
  exit status
- Structured logging (JSON format) MUST be used for all operational logs
- Log levels MUST be appropriate: DEBUG (detailed), INFO (operational), WARN (issues), 
  ERROR (failures)
- Logs MUST include correlation IDs to trace related operations

**Reproducibility**:
- Pipeline components MUST be deterministic: same input + parameters = same output
- Data transformations MUST be versioned (code version + schema version tracked)
- Idempotency MUST be maintained: running the same pipeline multiple times produces same result
- Configuration and parameters MUST be captured in run metadata

**Observability**:
- Text-based I/O ensures debuggability: inputs/outputs must be inspectable without special tools
- Processing steps MUST emit progress indicators for long-running operations
- Metrics MUST be captured: throughput, latency, resource utilization
- Data catalogs SHOULD index datasets with searchable metadata

**Rationale**: Metadata enables discovery, understanding, and trust in data assets. Lineage 
enables debugging, impact analysis, and compliance. Reproducibility ensures experiments can 
be repeated, results verified, and issues diagnosed reliably.

---

### V. Data Security & Privacy

All data pipelines MUST implement security and privacy by design, aligned with DMBOK Data 
Security knowledge area:

**Encryption Standards**:
- Data at rest MUST be encrypted (storage encryption enabled)
- Data in transit MUST be encrypted (TLS 1.2+ for network communication)
- Database connections MUST use encrypted protocols
- Encryption keys MUST be rotated according to policy

**Access Control**:
- Role-based access control (RBAC) MUST restrict pipeline access to authorized users
- Principle of least privilege: grant minimum necessary permissions
- Access permissions MUST be reviewed regularly
- Service accounts MUST have dedicated credentials (no shared accounts)

**Secret Management**:
- Credentials, API keys, passwords MUST use secure vaults
- Minimum: environment variables (not hardcoded in code)
- Preferred: HashiCorp Vault, AWS Secrets Manager, Azure Key Vault, or similar
- Secrets MUST NOT be committed to version control (.gitignore enforced)

**PII Protection**:
- Personally Identifiable Information (PII) MUST be identified and classified
- PII MUST be masked or tokenized in non-production environments
- Data minimization: collect only necessary PII
- Retention policies: delete PII when no longer needed

**Audit & Compliance**:
- All data access MUST be logged: timestamps, user IDs, actions performed
- Audit logs MUST be tamper-proof (write-once storage or signed logs)
- Security events (failed logins, unauthorized access attempts) MUST trigger alerts
- Compliance awareness: understand GDPR, CCPA, HIPAA concepts even in learning context

**Secure Development**:
- Dependencies MUST be scanned for known vulnerabilities (e.g., `pip-audit`, `safety`)
- Code MUST be reviewed for security issues (SQL injection, path traversal, etc.)
- Input validation MUST sanitize untrusted data
- Error messages MUST NOT leak sensitive information

**Rationale**: Security breaches often stem from learning projects that graduate to production 
without security design. Building secure habits from day one prevents vulnerabilities and 
prepares for real-world deployment. Even learning projects should protect any real or realistic 
data.

**Learning Context**: For toy datasets, security can be simplified (local file permissions, 
basic authentication), but architecture must demonstrate understanding of production security 
patterns. Focus on establishing good habits rather than enterprise-grade implementation.

---

### VI. Data Architecture & Integration

Pipeline architecture and integration patterns MUST follow DMBOK Data Architecture and Data 
Integration & Interoperability principles:

**Architecture Patterns**:
- **Layered Architecture**: Separate ingestion, transformation, quality, output concerns
- **Microservices**: Decompose complex pipelines into independently deployable components
- **Event-Driven**: Use message queues for decoupled, asynchronous processing when appropriate
- **Batch vs. Streaming**: Choose based on latency requirements and data volume

**Integration Standards**:
- **ETL/ELT**: Extract-Transform-Load vs. Extract-Load-Transform based on processing location
- **CDC (Change Data Capture)**: Track incremental changes for efficient updates
- **API-First**: Design RESTful or GraphQL APIs for component integration
- **Message Queues**: Kafka, RabbitMQ, or cloud equivalents for event streaming
- **Data Virtualization**: Query federation when physical integration not required

**Design Principles**:
- Pipeline stages MUST be separable and runnable independently
- Avoid tight coupling: components should not depend on internal implementation details
- Use standard interfaces: agreed schemas, protocols, data formats
- Design for failure: retry logic, circuit breakers, graceful degradation
- Scalability: horizontal scaling preferred over vertical scaling

**Error Handling**:
- Error detection MUST be comprehensive (validation, runtime, external failures)
- Errors MUST be logged with context (which stage failed, input data, error message)
- Recovery strategies MUST be explicit: retry with backoff, skip and log, fail fast
- Dead letter queues for unprocessable records
- Alerting on repeated failures

**Technology Choices**:
- Prefer standard, well-established tools over custom implementations
- Avoid vendor lock-in: use open standards and abstraction layers
- Document technology decisions and trade-offs
- Isolate tool-specific code in adapters/wrappers

**Rationale**: Well-designed architecture enables scalability, maintainability, and reliability. 
Standard integration patterns reduce complexity and enable interoperability. DMBOK-aligned 
architecture prepares learning projects for production evolution.

---

### VII. Simplicity & Performance

Start simple, but remain performance-aware and focused on operational excellence:

**Simplicity Principles**:
- **YAGNI (You Aren't Gonna Need It)**: Build only what is needed for current learning objectives
- Avoid premature optimization: establish performance baselines before optimizing
- Simple solutions MUST be attempted before complex patterns (e.g., direct file processing 
  before introducing streaming frameworks)
- Complexity MUST be justified: document why simpler alternative was insufficient
- Prefer standard library and well-established tools over custom implementations

**Performance Practices**:
- **Measure First**: Use profiling to identify actual bottlenecks before optimizing
- Performance goals (if any) MUST be explicit and measurable (e.g., "process 10K records/sec", 
  "< 5min end-to-end latency")
- Establish baseline metrics early: throughput, latency, resource utilization
- Optimize hot paths: focus on code executed frequently or processing large volumes
- Use appropriate data structures: hash maps for lookups, generators for streaming

**Operational Excellence**:
- **Monitoring**: Track pipeline health, data freshness, error rates
- **Alerting**: Proactive notifications on failures, SLA violations, anomalies
- **Resource Management**: Monitor CPU, memory, disk usage; optimize when necessary
- **Cost Awareness**: Track cloud costs, storage usage, compute time
- **Continuous Improvement**: Regular reviews of performance, quality, maintainability

**Code Maintainability**:
- Readable code is more valuable than clever code
- Consistent style trumps personal preference (follow PEP 8 for Python)
- Refactor continuously: improve design as understanding grows
- Document trade-offs and technical debt
- Delete unused code promptly

**Rationale**: Learning projects benefit from simplicity - complex architectures obscure 
fundamentals. Performance awareness without premature optimization builds intuition for when 
optimization is actually needed. Operational excellence ensures pipelines remain reliable and 
maintainable as they evolve.

---

## DMBOK Knowledge Area Guidelines

This section provides practical guidance for applying DAMA-DMBOK framework principles to 
data pipeline development. Each knowledge area includes purpose, core practices, learning 
context guidance, and quality criteria.

**Learning Project Approach**: These guidelines are adapted for learning environments with 
flexibility for experimentation while building production-ready habits.

---

### Data Governance

**Purpose**: Establish accountability, policies, and standards for data management to ensure 
data is treated as a strategic asset with clear ownership and compliance.

**Core Practices**:

1. **Data Ownership & Stewardship**
   - Assign data owners for each dataset (who is accountable for quality and access)
   - Define steward roles for data quality monitoring and issue resolution
   - Document ownership in data catalogs or README files

2. **Data Policies & Standards**
   - Document data policies: retention periods, access rules, usage restrictions
   - Define naming conventions: datasets, tables, columns, files
   - Establish schema standards: required fields, data types, constraints
   - Create data classification taxonomy: public, internal, confidential, restricted

3. **Business Glossary**
   - Maintain definitions for key business terms and metrics
   - Ensure consistent terminology across pipelines and documentation
   - Link technical schemas to business definitions
   - Example: "Customer" = "Individual with at least one completed transaction in past 12 months"

4. **Compliance & Regulatory Awareness**
   - Understand applicable regulations even in learning context (GDPR, CCPA concepts)
   - Document data processing purposes and legal basis
   - Implement data subject rights: access, deletion, portability (where applicable)
   - Maintain audit trails for compliance verification

5. **Governance Processes**
   - Change management: review and approve schema changes, new data sources
   - Exception handling: document justified deviations from standards
   - Regular reviews: data quality, access permissions, policy compliance

**Learning Project Guidance**:
- Start with simple ownership documentation (README with owner name/contact)
- Create lightweight data dictionary in Markdown or spreadsheet
- Document key decisions and exceptions in decision logs
- Focus on establishing governance habits rather than heavyweight processes

**Quality Criteria**:
- [ ] Data ownership documented for all datasets
- [ ] Key business terms defined in glossary or documentation
- [ ] Data policies documented (retention, access, classification)
- [ ] Compliance requirements identified and documented
- [ ] Change management process defined for schema modifications

---

### Data Architecture

**Purpose**: Design scalable, flexible, and maintainable data pipeline architectures that 
support current and future business needs while enabling integration across systems.

**Core Practices**:

1. **Reference Architecture Patterns**
   - **Lambda Architecture**: Batch layer + speed layer for real-time + historical analytics
   - **Kappa Architecture**: Streaming-only for simplified real-time processing
   - **Data Lakehouse**: Combine data lake flexibility with data warehouse structure
   - **Data Mesh**: Decentralized domain-oriented data ownership
   - Choose pattern based on latency requirements, data volume, team skills

2. **Pipeline Design Patterns**
   - **Batch Processing**: Scheduled full or incremental loads (default for learning)
   - **Streaming**: Real-time event processing with Kafka, Kinesis, or Pub/Sub
   - **Micro-batching**: Small batches for near-real-time with batch simplicity
   - **CDC (Change Data Capture)**: Efficient incremental updates from databases
   - **Data Virtualization**: Query federation without physical movement

3. **Integration Layers**
   - **Ingestion Layer**: Extract data from sources, handle connectivity
   - **Transformation Layer**: Clean, enrich, aggregate, apply business logic
   - **Quality Layer**: Validate, profile, monitor data quality
   - **Storage Layer**: Persist data in appropriate formats and systems
   - **Serving Layer**: Expose data via APIs, dashboards, exports

4. **Scalability Design**
   - Horizontal scaling: add more workers rather than bigger machines
   - Partitioning: split data by key (date, region, customer) for parallel processing
   - Columnar formats: Parquet for analytics, Avro for row-based operations
   - Caching: materialized views, pre-aggregations for frequent queries
   - Compression: balance storage cost vs. CPU overhead

5. **Data Flow Documentation**
   - Create data flow diagrams (Mermaid, draw.io, or similar)
   - Document source systems, transformation steps, target systems
   - Identify dependencies between pipelines
   - Maintain architecture decision records (ADRs) for key choices

**Learning Project Guidance**:
- Start with simple batch processing before attempting streaming
- Use file-based storage (Parquet, JSON) before databases
- Draw simple flow diagrams for even small pipelines (builds good habits)
- Experiment with one architecture pattern per project to understand trade-offs

**Quality Criteria**:
- [ ] Architecture pattern chosen and documented (batch/streaming/hybrid)
- [ ] Pipeline layers clearly separated (ingestion, transformation, quality, storage)
- [ ] Data flow diagram exists showing sources, transformations, targets
- [ ] Scalability considerations documented (even if "not optimized yet")
- [ ] Key architecture decisions recorded with rationale

---

### Data Modeling & Design

**Purpose**: Create accurate, consistent, and well-documented data models that represent 
business concepts and support analytical and operational requirements.

**Core Practices**:

1. **Modeling Levels**
   - **Conceptual Model**: High-level business entities and relationships (diagrams)
   - **Logical Model**: Detailed entities, attributes, constraints (platform-independent)
   - **Physical Model**: Implementation-specific schemas with types, indexes, partitions
   - Progress from conceptual → logical → physical as design evolves

2. **Schema Design Principles**
   - **Normalization**: Reduce redundancy for transactional systems (OLTP)
   - **Denormalization**: Optimize for query performance in analytical systems (OLAP)
   - **Star Schema**: Fact tables + dimension tables for data warehouses
   - **Slowly Changing Dimensions (SCD)**: Track historical changes (Type 1, 2, 3)
   - Choose approach based on workload (transactional vs. analytical)

3. **Schema Documentation**
   - Document every table/entity: purpose, business owner, update frequency
   - Document every column: name, data type, constraints, business meaning, example values
   - Define relationships: primary keys, foreign keys, cardinality
   - Include sample data and edge cases in documentation

4. **Schema Versioning**
   - Version schemas semantically: v1.0, v1.1, v2.0
   - Document breaking changes vs. backward-compatible additions
   - Maintain migration scripts for schema evolution
   - Communicate schema changes to downstream consumers

5. **Data Contracts**
   - Define explicit contracts between producers and consumers
   - Contract includes: schema, quality SLAs, delivery frequency, contact info
   - Version contracts and publish to central repository
   - Validate data against contract at pipeline boundaries

**Learning Project Guidance**:
- Start with simple flat schemas before designing normalized models
- Draw entity-relationship diagrams even for toy projects (practice!)
- Document schemas in Markdown tables or JSON Schema format
- Use tools like `frictionless` or `Great Expectations` to validate schemas

**Quality Criteria**:
- [ ] Conceptual data model documented (entities and relationships)
- [ ] Logical schema documented (all tables/columns with descriptions)
- [ ] Physical schema matches logical design
- [ ] Schema versioning strategy defined
- [ ] Data contracts published for key datasets

---

### Data Storage & Operations

**Purpose**: Implement reliable, performant, and cost-effective data storage solutions with 
proper operational practices for backup, recovery, and monitoring.

**Core Practices**:

1. **Storage Technology Selection**
   - **Relational Databases**: PostgreSQL, MySQL for transactional workloads
   - **Columnar Databases**: DuckDB, ClickHouse for analytical queries
   - **NoSQL**: MongoDB (documents), Redis (key-value), Cassandra (wide-column)
   - **Data Lakes**: S3, ADLS, GCS for raw and processed data at scale
   - **File Formats**: Parquet (analytics), JSON (interchange), CSV (human-readable)
   - Choose based on access patterns, query requirements, and scale

2. **Database Design**
   - **OLTP (Transactional)**: Normalized schemas, row-based storage, ACID transactions
   - **OLAP (Analytical)**: Denormalized schemas, columnar storage, batch updates
   - Indexing strategy: primary keys, foreign keys, query-specific indexes
   - Partitioning strategy: range (date), hash (ID), list (category) partitioning
   - Compression: balance storage savings vs. query performance

3. **Backup & Recovery**
   - Automated backups with defined frequency (daily, weekly)
   - Test recovery procedures regularly (don't trust untested backups!)
   - Point-in-time recovery for critical datasets
   - Document RTO (Recovery Time Objective) and RPO (Recovery Point Objective)
   - Store backups in separate location (different region or system)

4. **Monitoring & Maintenance**
   - Monitor storage usage: disk space, growth trends, capacity planning
   - Monitor performance: query latency, throughput, connection counts
   - Automated maintenance: VACUUM, index rebuilding, statistics updates
   - Log slow queries for optimization opportunities
   - Set up alerts for storage thresholds and performance degradation

5. **Data Lifecycle Management**
   - Define retention policies: how long to keep raw data, processed data, logs
   - Implement archival for older data (cold storage, compressed formats)
   - Automate deletion of expired data per policy
   - Document data lineage for compliance audits

**Learning Project Guidance**:
- Start with local file storage (Parquet, JSON) before databases
- Use DuckDB for local analytics (in-process, no server required)
- Implement simple backup: copy files to another directory with timestamps
- Monitor with basic scripts: check file sizes, row counts, modification times

**Quality Criteria**:
- [ ] Storage technology chosen and justified for workload
- [ ] Backup strategy defined and tested
- [ ] Monitoring in place for storage usage and performance
- [ ] Retention policies documented and implemented
- [ ] Maintenance procedures documented (even if manual)

---

### Data Security

**Purpose**: Protect data assets from unauthorized access, breaches, and loss through 
defense-in-depth security practices aligned with DMBOK Data Security principles.

**Core Practices**:

1. **Encryption**
   - **At Rest**: Enable storage encryption for databases, files, object storage
   - **In Transit**: Use TLS 1.2+ for all network communication
   - **Key Management**: Rotate encryption keys regularly, use managed key services
   - **Column-Level**: Encrypt sensitive columns (SSN, credit cards) separately

2. **Access Control**
   - **RBAC (Role-Based Access Control)**: Define roles (reader, writer, admin), assign users to roles
   - **Principle of Least Privilege**: Grant minimum permissions necessary
   - **Service Accounts**: Dedicated credentials for applications, no shared passwords
   - **MFA (Multi-Factor Authentication)**: Require for production access
   - **Regular Reviews**: Audit permissions quarterly, revoke unused access

3. **Authentication & Authorization**
   - Strong password policies or SSO (Single Sign-On)
   - API authentication: tokens, API keys, OAuth 2.0
   - Database authentication: separate credentials per service
   - Audit failed authentication attempts, lock accounts after repeated failures

4. **Data Masking & Tokenization**
   - **Masking**: Replace PII with fake but realistic data in non-prod (john.doe@example.com → j***@***.com)
   - **Tokenization**: Replace sensitive data with tokens, maintain mapping securely
   - **Anonymization**: Remove identifiers for analytics datasets
   - Apply masking automatically in non-production environments

5. **Audit Logging**
   - Log all data access: who, what, when, from where
   - Log security events: failed logins, permission changes, data exports
   - Logs immutable: write-once storage or cryptographic signing
   - Retain audit logs per compliance requirements (1+ years)
   - Monitor logs for suspicious patterns

6. **Vulnerability Management**
   - Scan dependencies for known vulnerabilities (`pip-audit`, `safety`, Dependabot)
   - Apply security patches promptly
   - Code reviews include security checks (SQL injection, path traversal, XSS)
   - Penetration testing for production systems

**Learning Project Guidance**:
- Start with environment variables for secrets (`.env` files, `.gitignore` enforced)
- Use basic file permissions (`chmod 600` for config files)
- Implement simple access logging (write to log file: timestamp, user, action)
- Learn PII identification and masking patterns even with toy data

**Quality Criteria**:
- [ ] Secrets managed securely (not hardcoded, not in version control)
- [ ] Encryption enabled for data at rest and in transit
- [ ] Access control implemented (authentication required)
- [ ] PII identified and protected (masked in non-prod)
- [ ] Audit logging captures access and security events
- [ ] Dependencies scanned for vulnerabilities

---

### Data Integration & Interoperability

**Purpose**: Enable seamless data flow between systems using standard patterns for extraction, 
transformation, and loading while maintaining data quality and consistency.

**Core Practices**:

1. **Integration Patterns**
   - **ETL (Extract-Transform-Load)**: Transform data before loading (data warehouse pattern)
   - **ELT (Extract-Load-Transform)**: Load raw data, transform in target system (data lake pattern)
   - **Real-Time Streaming**: Kafka, Kinesis, Pub/Sub for event-driven architectures
   - **Batch Processing**: Scheduled loads (hourly, daily, weekly)
   - **Micro-Batching**: Small batches for near-real-time (e.g., every 5 minutes)
   - **CDC (Change Data Capture)**: Capture only changed records for efficiency

2. **Extraction Methods**
   - **Full Extract**: Pull entire dataset (simple but inefficient for large data)
   - **Incremental Extract**: Pull only new/changed records (filter by timestamp, sequence ID)
   - **API Integration**: RESTful APIs, GraphQL, webhooks
   - **Database Replication**: Logical replication, triggers, change streams
   - **File Transfer**: SFTP, S3 sync, object storage APIs

3. **Transformation Standards**
   - **Data Cleaning**: Handle nulls, trim whitespace, standardize formats
   - **Data Enrichment**: Join with reference data, lookup codes, calculate derived fields
   - **Data Aggregation**: Sum, count, average, group by dimensions
   - **Data Validation**: Check quality dimensions, apply business rules
   - **Idempotent Transformations**: Same input always produces same output

4. **Loading Methods**
   - **Append**: Add new records only (logs, events)
   - **Upsert**: Insert new records, update existing (merge/ON CONFLICT DO UPDATE)
   - **Overwrite**: Replace entire dataset (full refresh)
   - **SCD (Slowly Changing Dimensions)**: Track historical changes with effective dates

5. **Error Handling & Recovery**
   - **Retry Logic**: Exponential backoff for transient errors
   - **Dead Letter Queues**: Store unprocessable records for manual review
   - **Circuit Breakers**: Stop calling failing services to prevent cascading failures
   - **Idempotency Keys**: Prevent duplicate processing on retries
   - **Alerting**: Notify on repeated failures or data quality issues

6. **API Design (if building APIs)**
   - RESTful principles: resources, HTTP methods (GET, POST, PUT, DELETE)
   - Versioning: URL path (`/v1/customers`) or headers
   - Pagination: limit/offset or cursor-based for large result sets
   - Rate limiting: protect backend from overload
   - Documentation: OpenAPI (Swagger) specification

**Learning Project Guidance**:
- Start with file-based ETL (read CSV, transform, write Parquet)
- Use simple incremental loads (filter by max timestamp from previous run)
- Implement basic retry logic (try 3 times with 1-second delay)
- Build simple REST API with Flask/FastAPI for practice

**Quality Criteria**:
- [ ] Integration pattern chosen and documented (ETL/ELT/streaming)
- [ ] Extraction method efficient (incremental where possible)
- [ ] Transformations are idempotent and documented
- [ ] Error handling implemented (retry, dead letter queue)
- [ ] Loading method appropriate for data type (append/upsert/overwrite)

---

### Metadata Management

**Purpose**: Capture, organize, and make discoverable comprehensive metadata about data assets 
to enable understanding, trust, and effective data usage.

**Core Practices**:

1. **Metadata Types**
   - **Technical Metadata**: Schemas, data types, formats, sizes, row counts, indexes
   - **Business Metadata**: Definitions, owners, stewards, criticality, usage
   - **Operational Metadata**: Run times, success/failure, row counts processed, durations
   - **Lineage Metadata**: Sources, transformations, targets, dependencies
   - **Quality Metadata**: Quality scores, validation results, profiling statistics

2. **Data Catalogs**
   - Implement data catalog: centralized inventory of all datasets
   - Catalog entries include: name, description, owner, schema, location, update frequency
   - Enable search and discovery: tag datasets with keywords, categories
   - Link to documentation: data dictionaries, ERDs, usage examples
   - Tools: Apache Atlas, DataHub, Amundsen, or simple Markdown catalog

3. **Data Lineage**
   - Capture end-to-end lineage: source system → transformations → target system
   - Document transformation logic: what changes were applied and why
   - Track dataset versions and code versions together
   - Visualize lineage: DAG diagrams, flow charts
   - Enable impact analysis: "If I change this source, what breaks?"

4. **Metadata Automation**
   - Auto-extract technical metadata: scan schemas, infer data types
   - Auto-capture operational metadata: log all pipeline runs with metadata
   - Auto-generate data dictionaries from schemas and comments
   - Use tools: `great_expectations`, `frictionless`, custom scripts

5. **Metadata Standards**
   - Use standard vocabularies: Dublin Core, DCAT, Schema.org
   - Consistent naming conventions for metadata fields
   - Required vs. optional metadata fields
   - Metadata quality checks: completeness, accuracy

**Learning Project Guidance**:
- Start with simple README-based catalog (Markdown table of datasets)
- Document lineage in comments or diagram (even hand-drawn)
- Log operational metadata to JSON files (run ID, start/end times, row counts)
- Use docstrings to capture business logic alongside code

**Quality Criteria**:
- [ ] Data catalog exists (even if simple) listing all datasets
- [ ] Technical metadata captured (schemas, types, formats)
- [ ] Operational metadata logged for all pipeline runs
- [ ] Lineage documented (sources, transformations, targets)
- [ ] Metadata searchable/discoverable

---

### Data Quality

**Purpose**: Ensure data fitness for intended use through systematic measurement, monitoring, 
and improvement of data quality across all quality dimensions.

**Core Practices**:

1. **Quality Dimensions (6Cs)**
   - **Accuracy**: Data correctly represents real-world values
     - Validation: cross-reference with authoritative sources
     - Example: address validates against postal service API
   
   - **Completeness**: All required data present, no unexpected nulls
     - Validation: check NOT NULL constraints, required field presence
     - Example: all customers have email addresses
   
   - **Consistency**: Same data across systems matches
     - Validation: cross-system reconciliation, referential integrity
     - Example: customer name same in CRM and billing system
   
   - **Timeliness**: Data available when needed, freshness tracked
     - Validation: check data age against SLAs
     - Example: yesterday's sales data available by 8am today
   
   - **Validity**: Data conforms to formats, ranges, business rules
     - Validation: regex patterns, range checks, enum validation
     - Example: email matches pattern, age between 0-120
   
   - **Uniqueness**: No unintended duplicates
     - Validation: primary key uniqueness, duplicate detection
     - Example: no duplicate customer records with same email

2. **Validation Rules**
   - **Schema Validation**: Check data types, required fields, structure
   - **Range Validation**: Numeric bounds, date ranges
   - **Format Validation**: Regex patterns (email, phone, URLs)
   - **Referential Integrity**: Foreign keys exist in reference tables
   - **Business Rules**: Custom logic (e.g., "discount ≤ price")
   - Implement at pipeline ingestion and transformation stages

3. **Quality SLAs**
   - Define measurable targets per quality dimension
   - Example: "99% of records pass all validation rules"
   - Example: "Data latency < 1 hour from source system"
   - Example: "0 critical data quality issues in production"
   - Track SLA compliance over time, report trends

4. **Data Profiling**
   - Analyze data distributions before pipeline design
   - Column statistics: min, max, avg, median, std dev, null percentage
   - Value frequency distributions: top values, rare values
   - Outlier detection: values far from normal range
   - Pattern detection: common formats, anomalies
   - Tools: `pandas-profiling`, `ydata-profiling`, DuckDB, Great Expectations

5. **Quality Monitoring**
   - Continuous quality checks in production pipelines
   - Quality dashboards: visualize pass rates, trends, anomalies
   - Automated quality tests in CI/CD
   - Alerts on quality threshold violations
   - Quality scorecards for executive visibility

6. **Root Cause Analysis**
   - When quality issues occur, investigate source
   - Log which validation rule failed with context
   - Trace lineage back to source system
   - Document issue, root cause, and remediation
   - Implement preventive measures for future

**Learning Project Guidance**:
- Start with basic schema validation (required fields, data types)
- Add business rule validation incrementally
- Create intentional quality issues in toy data to test validation
- Profile data before pipeline design (understand distributions)
- Log validation failures to separate file for review

**Quality Criteria**:
- [ ] Quality dimensions defined for each dataset (6Cs applicable)
- [ ] Validation rules documented and implemented
- [ ] Quality SLAs defined and tracked
- [ ] Data profiling performed before pipeline design
- [ ] Validation failures logged with actionable error messages
- [ ] Quality metrics captured and reportable

---

## Code Quality & Maintainability Standards

Code MUST prioritize readability, understandability, maintainability, and reusability. These 
standards apply to all pipeline code, transformation logic, and supporting utilities.

---

### Readability Standards

**Python Style (PEP 8 Compliance MANDATORY)**:
- **Line Length**: Maximum 88 characters (Black formatter default) or 100 (acceptable)
- **Indentation**: 4 spaces per level (no tabs)
- **Imports**: Organized in groups: standard library, third-party, local imports
- **Blank Lines**: 2 blank lines between top-level functions/classes, 1 within functions
- **Naming Conventions**:
  - `snake_case` for functions, variables, modules
  - `PascalCase` for classes
  - `UPPER_SNAKE_CASE` for constants
  - Descriptive names: `calculate_customer_ltv()` not `calc()`

**Function Design**:
- **Maximum Length**: 50 lines per function (encourage decomposition)
- **Single Responsibility**: Each function does one thing well
- **Pure Functions Preferred**: No side effects where possible (easier to test)
- **Type Hints**: Use Python type hints for function signatures

```python
def transform_customer_data(
    raw_data: pd.DataFrame,
    reference_date: datetime
) -> pd.DataFrame:
    """Transform raw customer data with business logic."""
    # Implementation
```

**Comments**:
- **When to Comment**:
  - Complex business logic that isn't obvious from code
  - "Why" not "what": explain reasoning, not mechanics
  - Edge cases and assumptions
  - TODOs and FIXMEs with issue tracker references
- **When NOT to Comment**:
  - Obvious code (`x = x + 1  # increment x`)
  - Outdated comments (worse than no comments!)

**Code Structure**:
- **Logical Grouping**: Related functions together
- **Top-Down Reading**: High-level functions at top, helpers below
- **Avoid Deep Nesting**: Maximum 3 levels of indentation

---

### Documentation Standards

**Module-Level Docstrings**:

```python
"""
Customer Lifetime Value (LTV) Calculation Module

This module provides functions for calculating and predicting customer LTV based on
historical transaction data using RFM analysis and cohort modeling.

Author: Data Engineering Team
Created: 2026-02-15
Dependencies: pandas, numpy, scikit-learn

Key Functions:
- calculate_ltv(): Compute LTV for existing customers
- predict_ltv(): Forecast LTV for new customers
- generate_ltv_report(): Create executive summary

Usage:
    from src.ltv import calculate_ltv
    ltv_scores = calculate_ltv(transactions_df)
"""
```

**Function Docstrings (Google Style - MANDATORY for public functions)**:

```python
def calculate_customer_lifetime_value(
    customer_id: int,
    transaction_history: pd.DataFrame,
    discount_rate: float = 0.1,
    forecast_periods: int = 12
) -> float:
    """
    Calculate projected lifetime value of a customer using DCF analysis.
    
    Uses discounted cash flow (DCF) methodology to project future value based on
    historical purchase patterns. Applies cohort-based retention curves and
    average order value trends.
    
    Args:
        customer_id: Unique customer identifier (integer primary key)
        transaction_history: DataFrame with required columns:
            - date (datetime): Transaction date
            - amount (float): Transaction amount in USD
            - product_id (int): Product identifier
            Must be sorted by date ascending, no nulls in required columns
        discount_rate: Annual discount rate for future cash flows (default 0.1 = 10%)
            Range: 0.0 to 1.0
        forecast_periods: Number of months to forecast (default 12)
            Range: 1 to 60
    
    Returns:
        Projected lifetime value in USD (float, 2 decimal places)
        Returns 0.0 if customer has no transaction history
    
    Raises:
        ValueError: If transaction_history is empty or missing required columns
        ValueError: If discount_rate not in valid range [0.0, 1.0]
        TypeError: If customer_id is not an integer
    
    Examples:
        >>> transactions = pd.DataFrame({
        ...     'date': pd.to_datetime(['2024-01-01', '2024-02-01']),
        ...     'amount': [100.0, 150.0],
        ...     'product_id': [1, 2]
        ... })
        >>> calculate_customer_lifetime_value(123, transactions)
        1250.50
        
        >>> # Customer with no history returns 0
        >>> empty_df = pd.DataFrame(columns=['date', 'amount', 'product_id'])
        >>> calculate_customer_lifetime_value(456, empty_df)
        0.0
    
    Notes:
        - Assumes retention curve follows cohort historical patterns
        - Does not account for seasonality (future enhancement)
        - Uses monthly cohorts for retention analysis
    """
    # Implementation here
```

**README.md for Every Component**:

Every component/pipeline MUST have a README.md with:
- Overview and purpose
- Installation instructions
- Usage examples
- Input/output schema documentation
- Dependencies
- Testing instructions
- Known limitations
- Author/contact information

---

### Maintainability Standards

**DRY Principle (Don't Repeat Yourself)**:
- Extract repeated logic into functions: `src/lib/utils.py`
- Reusable transformations: `src/lib/transforms.py`
- Common validators: `src/lib/validators.py`

**Configuration Externalization**:
- Use configuration files: `config.yaml`, `config.json`
- Environment-specific configs: `config_dev.yaml`, `config_prod.yaml`
- Environment variables for secrets and deployment-specific values
- Load config at startup, pass to functions as parameters

**Error Handling**:
- Use specific exception types, not generic `Exception`
- Provide meaningful error messages with context
- Include recovery guidance in error messages
- Log errors before raising

**Version Control Practices**:
- **Commit Messages**: Semantic format
  - `feat: add customer LTV calculation`
  - `fix: handle null values in transaction amount`
  - `refactor: extract validation logic to separate module`
  - `docs: update README with API examples`
  - `test: add edge cases for LTV calculator`
- **Branch Strategy**: Feature branches, PR workflow
- **PR Template**: Include checklist (tests pass, docs updated, DMBOK compliance)

---

### Reusability Standards

**Component Library Structure**:

```
src/
├── lib/                      # Shared reusable components
│   ├── __init__.py
│   ├── validators.py         # Common validation functions
│   ├── transforms.py         # Reusable transformations
│   ├── io_utils.py          # File I/O utilities
│   ├── logging_config.py    # Logging setup
│   └── quality_checks.py    # Data quality validators
├── pipelines/               # Specific pipeline implementations
│   ├── customer_ltv/
│   ├── sales_aggregation/
│   └── inventory_forecast/
└── models/                  # Data models/schemas
    ├── customer.py
    └── transaction.py
```

**Reusable Function Design**:
- Parameterized for different contexts
- Framework-agnostic where possible
- Well-documented with examples
- Testable in isolation

---

### Testing Standards

**Test Coverage Requirements**:
- **Minimum 80% coverage** for core pipeline logic
- 100% coverage for critical transformations and validators
- Use `pytest-cov` to measure coverage

**Test Organization**:

```
tests/
├── unit/                    # Unit tests (fast, isolated)
│   ├── test_transforms.py
│   ├── test_validators.py
│   └── test_quality_checks.py
├── integration/             # Integration tests (multi-component)
│   ├── test_ltv_pipeline.py
│   └── test_data_flow.py
├── contract/                # Contract tests (API, schemas)
│   └── test_api_endpoints.py
├── fixtures/                # Shared test data
│   ├── sample_transactions.csv
│   └── expected_outputs.parquet
└── conftest.py             # Pytest configuration and fixtures
```

**Test Naming Convention**:

```python
def test_<function_name>_<scenario>_<expected_outcome>():
    """Test that function behaves correctly under scenario."""
    
# Examples:
def test_calculate_ltv_with_valid_data_returns_positive_value():
def test_calculate_ltv_with_empty_dataframe_returns_zero():
def test_validate_schema_with_missing_column_raises_error():
```

**Test Isolation**:
- Tests MUST NOT share state
- Use fixtures for setup, not global variables
- Clean up resources in teardown or use `tmp_path` fixture

---

### Code Review Checklist

Before approving PR, verify:

- [ ] **TDD Compliance**: Tests written before implementation
- [ ] **PEP 8**: Code formatted with Black or Ruff
- [ ] **Type Hints**: Function signatures include type annotations
- [ ] **Docstrings**: All public functions documented (Google style)
- [ ] **README**: Updated with new features, usage examples
- [ ] **Tests**: Pass with >= 80% coverage, new tests added
- [ ] **Configuration**: No hardcoded values (use config files)
- [ ] **Error Handling**: Specific exceptions with meaningful messages
- [ ] **DMBOK Compliance**: Data quality, security, metadata standards met
- [ ] **No Secrets**: No API keys, passwords in code or version control
- [ ] **Comments**: Explain "why", not "what"
- [ ] **DRY**: No code duplication, reusable components extracted

---

## Technical Standards & Constraints

This section defines technology choices, pipeline patterns, and technical constraints aligned 
with DMBOK principles for learning-focused data engineering projects.

---

### Storage Formats & Standards

**File Formats (Prioritized)**:

1. **Parquet** (PRIMARY for analytics)
   - Columnar storage optimized for analytical queries
   - Excellent compression (typically 10x smaller than CSV)
   - Schema embedded in file, supports complex types
   - Use for: processed data, analytical datasets, data lake storage

2. **JSON/JSONL** (for semi-structured data)
   - Human-readable, self-describing
   - JSON Lines (`.jsonl`) for streaming and line-by-line processing
   - Use for: API responses, logs, configuration, nested data

3. **CSV** (for interchange and human review)
   - Universal compatibility, simple structure
   - Use for: small datasets, manual inspection, Excel compatibility
   - MUST include header row with column names

4. **Avro** (advanced: schema evolution)
   - Binary format with embedded schema
   - Excellent for streaming (Kafka standard)
   - Use for: event streams, when schema evolution critical

**Format Selection Guide**:
- **Analytics workload**: Parquet
- **Logs/events**: JSONL
- **Configuration**: YAML (human-editable) or JSON (machine-readable)
- **Data exchange**: CSV (simple) or Parquet (large volumes)
- **Real-time streaming**: Avro (Kafka) or JSON (simple)

**Naming Conventions**:
- Include date partition: `data_YYYY_MM_DD.parquet`
- Include version if applicable: `schema_v2_customers.parquet`
- Use lowercase with underscores: `customer_transactions.parquet`

---

### Pipeline Patterns & Architecture

**Pattern Selection Matrix**:

| Pattern | Latency | Complexity | Use When |
|---------|---------|------------|----------|
| **Batch (Full Load)** | Hours-Days | Low | Small data, infrequent updates, learning basics |
| **Batch (Incremental)** | Hours | Medium | Large data, regular updates, timestamp tracking |
| **Micro-Batch** | Minutes | Medium | Near real-time, acceptable small delays |
| **Streaming** | Seconds | High | Real-time requirements, event-driven |
| **CDC** | Minutes | High | Database replication, incremental sync |

**Default for Learning**: Start with Batch Processing (full or incremental) before attempting streaming.

---

### Technology Stack

**Python Environment (MANDATORY)**:
- **Version**: Python 3.11+ (current stable, f-strings, type hints, performance improvements)
- **Package Manager**: `pip` with `pyproject.toml` or `requirements.txt`
- **Virtual Environment**: `venv` or `conda` (isolate dependencies)

**Data Processing Libraries (Prioritized)**:

1. **Pandas** (DEFAULT for small-medium data)
   - Use for: < 1M rows, exploratory analysis, prototyping
   - Strengths: Easy API, rich ecosystem, widespread knowledge

2. **Polars** (PREFERRED for performance)
   - Use for: 1M-100M rows, performance-critical transformations
   - Strengths: Fast (Rust-based), lazy evaluation, parallel processing

3. **DuckDB** (PREFERRED for SQL analytics)
   - Use for: Analytical queries, aggregations, local data warehouse
   - Strengths: Fast columnar database, SQL interface, no server

**Data Quality & Testing**:
- **Great Expectations**: Schema validation, data quality checks (RECOMMENDED)
- **Frictionless**: Data validation framework, simple API
- **Pytest**: Testing framework (MANDATORY)

**Version Control & CI/CD**:
- **Git**: MANDATORY for all code
- **GitHub Actions / GitLab CI**: Automated testing, linting (RECOMMENDED)
- **Pre-commit hooks**: Run checks before commit (Black, Ruff, tests)

---

### Development Environment Setup

**Project Structure Template**:

```
data-pipeline-project/
├── .git/                       # Git repository
├── .gitignore                  # Ignore data files, secrets, __pycache__
├── README.md                   # Project overview
├── pyproject.toml              # Package configuration
├── requirements.txt            # Dependencies (pinned versions)
│
├── config/                     # Configuration files
│   ├── config_dev.yaml
│   └── config_prod.yaml
│
├── data/                       # Data directory (gitignored)
│   ├── raw/                    # Source data (immutable)
│   ├── processed/              # Transformed data
│   ├── output/                 # Final outputs
│   └── logs/                   # Pipeline logs
│
├── src/                        # Source code
│   ├── __init__.py
│   ├── lib/                    # Reusable components
│   │   ├── validators.py
│   │   ├── transforms.py
│   │   └── io_utils.py
│   ├── pipelines/              # Specific pipelines
│   │   ├── customer_ltv/
│   │   └── sales_aggregation/
│   └── models/                 # Data models/schemas
│       └── schemas.py
│
├── tests/                      # Test suite
│   ├── unit/
│   ├── integration/
│   ├── contract/
│   └── conftest.py
│
├── notebooks/                  # Jupyter notebooks (exploration)
│   └── exploratory_analysis.ipynb
│
├── docs/                       # Documentation
│   ├── architecture.md
│   └── pipeline_guide.md
│
└── scripts/                    # Utility scripts
    ├── run_pipeline.sh
    └── setup_db.sql
```

---

### Performance Guidelines

**Optimization Philosophy**:
1. **Make it work** (correctness first)
2. **Make it right** (clean code, tests)
3. **Make it fast** (only if needed, measure first)

**When to Optimize**:
- Baseline performance unacceptable (hours when minutes expected)
- Bottleneck identified via profiling (not guessing!)
- Clear performance goal defined (e.g., "< 5 min processing time")

**Common Optimizations**:
1. Use Columnar Formats: Parquet instead of CSV (10x faster reads)
2. Filter Early: Apply WHERE clauses before loading full dataset
3. Use Appropriate Data Types: `int32` instead of `int64` if range fits
4. Vectorized Operations: Pandas/Polars vectorization over loops
5. Lazy Evaluation: Polars `.scan_parquet()` vs. `.read_parquet()`
6. Partitioning: Partition large datasets by date, region, etc.

**Anti-Patterns to Avoid**:
- ❌ Loop over DataFrame rows (use vectorized operations)
- ❌ Load entire dataset when subset needed (filter early)
- ❌ Nested loops in transformation logic (use joins)
- ❌ Reading CSV when Parquet available (10x speed difference)

---

## Development Workflow

This section defines processes for code development, review, testing, and deployment aligned 
with DMBOK operational excellence and CI/CD best practices.

---

### Code Review Requirements

**Pre-Review Checklist (Author)**:

Before submitting PR, author MUST verify:

- [ ] **TDD Compliance**: Tests written before implementation, all tests pass
- [ ] **Code Quality**: Black/Ruff formatting applied, no linting errors
- [ ] **Documentation**: Docstrings updated, README reflects changes
- [ ] **DMBOK Alignment**: Check against 7 principles and 8 knowledge areas
- [ ] **No Secrets**: API keys, passwords removed, use environment variables
- [ ] **Test Coverage**: >= 80% for new code (run `pytest --cov`)
- [ ] **Manual Testing**: Pipeline runs end-to-end successfully

**Review Checklist (Reviewer)**:

Reviewers MUST verify:

1. **Functional Correctness**: Code solves stated problem, edge cases handled
2. **Code Quality**: PEP 8 compliance, function length < 50 lines, DRY principle
3. **Documentation**: Docstrings present, README updated
4. **Testing**: Tests cover happy path and edge cases, coverage >= 80%
5. **DMBOK Compliance**: Data quality, security, metadata requirements met
6. **Security**: No secrets in code, input validation present

---

### Quality Gates

Before merging to main branch, ALL gates MUST pass:

**Gate 1: Automated Checks (CI/CD)**
- [ ] All tests pass
- [ ] Code coverage >= 80%
- [ ] No linting errors
- [ ] No critical security vulnerabilities

**Gate 2: Functional Validation**
- [ ] Pipeline runs end-to-end successfully
- [ ] Output data meets quality SLAs

**Gate 3: Documentation**
- [ ] README updated
- [ ] Schema changes documented

**Gate 4: DMBOK Compliance**
- [ ] Data quality checks implemented
- [ ] Metadata captured
- [ ] Security requirements met

---

### CI/CD Pipeline

**Continuous Integration (on every push)**:

```yaml
# .github/workflows/ci.yml
name: CI Pipeline

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      
      - name: Install dependencies
        run: |
          pip install -r requirements.txt
          pip install pytest pytest-cov black ruff pip-audit
      
      - name: Lint with Ruff
        run: ruff check src/
      
      - name: Format check with Black
        run: black --check src/
      
      - name: Run tests with coverage
        run: pytest --cov=src --cov-report=xml --cov-report=term
      
      - name: Check test coverage >= 80%
        run: coverage report --fail-under=80
      
      - name: Security scan
        run: pip-audit
```

---

### Monitoring & Alerting

**Pipeline Health Monitoring**:

Track key metrics:
- **Execution Metrics**: Run duration, success/failure rate, throughput
- **Data Quality Metrics**: Validation pass rate, record counts
- **Resource Metrics**: CPU, memory, disk I/O
- **Business Metrics**: Data freshness, SLA compliance

**Logging Standards**:

```python
import logging
import json
from datetime import datetime

logger = logging.getLogger(__name__)

def log_pipeline_run(
    pipeline_name: str,
    status: str,
    duration_seconds: float,
    records_processed: int,
    **kwargs
):
    """Log pipeline run with structured metadata."""
    log_entry = {
        "timestamp": datetime.utcnow().isoformat(),
        "pipeline": pipeline_name,
        "status": status,
        "duration_seconds": duration_seconds,
        "records_processed": records_processed,
        **kwargs
    }
    
    if status == 'failed':
        logger.error(json.dumps(log_entry))
    else:
        logger.info(json.dumps(log_entry))
```

**Alerting Strategy**:

| Alert Level | Condition | Action |
|-------------|-----------|--------|
| **INFO** | Normal operation | Log only |
| **WARN** | Degraded but operational | Log, investigate later |
| **ERROR** | Pipeline failed | Alert on-call |
| **CRITICAL** | Service outage | Page on-call immediately |

---

### Learning Progression

**Phase 1: Fundamentals (Weeks 1-2)**
- Single-file Python scripts
- Read CSV, transform, write Parquet
- Basic validation
- Local execution

**Phase 2: Structured Pipelines (Weeks 3-4)**
- Multi-file project structure
- Reusable functions
- Unit tests with pytest
- Git version control

**Phase 3: Production Patterns (Weeks 5-6)**
- TDD workflow
- Data quality framework
- Integration tests
- CI/CD with GitHub Actions

**Phase 4: Advanced Concepts (Weeks 7-8)**
- Incremental pipelines
- Performance optimization
- Orchestration
- DMBOK compliance review

---

## Governance

---

### Amendment Procedure

**When to Amend Constitution**:
- Major technology shifts
- New DMBOK releases
- Project scope changes
- Principle conflicts discovered

**Version Increment Rules** (Semantic Versioning):
- **MAJOR (X.0.0)**: Principle removal, backward-incompatible redefinition
- **MINOR (x.Y.0)**: New principle added, materially expanded guidance
- **PATCH (x.y.Z)**: Clarifications, wording improvements, typo fixes

**Amendment Process**:
1. Document proposed change with rationale
2. Stakeholders review and discuss
3. Approval (consensus or voting)
4. Update constitution and templates
5. Communicate changes

---

### Compliance Verification

**PR-Level Compliance**:

Every PR MUST include DMBOK compliance checklist:

```markdown
## DMBOK Compliance Checklist

### Core Principles
- [ ] I. Modularity & Reusability
- [ ] II. Data Quality & Governance
- [ ] III. Test-First Development
- [ ] IV. Metadata & Lineage
- [ ] V. Data Security & Privacy
- [ ] VI. Data Architecture & Integration
- [ ] VII. Simplicity & Performance

### Knowledge Areas (if applicable)
- [ ] Data Governance
- [ ] Data Architecture
- [ ] Data Modeling
- [ ] Data Storage
- [ ] Data Security
- [ ] Data Integration
- [ ] Metadata Management
- [ ] Data Quality

### Code Quality
- [ ] PEP 8 compliance
- [ ] Docstrings present
- [ ] Test coverage >= 80%
- [ ] No secrets in code
```

**Quarterly Constitution Review**:

Every quarter, conduct constitution alignment review:
1. Audit sample pipelines
2. Review exceptions
3. Assess effectiveness
4. Identify gaps
5. Propose amendments

**Complexity Justification**:

When introducing complexity that violates "Simplicity & Performance" principle, document:

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| Example: Kafka streaming | Real-time requirement | Batch latency exceeds SLA |

---

### Runtime Guidance

**Constitution as Source of Truth**:
- Use `.specify/memory/constitution.md` (this file) for all governance decisions
- When templates conflict with constitution, constitution takes precedence

**Principle Conflicts**:

If principles conflict:
1. Identify conflict
2. Consider context
3. Seek guidance
4. Document decision

**Principle Priority** (when forced to choose):
1. Test-First Development (NON-NEGOTIABLE)
2. Data Security & Privacy
3. Data Quality & Governance
4. Metadata & Lineage
5. Modularity & Reusability
6. Data Architecture & Integration
7. Simplicity & Performance

---

**Version**: 2.0.0 | **Ratified**: 2026-02-15 | **Last Amended**: 2026-02-15
