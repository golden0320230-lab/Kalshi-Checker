# Threat Model And Misuse Boundaries

## Intentional Scope

This project is intentionally limited to:

- local-only analysis
- public Polymarket data
- explainable scoring and watchlist monitoring
- local reports and exports

It is intentionally out of scope for:

- order placement
- trade automation
- private API integration
- wallet signing
- account linkage
- deanonymization
- identity correlation beyond public wallet-level behavior

## Assets

Primary assets in scope:

- local SQLite data
- local reports and exports
- local scoring snapshots and watch alerts
- reproducible analysis logic

Assets intentionally not handled:

- custody keys
- account credentials
- broker or exchange sessions
- transaction signing material

## Trust Boundaries

### External Public Data

The tool consumes public Polymarket API responses. Those responses may drift in shape, fail temporarily, or contain incomplete metadata.

Mitigation:

- DTO validation
- typed client exceptions
- graceful ingestion failure handling
- idempotent persistence

### Local Persistence

All analysis artifacts are stored locally in SQLite. This keeps the blast radius on one machine but also means the local filesystem is the main confidentiality boundary.

Mitigation:

- no remote DB dependency
- explicit local file paths
- no background data exfiltration path in the app

### User Interpretation

The system surfaces rankings and alerts that may influence discretionary research. It should not be described as certainty, alpha guarantees, or trading automation.

Mitigation:

- explainable features
- top reasons and sample-size context in reports
- explicit local-only/no-execution scope in docs

## Misuse Cases

### Misuse: turning the tool into a bet execution bot

Not supported.

Current codebase has:

- no order placement module
- no private auth flow
- no signing path
- no exchange account connection

### Misuse: linking wallets to real-world identities

Not supported.

Current codebase intentionally restricts itself to public wallet-level behavior. There is no identity enrichment or deanonymization workflow.

### Misuse: using reports as guaranteed signals

Not supported.

The scoring system is heuristic and historical. It should be treated as a research aid, not as an execution oracle.

### Misuse: sharing local exports as if they were authoritative surveillance outputs

Out of scope.

Reports are local analyst artifacts built from public data and heuristic scoring, not compliance or enforcement records.

## Operational Risks

### API shape drift

Public endpoints may change or become partially inconsistent.

### Sparse or biased data

Some wallets have too little history for strong conclusions.

### False positives

High rank does not guarantee durable predictive value.

### False negatives

Interesting wallets can remain under threshold or outside the seeded universe.

## Guardrails In The Current Design

- local SQLite only
- explicit public-data-only docs
- no trade execution path
- no wallet-signing path
- no private credential handling
- deterministic thresholds and explainability payloads
- tests around ingestion, scoring, flagging, watch mode, and reporting

## Reviewer Checklist

When adding new features, reject changes that introduce:

- secret or private account dependencies
- order placement capabilities
- transaction signing
- identity resolution or deanonymization
- remote storage by default
- non-explainable ranking logic without persisted reasoning
