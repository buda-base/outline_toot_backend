# Elasticsearch Curation + Import Workflow (Works / Persons)

This system keeps an **imported snapshot** from an original DB, while allowing **local curation** (edits, duplicates/merges) in Elasticsearch (ES). It tracks all edits in a separate index for audit and reproducibility.
Goal: **fast incremental imports** and **clear “was modified / when / by whom”**.

---

## Document shape (core fields)

Each `work` / `person` document contains:

```json
{
  "_id": "WA12345",
  "type": "work/person",
  "prefLabel_bo": "…",
  "altLabel_bo": "…",

  "origin": "imported", # for records originally imported from BDRC, "local" for records created in the tool

  "source": { # information from the BDRC db
    "updated_at": "2026-02-01T10:15:00Z"
  },

  "curation": { # information on modifications from the tool
    "modified": false,
    "modified_at": null, # timestamp
    "modified_by": null, # user name
    "edit_version": 0 # iterative: 1, 2, 3, etc.
  },

  "record_status": "active", # or "duplicate" or "withdrawn"
  "canonical_id": null, # if duplicate, the id of the canonical document

  "import": { # import bookkeeping
    "last_run_at": "2026-02-12T09:00:00Z",
    "last_result": "updated"
  }
}
```

---

## Action: Importing from BDRC (incremental)

### Step 1 — read changed records from BDRC (watermark)

Keep a checkpoint per type, e.g. `last_source_updated_at`.
Don't import records where `updated_at > checkpoint`.

```json
{
  "_id": "work_import_record",
  "last_update_at": "..."
}
```

### Step 2 — upsert into ES with conditional overwrite

Use ES **Bulk Update** with **scripted_upsert** so ES decides overwrite rules **without you reading each doc first**.

**Bulk request (NDJSON)**

```json
{ "update": { "_index": "bec", "_id": "WA12345" } }
{
  "scripted_upsert": true,
  "upsert": {
    "type": "work",
    "origin": "imported",
    "source": {
      "updated_at": "2026-02-01T10:15:00Z"
    },
    "curation": { "modified": false, "modified_at": null, "modified_by": null, "edit_version": 0 },
    "status": "active",
    "canonical_id": null,
    "prefLabel_bo": "Imported title",
    "altLabel_bo": ["Imported title"]
  },
  "script": {
    "lang": "painless",
    "source": """
      if (ctx._source.source == null) { ctx._source.source = [:]; }
      ctx._source.source.updated_at = params.doc.source.updated_at;

      if (ctx._source.import == null) { ctx._source.import = [:]; }
      ctx._source.import.last_run_at = params.now;

      // Only overwrite source-owned fields if NOT curated
      boolean modified = (ctx._source.curation != null && ctx._source.curation.modified == true);

      if (!modified) {
        // Example source-owned fields:
        ctx._source.prefLabel_bo = params.doc.prefLabel_bo;
        ctx._source.altLabel_bo = params.doc.altLabel_bo;

        // record result
        ctx._source.import.last_result = 'updated_or_created';
      } else {
        ctx._source.import.last_result = 'skipped_modified';
      }

      // Ensure curation object exists
      if (ctx._source.curation == null) {
        ctx._source.curation = params.doc.curation;
      }
    """,
    "params": {
      "now": "2026-02-12T09:00:00Z",
      "doc": {
        "type": "work",
        "origin": "imported",
        "source": {
          "updated_at": "2026-02-01T10:15:00Z"
        },
        "curation": { "modified": false, "modified_at": null, "modified_by": null, "edit_version": 0 },
        "prefLabel_bo": "Imported title",
        "altLabel_bo": "Imported title"
      }
    }
  }
}
```

**Importer rule**

* If `curation.modified == false`: import updates “source-owned” fields
* If `curation.modified == true`: import updates only `source.*` + `import.*`, and **does not clobber curated content**

---

## Action: Updating a document that was imported (curation edit)

When a curator edits a document:

1. update the business fields (ex: `prefLabel_bo`, `altLabel_bo`)
2. mark it as curated (`curation.modified = true`)
3. set `modified_at/by`, increment `edit_version`

**Update request example**

```json
POST /bec/_update/work:WA12345
{
  "doc": {
    "prefLabel_bo": "Curated (fixed) title",
    "curation": {
      "modified": true,
      "modified_at": "2026-02-12T11:30:00Z",
      "modified_by": "xxx",
      "edit_version": 1
    }
  }
}
```

**Result**

* Future imports for `WA12345` will *not* overwrite the curated title.

---

## Action: Creating a brand new local-only record

Local-only records have no `source.id`.

```json
POST /bec/_doc/WA1BC987
{
  "type": "work",
  "origin": "local",

  "prefLabel_bo": "A brand new work (created in the tool)",

  "curation": {
    "modified": true,
    "modified_at": "2026-02-12T12:00:00Z",
    "modified_by": "xxx",
    "edit_version": 1
  },

  "status": "active",
  "canonical_id": null
}
```

---

## Action: Merging two documents (mark duplicate)

Instead of deleting, mark the loser as a duplicate that points to the winner.

Example: merge `WA777` into `WA12345`.

### Update the duplicate (loser)

```json
POST /entities/_update/work:WA777
{
  "doc": {
    "status": "duplicate",
    "canonical_id": "WA12345",
    "curation": {
      "modified": true,
      "modified_at": "2026-02-12T14:10:00Z",
      "modified_by": "xxx",
      "edit_version": 3
    }
  }
}
```

**Query behavior**

* Search results should normally filter `status:"active"`
* When you fetch a doc and see `status:"duplicate"`, follow `canonical_id`

---

## Action: Tracking “what changed and when” (audit index)

Keep a separate append-only ES index (`bec_changes`) to store events.

Example event for a curator edit:

```json
POST /entity_changes/_doc
{
  "timestamp": "2026-02-12T11:30:00Z",
  "actor": "xxx",
  "entity_type": "work",
  "entity_id": "WA12345",
  "action": "edit",
  "diff": {
    "title": { "from": "Imported title", "to": "Curated (fixed) title" }
  },
  "correlation_id": "req-8b0f6c" # an internal id pointing to an operation
}
```

Example event for an import update:

```json
POST /entity_changes/_doc
{
  "timestamp": "2026-02-12T09:00:00Z",
  "actor": "importer",
  "entity_type": "work",
  "entity_id": "WA12345",
  "action": "import_update",
  "diff": {
    "source.updated_at": { "from": "2026-01-01T00:00:00Z", "to": "2026-02-01T10:15:00Z" }
  },
  "correlation_id": "import-2026-02-12"
}
```

---

## Quick rules of thumb

* **Never delete duplicates** → use `status:"duplicate"` + `canonical_id`
* **Imports must never overwrite curated docs** → check `curation.modified`
* **Incremental import** → query source DB by `updated_at` watermark + Bulk update in ES
* **Audit trail** → store events in a separate index, not in the main docs
