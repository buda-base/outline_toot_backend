This doc describes the mechanism through which the works, persons and outlines / segments are synced from the BDRC database to the OpenSearch DB of the outline tool.

The general principle is to have an offline script that:
1. clones / pulls from the works, outlines and persons repos from https://gitlab.com/bdrc-data (all suffixed with -20220922)
2. checks the previous sync information in the opensearch db and list all the records to import from git
3. import the relevant records

**1** is straightforward. We consider that the git repos are linear and have one branch only.

## list records to import

For **2**, each sync to the works writes the opensearch document with id = `work_import_record`:

```json
{
  "last_updated_at": "...",
  "last_revision_imported": "..."
}
```

Where `last_updated_at` is the timestamp at the end of a sync, and `last_revision_imported` is the most recent commit id of the git repo when syncing.

If the document is not present or `force` mode is on or `last_revision_imported` can't be found in the commit history, then all records are imported.

Else the script will only list the records that were changed between the commit in `last_revision_imported` and the latest commit.

## Importing records

When importing a record, the record import function will be passed an argument that is a path to a `.trig` file, relative to the root of a git repo. The path can be assumed to be in the form `{hash}/{id}.trig`.

A work or person record is relatively straightforward to import, with the following conventions:
- if a record does not have `bda:{id} adm:status bda:StatusReleased`
   * if it's not in the db yet, ignore it and don't import it
   * else:
      * if it has a replacement, `bda:{id} adm:replaceWith bdr:{newid}`, merge `{id}` into `{newid}` in the db (and mark it as record_status=duplicate)
      * else just mark it as record_status=withdrawn
- else:
   * import object of `bdr:{id} skos:prefLabel` with `lang=bo-x-ewts` as field `prefLabel_bo` after conversion from EWTS to Unicode with pyewts, and object of `bdr:{id} skos:prefLabel` with `lang=bo` directly
   * import objects of `bdr:{id} skos:altLabel` in the same way

To find the author of a work, look at all objects of `bdr:{wa_id} :creator ...`, these are objects with two properties: `:agent` (the object of which is the BDRC person), and `:role` (the object of which is a [role](https://github.com/buda-base/owl-schema/blob/master/roles/creators.ttl)). Authors are persons with the following roles: `bdr:R0ER0011`, `bdr:R0ER0014` (if that role is present, ignore all other roles for authors), `bdr:R0ER0019`, `bdr:R0ER0025`. Import the BDRC person id (`P...`) in the `author` field of the db. A possible later improvement (depending on the queries) will be to also add the person's `skos:prefLabel` / `skos:altLabel` as an `author_names_bo` field.

also from https://eroux.fr/entityScores.ttl.gz import the object of `bdr:{id} tmp:entityScore` as field `db_score`.
