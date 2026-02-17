## Importing volume segments from existing outlines

When importing a volume (`(w_id, i_id)` from OCR, `(ie_id, ve_id)` from input etexts), it is necessary to import its segments from existing outlines. This operation requires a few steps.

#### lookups

The system must maintain the following maps (can be lazily fetched and kept in memory, no need to refresh at each import):
- `rep_id_to_mw_id`
- `mw_id_to_o_id`
- `o_id_to_rep_id`
- `vol_id_to_volume_numbers`
- `vol_id_to_pages_intro`

`rep_id_to_mw_id` is quite straightforward and maps `ie_id` and `w_id` to their corresponding `mw_id`. This map can be obtained by querying in sparql:

```sparql
select ?rep ?mw {
  ?rep :instanceReproductionOf ?mw .
}
```

`mw_id_to_o_id` has the correspondence between instances (`mw_id`) and their outlines (`o_id`):

```sparql
select ?mw ?o {
	?o :outlineOf ?mw .
	FILTER(exists {
		?oadm adm:adminAbout ?o ;
		      adm:status bda:StatusReleased .
		})
}
```

`o_id_to_rep_id` has the correspondence between the outlines (`o_id`) and the (possibly multiple) reproductions (`w_id` or `ie_id`) that are directly referenced in the content locations:

```sparql
select distinct ?o ?rep {
	?o :outlineOf ?mw .
	FILTER(exists {
		?oadm adm:adminAbout ?o ;
		      adm:status bda:StatusReleased .
		})
	?oadm adm:adminAbout ?o ;
	      adm:graphId ?og .
	graph ?og {
		?cl :contentLocationInstance ?rep .
	}
}
```

`vol_id_to_volume_numbers` is the result of

```sparql
select distinct ?v ?vnum {
	?v :volumeNumber ?vnum
}
```

`vol_id_to_volume_numbers` is the result of

```sparql
select distinct ?v ?vnum {
	?v :volumeNumber ?vnum
}
```

finally, `vol_id_to_pages_intro` is the result of

```sparql
select distinct ?v ?vnum {
	?v :volumePagesTbrcIntro ?pi
}
```

with a default of 0.

#### deriving import info

Once these lookups are available, the import can find the relevant outline and mode for the import info, which consists in:

```json
{
	"outline_id": "O...",
	"import_mode": "direct or no_location",
	"cl_rep_id": "W... or IE..."
}
```

The algorithm goes as follows:
- we call `(rep_id, vol_id)` the typle `(w_id, i_id)` or `(ie_id, ve_id)`
- get `mw_id` from `rep_id` in `rep_id_to_mw_id`
- get `o_id` from `mw_id` in `mw_id_to_o_id`
- if no `o_id`, then no outline is present for the volume, return
- if `rep_id` is in `o_id_to_rep_id`, then mode = direct and cl_rep_id = rep_id, else mode = no_location and cl_rep_id is any value from `o_id_to_rep_id`

#### Importing segments

Once the import info is done, the import reads `{hash}/{o_id}.trig` from https://gitlab.com/bdrc-data/outlines-20220922/ (after a recent pull), where `{hash}` is the first two digits of the md5 sum of `{o_id}`. The file is read as a trig file, with the main graph being `bdg:{o_id}`.

The import then reads the volume number for `vol_id` in `vol_id_to_volume_numbers`.

It then gets all the content locations for the `cl_rep_id` / volume number from the import info in the outline (using rdflib, we represent it here in sparql but python code will be used):

```sparql
select ?cl {
	?cl :contentLocationInstance ?cl_rep .
}
```

then filter on 
- `?cl :contentLocationVolume ?vnum` if `not exists(?cl :contentLocationEndVolume *)`
- `?cl :contentLocationVolume ?vstart ; :contentLocationEndVolume ?vend FILTER(?vend >= ?vnum && ?vstart <= ?vnum)` else

this will give all the content locations in the volume. Then get the mw parts (segments):

```
?mwpart :contentLocation ?cl ;
        :partType ?pt ;
        skos:prefLabel ?title ;
        :instanceOf ?wa .
```

and filter on `?pt in (bdr:PartTypeText, bdr:PartTypeEditorial, bdr:PartTypeTableOfContent)`  (PartTypeTableOfContent should be mapped to editorial).

For each mw part (segment), then import the coordinates if import_mode = direct (otherwise, no coordinates are imported). 

##### Import image coordinates (volumes from OCR)

If the volume is from ocr (`(w_id, i_id)`), then coordinates are page numbers (which in our case are really "image numbers"):

```sparql
   ?cl :contentLocationPage ?pstart .
   ?cl :contentLocationEndPage ?pend .
```

with the convention that:
- if there's no `?pstart`, the content location is assumed to start at the beginning of the volume
- if there is no `?pend` it is assumed to end at the end of the volume.

Be mindful that if there is a `?pend` but the contentLocationEndVolume is > vnum, then the content location ends at the end of the volume (as we're only looking at one volume).

Then order the mw by ascending `?pstart` and organize them in a flat structure. 

Add some warnings in the data for the js client if:
- there is an erroneous overlap, `?pend` > next `?pstart`
- there is a `?pend` = next `?pstart`, meaning there is probably a page break in the middle of a page, which will need to be indicated precisely by the annotators

Then look at pagination information to map image numbers to character coordinates and use these for the segment character coordinates.

##### Import etext coordinates

If the volume is from manual etexts (`(ie_id, ve_id)`), then coordinates are etext numbers and milestone IDs:

```sparql
   ?cl :contentLocationEtext ?estart .
   ?cl :contentLocationIdInEtext ?idstart .
   ?cl :contentLocationEndEtext ?eend .
   ?cl :contentLocationEndIdInEtext ?idend .
```

with the convention that:
- if there's no `?estart`, the content location is assumed to start at the beginning of the volume
- if there's no `?idstart`, the content location is assumed to start at the beginning of the etext
- if there's no `?eend`, the content location is assumed to end at the end of the volume
- if there's no `?idend`, the content location is assumed to end at the etext

Be mindful that if there is a `?eend` / `?idend` but the contentLocationEndVolume is > vnum, then the content location ends at the end of the volume (as we're only looking at one volume).

Then order the mw by ascending `?estart` and organize them in a flat structure. 

Add some warnings in the data for the js client if:
- there is an erroneous overlap, `?eend` > next `?estart`
- there is a `?eend` = next `?estart` with no `?idend` / next `?idstart`, meaning there is probably a page break in the middle of a page, which will need to be indicated precisely by the annotators

Then import the etexts using the code in https://github.com/buda-base/ao_etexts/tree/main/bdrc_etext_sync with the proper segment character coordinates (the code is not very easy, use AI to understand it and connect to it).