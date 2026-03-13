xquery version "3.1";

(:~
 : TEI Annotation Extraction with Pre-fetch Caching
 : 
 : This script extracts annotation data from TEI/XML files and enriches them
 : with entity names from Airtable. It uses a three-pass approach:
 : 
 : Pass 1: Scan all files to collect unique entity references (no API calls)
 : Pass 2: Batch-fetch all entities from Airtable, build cache
 : Pass 3: Process files using the pre-built cache (no API calls)
 : 
 : This approach minimizes API calls from potentially thousands down to
 : just the number of unique entities divided by batch size.
 :)

import module namespace airtable="http://joewiz.org/ns/xquery/airtable" at "/db/system/repo/airtable-1.0.3/content/airtable.xqm";
declare namespace util = "http://exist-db.org/xquery/util";
declare namespace xmldb = "http://exist-db.org/xquery/xmldb";
declare namespace tei = "http://www.tei-c.org/ns/1.0";

(: ============================================================================
   1. CONFIGURATION - Update these values for your environment
   ============================================================================ :)

(: Airtable credentials - UPDATE THESE :)
declare variable $access-token := "patYQmtPBGxqXxniZ.7b5d1334c05929224fc30f5dfc48f6ab4e3864d7da0a6e02bf11dcdae09b9790";
declare variable $base-id := "appzWKtDLcSfCuSiM";

(: Volume to process :)
declare variable $volume-id := "frus1977-80v24";

(: Processing settings :)
declare variable $batch-size := 20;        (: Files per batch for progress reporting :)
declare variable $api-batch-size := 50;    (: Records per Airtable API call :)
declare variable $process-all := true();  (: false() for sample/testing :)
declare variable $sample-size := 20;       (: Number of files if not processing all :)
declare variable $max-batches := 0;        (: Maximum batches (0 = all) :)
declare variable $start-batch := 1;        (: Starting batch (for resuming) :)

(: Paths :)
declare variable $xml-source-path := "/db/apps/hsg-annotate-data/data/documents";
declare variable $output-collection := "/db/apps/hsg-annotate-data/tei";
declare variable $output-filename := "annotations_" || $volume-id || ".xml";
declare variable $progress-filename := "progress_" || $volume-id || ".xml";

(: Table IDs :)
declare variable $documents-table-id := "tblpwobb3qQ58wQW7";
declare variable $people-table-id := "tblJj0p0djKMxkGET";
declare variable $places-table-id := "tblGyzmMjH1A18evN";
declare variable $organizations-table-id := "tblkd0LONkYSivrgR";
declare variable $events-table-id := "tbljW32VQlug8N1ST";
declare variable $topics-table-id := "tbljhk4lm5Z7Qs7IE";
declare variable $agreements-table-id := "tblKYfd6ZKTA4eEa3";
declare variable $compound-subjects-table-id := "tbl617ssPfUwVj3Zo";
declare variable $programs-table-id := "tblGo1pT25lXFfl1D";
declare variable $works-table-id := "tblezNv4li1P4S5Ay";
declare variable $index-entries-table-id := "tbl8hs3QzcPkYYTE7";

(: ============================================================================
   2. LOGGING AND PROGRESS TRACKING
   ============================================================================ :)

declare function local:log($level as xs:string, $component as xs:string, $message as xs:string) {
    let $timestamp := format-dateTime(current-dateTime(), "[Y]-[M01]-[D01] [H01]:[m01]:[s01]")
    let $formatted := concat($timestamp, " [", $level, "] [", $component, "] ", $message)
    return util:log($level, $formatted)
};

declare function local:save-progress(
    $phase as xs:string,
    $batch-num as xs:integer, 
    $total-batches as xs:integer,
    $items-processed as xs:integer, 
    $total-items as xs:integer,
    $annotation-count as xs:integer
) {
    let $progress := 
        <progress>
            <timestamp>{current-dateTime()}</timestamp>
            <volume-id>{$volume-id}</volume-id>
            <phase>{$phase}</phase>
            <batch-number>{$batch-num}</batch-number>
            <total-batches>{$total-batches}</total-batches>
            <items-processed>{$items-processed}</items-processed>
            <total-items>{$total-items}</total-items>
            <annotation-count>{$annotation-count}</annotation-count>
            <percent-complete>{
                if ($total-items > 0) then 
                    format-number($items-processed div $total-items * 100, "##0.0")
                else "0.0"
            }%</percent-complete>
        </progress>
    return
        try {
            xmldb:store($output-collection, $progress-filename, $progress),
            local:log("INFO", "progress", $phase || " - " || $items-processed || "/" || $total-items)
        } catch * {
            local:log("ERROR", "progress", "Failed to save progress: " || $err:description)
        }
};

(: ============================================================================
   3. TABLE/TYPE MAPPING FUNCTIONS
   ============================================================================ :)

declare function local:get-table-id($element-name as xs:string, $type as xs:string?) as xs:string {
    switch($element-name)
        case "persName" return $people-table-id
        case "placeName" return $places-table-id
        case "orgName" return $organizations-table-id
        case "rs" return
            switch($type)
                case "event" return $events-table-id
                case "topic" return $topics-table-id
                case "agreement" return $agreements-table-id
                case "compound-subject" return $compound-subjects-table-id
                case "program" return $programs-table-id
                case "work" return $works-table-id
                case "index-entry" return $index-entries-table-id
                default return $topics-table-id
        default return $topics-table-id
};

declare function local:get-entity-type($table-id as xs:string) as xs:string {
    switch($table-id)
        case $people-table-id return "People"
        case $places-table-id return "Places"
        case $organizations-table-id return "Organizations"
        case $events-table-id return "Events"
        case $topics-table-id return "Topics"
        case $agreements-table-id return "Agreements and Mandates"
        case $compound-subjects-table-id return "Compound Subjects"
        case $programs-table-id return "Programs"
        case $works-table-id return "Works"
        case $index-entries-table-id return "Index Entries"
        default return "Unknown"
};

(: ============================================================================
   4. PASS 1: COLLECT UNIQUE ENTITY REFERENCES (No API calls)
   ============================================================================ :)

(:~
 : Scans all files and collects unique (table-id, record-id) pairs.
 : Returns a map where keys are table IDs and values are sequences of record IDs.
 :)
declare function local:collect-entity-refs($files as xs:string*) as map(*) {
    let $_ := local:log("INFO", "collect", "Scanning " || count($files) || " files for entity references")
    let $volume-path := $xml-source-path || "/" || $volume-id
    
    (: Collect all refs from all files :)
    let $all-refs :=
        for $resource in $files
        let $doc := 
            try { doc($volume-path || "/" || $resource) } 
            catch * { () }
        where exists($doc)
        for $element in $doc//tei:*[@corresp]
        let $corresp := string($element/@corresp)
        let $table-id := local:get-table-id(local-name($element), string($element/@type))
        where $corresp != ""
        return 
            <ref table="{$table-id}" id="{$corresp}"/>
    
    let $_ := local:log("INFO", "collect", "Found " || count($all-refs) || " total entity references")
    
    (: Group by table and get distinct IDs :)
    let $grouped := map:merge(
        for $ref in $all-refs
        group by $table := string($ref/@table)
        let $unique-ids := distinct-values($ref/@id)
        return map:entry($table, $unique-ids)
    )
    
    let $total-unique := sum(for $table in map:keys($grouped) return count(map:get($grouped, $table)))
    let $_ := local:log("INFO", "collect", "Found " || $total-unique || " unique entities across " || 
                                          count(map:keys($grouped)) || " tables")
    
    return $grouped
};

(: ============================================================================
   5. PASS 2: BATCH FETCH ENTITIES FROM AIRTABLE
   ============================================================================ :)

(:~
 : Splits a sequence into chunks of specified size.
 :)
declare function local:chunk-sequence($seq as item()*, $size as xs:integer) as array(*)* {
    let $count := count($seq)
    for $i in 1 to xs:integer(ceiling($count div $size))
    let $start := ($i - 1) * $size + 1
    return array { subsequence($seq, $start, $size) }
};

(:~
 : Fetches all entities for a single table using batched API calls.
 : Uses filterByFormula with RECORD_ID() to fetch specific records.
 :)
declare function local:fetch-entities-for-table($table-id as xs:string, $record-ids as xs:string*) as map(*) {
    let $entity-type := local:get-entity-type($table-id)
    let $_ := local:log("INFO", "fetch", "Fetching " || count($record-ids) || " " || $entity-type || " entities")
    
    let $chunks := local:chunk-sequence($record-ids, $api-batch-size)
    let $total-chunks := count($chunks)
    
    let $results := 
        for $chunk at $idx in $chunks
        let $_ := local:log("INFO", "fetch", $entity-type || " batch " || $idx || "/" || $total-chunks)
        
        (: Build filter formula: OR(RECORD_ID()='rec1', RECORD_ID()='rec2', ...) :)
        let $formula := "OR(" || string-join(
            for $id in $chunk?*
            return "RECORD_ID()='" || $id || "'"
        , ",") || ")"
        
        let $response := 
            try {
                airtable:list-records(
                    $access-token, 
                    $base-id, 
                    $table-id,
                    true(),     (: load-multiple-pages :)
                    (),         (: fields :)
                    $formula,   (: filter-by-formula :)
                    (),         (: max-records :)
                    (),         (: page-size :)
                    (),         (: sort :)
                    (),         (: view :)
                    ()          (: offset :)
                )
            } catch * {
                local:log("ERROR", "fetch", "API error for " || $entity-type || ": " || $err:description),
                map {}
            }
        
        let $records := 
            if (exists($response?records)) then 
                $response?records?* 
            else ()
        
        for $rec in $records
        let $name := 
            if (exists($rec?fields?Name)) then $rec?fields?Name
            else if (exists($rec?fields?("Full Name"))) then $rec?fields?("Full Name")
            else if (exists($rec?fields?("Index entry"))) then $rec?fields?("Index entry")
            else "[Unknown]"
        return 
            map:entry($table-id || "#" || $rec?id, $name)
    
    let $result-map := map:merge($results)
    let $_ := local:log("INFO", "fetch", "Retrieved " || map:size($result-map) || " " || $entity-type || " names")
    
    return $result-map
};

(:~
 : Fetches all entities from all tables and builds a complete cache.
 :)
declare function local:build-entity-cache($entity-refs as map(*)) as map(*) {
    let $_ := local:log("INFO", "cache", "Building entity cache from Airtable")
    
    let $table-results :=
        for $table-id in map:keys($entity-refs)
        let $record-ids := map:get($entity-refs, $table-id)
        where count($record-ids) > 0
        return local:fetch-entities-for-table($table-id, $record-ids)
    
    let $cache := map:merge($table-results)
    let $_ := local:log("INFO", "cache", "Entity cache built with " || map:size($cache) || " entries")
    
    return $cache
};

(: ============================================================================
   6. PASS 3: PROCESS FILES USING CACHE
   ============================================================================ :)

(:~
 : Processes a single TEI file using the pre-built cache.
 : No API calls are made in this function.
 :)
declare function local:process-file(
    $resource as xs:string, 
    $file-num as xs:integer, 
    $total-files as xs:integer, 
    $doc-id-map as map(*), 
    $entity-cache as map(*)
) as element()* {
    let $volume-path := $xml-source-path || "/" || $volume-id
    let $full-path := $volume-path || "/" || $resource
    let $doc-id := replace($resource, "\.xml$", "")
    
    let $document-record-id := 
        if (map:contains($doc-id-map, $doc-id)) then
            map:get($doc-id-map, $doc-id)
        else
            "[Not Found]"
    
    return
        try {
            let $tei-doc := doc($full-path)
            let $elements := $tei-doc//tei:*[@corresp]
            
            for $element in $elements
            let $corresp := string($element/@corresp)
            let $element-name := local-name($element)
            let $type := string($element/@type)
            let $text := 
                let $raw := normalize-space(string-join($element//text(), " "))
                return if ($raw != "") then $raw else "[Empty]"
            
            where $corresp != ""
            
            let $table-id := local:get-table-id($element-name, $type)
            let $entity-type := local:get-entity-type($table-id)
            let $cache-key := $table-id || "#" || $corresp
            
            (: Look up in pre-built cache - no API call :)
            let $entity-name :=
                if (map:contains($entity-cache, $cache-key)) then
                    map:get($entity-cache, $cache-key)
                else
                    $text  (: Fallback to annotation text :)
            
            return
                <entry>
                    <recordID>{$corresp}</recordID>
                    <tableID>{$table-id}</tableID>
                    <table_name>{$entity-type}</table_name>
                    <annotation_content>{$text}</annotation_content>
                    <entity_name>{$entity-name}</entity_name>
                    <annotation_properties>
                        <elements>
                            <element>{$element-name}</element>
                        </elements>
                        <types>
                            <type>{$type}</type>
                        </types>
                    </annotation_properties>
                    <documents>
                        <document>
                            <doc_number>{$volume-id || "#" || $doc-id}</doc_number>
                            <documentId>{$document-record-id}</documentId>
                        </document>
                    </documents>
                </entry>
        } catch * {
            local:log("ERROR", "file", "Error processing " || $resource || ": " || $err:description),
            ()
        }
};

(:~
 : Processes a batch of files.
 :)
declare function local:process-batch(
    $resources as xs:string*, 
    $batch-num as xs:integer, 
    $total-batches as xs:integer,
    $start-file-num as xs:integer, 
    $total-files as xs:integer,
    $doc-id-map as map(*), 
    $entity-cache as map(*)
) as element()* {
    let $start-time := current-dateTime()
    let $_ := local:log("INFO", "batch", "Processing batch " || $batch-num || "/" || $total-batches || 
                                        " (" || count($resources) || " files)")
    
    let $annotations := 
        for $resource at $idx in $resources
        let $file-num := $start-file-num + $idx - 1
        return local:process-file($resource, $file-num, $total-files, $doc-id-map, $entity-cache)
    
    let $duration := seconds-from-duration(current-dateTime() - $start-time)
    let $_ := local:log("INFO", "batch", "Batch " || $batch-num || " completed in " || 
                                        format-number($duration, "0.00") || "s (" || 
                                        count($annotations) || " annotations)")
    
    return $annotations
};

(: ============================================================================
   7. GROUP AND DEDUPLICATE RESULTS
   ============================================================================ :)

declare function local:group-entries($entries as element()*) as element()* {
    let $_ := local:log("INFO", "group", "Grouping " || count($entries) || " entries by entity ID")
    
    let $grouped :=
        for $entity-id in distinct-values($entries/recordID)
        let $entity-entries := $entries[recordID = $entity-id]
        let $first := $entity-entries[1]
        
        (: Deduplicate document references :)
        let $unique-docs := 
            for $doc in $entity-entries/documents/document
            group by $doc-num := $doc/doc_number/text()
            return $doc[1]
        
        return
            <entry>
                <recordID>{$entity-id}</recordID>
                <tableID>{$first/tableID/text()}</tableID>
                <table_name>{$first/table_name/text()}</table_name>
                <annotation_content>{$first/annotation_content/text()}</annotation_content>
                <entity_name>{$first/entity_name/text()}</entity_name>
                {$first/annotation_properties}
                <documents>
                    {$unique-docs}
                </documents>
            </entry>
    
    let $_ := local:log("INFO", "group", "Grouped into " || count($grouped) || " unique entities")
    return $grouped
};

(: ============================================================================
   8. MAIN PROCESSING PIPELINE
   ============================================================================ :)

let $start-time := current-dateTime()
let $_ := local:log("INFO", "main", "=== Starting TEI Annotation Extraction for " || $volume-id || " ===")

(: Ensure output collection exists :)
let $_ := 
    if (not(xmldb:collection-available($output-collection))) then
        xmldb:create-collection("/db", "generated")
    else ()

(: --- Load document records from Airtable --- :)
let $_ := local:log("INFO", "main", "Loading document records from Airtable")
let $document-records := 
    try {
        let $all-docs := airtable:list-records(
            $access-token, 
            $base-id, 
            $documents-table-id,
            true(),  (: load-multiple-pages :)
            (),      (: fields :)
            (),      (: filter-by-formula :)
            (),      (: max-records :)
            (),      (: page-size :)
            (),      (: sort :)
            (),      (: view :)
            ()       (: offset :)
        )
        let $records := if (exists($all-docs?records)) then $all-docs?records?* else ()
        
        let $volume-docs := 
            for $rec in $records
            where exists($rec?fields?Document) and starts-with($rec?fields?Document, $volume-id)
            return $rec
        
        let $_ := local:log("INFO", "main", "Found " || count($volume-docs) || " documents for " || $volume-id)
        return $volume-docs
    } catch * {
        local:log("ERROR", "main", "Failed to load documents: " || $err:description),
        ()
    }

(: Build document ID lookup map :)
let $doc-id-map := map:merge(
    for $doc in $document-records
    let $doc-number := $doc?fields?Document
    let $doc-id := if (contains($doc-number, "#")) then substring-after($doc-number, "#") else ""
    where $doc-id != ""
    return map:entry($doc-id, $doc?id)
)
let $_ := local:log("INFO", "main", "Document ID map: " || map:size($doc-id-map) || " entries")

(: --- Get files to process --- :)
let $volume-path := $xml-source-path || "/" || $volume-id
let $all-files := 
    try {
        xmldb:get-child-resources($volume-path)
    } catch * {
        local:log("ERROR", "main", "Cannot read " || $volume-path || ": " || $err:description),
        ()
    }

let $files-to-process := 
    if ($process-all) then $all-files
    else subsequence($all-files, 1, $sample-size)

let $total-files := count($files-to-process)
let $_ := local:log("INFO", "main", "Will process " || $total-files || " files")

(: --- PASS 1: Collect unique entity references --- :)
let $_ := local:log("INFO", "main", "=== PASS 1: Collecting entity references ===")
let $entity-refs := local:collect-entity-refs($files-to-process)

(: --- PASS 2: Build entity cache from Airtable --- :)
let $_ := local:log("INFO", "main", "=== PASS 2: Fetching entities from Airtable ===")
let $entity-cache := local:build-entity-cache($entity-refs)

(: --- PASS 3: Process files using cache --- :)
let $_ := local:log("INFO", "main", "=== PASS 3: Processing files ===")

let $total-batches := xs:integer(ceiling($total-files div $batch-size))
let $end-batch := 
    if ($max-batches > 0) then min(($start-batch + $max-batches - 1, $total-batches))
    else $total-batches

let $_ := local:log("INFO", "main", "Processing batches " || $start-batch || " to " || $end-batch || 
                                   " of " || $total-batches)

(: Process all batches :)
let $all-annotations := 
    let $initial := map { "annotations": (), "count": 0 }
    
    let $final := fold-left(
        ($start-batch to $end-batch),
        $initial,
        function($state, $batch-num) {
            let $start-idx := ($batch-num - 1) * $batch-size + 1
            let $end-idx := min(($batch-num * $batch-size, $total-files))
            let $batch-files := subsequence($files-to-process, $start-idx, $end-idx - $start-idx + 1)
            
            let $batch-results := local:process-batch(
                $batch-files, $batch-num, $end-batch, $start-idx, $total-files, 
                $doc-id-map, $entity-cache
            )
            
            let $new-count := $state?count + count($batch-results)
            let $_ := local:save-progress("processing", $batch-num, $total-batches, 
                                         $end-idx, $total-files, $new-count)
            
            return map {
                "annotations": ($state?annotations, $batch-results),
                "count": $new-count
            }
        }
    )
    
    return $final?annotations

(: --- Group and deduplicate --- :)
let $_ := local:log("INFO", "main", "=== Grouping annotations ===")
let $grouped-annotations := local:group-entries($all-annotations)

(: --- Build and save output --- :)
let $output-xml :=
    <annotations>
        <processing_info>
            <timestamp>{current-dateTime()}</timestamp>
            <volume_id>{$volume-id}</volume_id>
            <files_processed>{$total-files}</files_processed>
            <batches_processed>{$end-batch - $start-batch + 1}</batches_processed>
            <total_batches>{$total-batches}</total_batches>
            <document_count>{count($document-records)}</document_count>
            <annotation_count>{count($all-annotations)}</annotation_count>
            <unique_entity_count>{count($grouped-annotations)}</unique_entity_count>
            <entity_cache_size>{map:size($entity-cache)}</entity_cache_size>
        </processing_info>
        {$grouped-annotations}
    </annotations>

let $_ := 
    try {
        xmldb:store($output-collection, $output-filename, $output-xml),
        local:log("INFO", "main", "Output saved to " || $output-collection || "/" || $output-filename)
    } catch * {
        local:log("ERROR", "main", "Failed to save output: " || $err:description)
    }

let $duration := seconds-from-duration(current-dateTime() - $start-time)
let $_ := local:log("INFO", "main", "=== Completed in " || format-number($duration, "0.00") || "s ===")

return
    <success>
        <message>Extracted TEI annotations for volume {$volume-id}</message>
        <output_path>{$output-collection}/{$output-filename}</output_path>
        <stats>
            <files_processed>{$total-files}</files_processed>
            <total_annotations>{count($all-annotations)}</total_annotations>
            <unique_entities>{count($grouped-annotations)}</unique_entities>
            <entity_cache_size>{map:size($entity-cache)}</entity_cache_size>
            <processing_time>{format-number($duration, "0.00")}s</processing_time>
        </stats>
        <efficiency>
            <api_calls_saved>{count($all-annotations) - map:size($entity-cache)}</api_calls_saved>
            <reduction_percent>{
                if (count($all-annotations) > 0) then
                    format-number((1 - map:size($entity-cache) div count($all-annotations)) * 100, "0.0")
                else "0"
            }%</reduction_percent>
        </efficiency>
    </success>
