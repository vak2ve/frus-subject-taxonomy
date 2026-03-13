xquery version "3.1";

(:~
 : Step 2: Push Annotations to Airtable Index Entries
 : This script reads extracted annotations and creates/updates Index Entries
 : in Airtable, linking entities to their documents.
 :)

(: Import airtable module :)
import module namespace airtable="http://joewiz.org/ns/xquery/airtable" at "/db/system/repo/airtable-1.0.3/content/airtable.xqm";
declare namespace util = "http://exist-db.org/xquery/util";

(: ============================================================================
   1. CONFIGURATION
   ============================================================================ :)

(: Airtable credentials :)
declare variable $access-token := "patjM1uPFnFBa41p1.52fd0acfb1905dec703438b7e20cf9e45c64e8a8271dc563ca198ccfd7e04ec1";
declare variable $base-id := "apppAb6AEJB9YfHf4";

(: Project configuration - FIXED volume ID :)
declare variable $project-id := "frus1977-80v24";
declare variable $project-record-id := "rec9niBEm7zZWVRU8";

(: PROCESSING SETTINGS :)
declare variable $batch-size := 5;           (: Entries per batch :)
declare variable $restart-batch := 1;        (: Batch number to start from (for resuming after crash) :)

(: Table IDs :)
declare variable $index-entries-table-id := "tbl8hs3QzcPkYYTE7";
declare variable $documents-table-id := "tblpwobb3qQ58wQW7";
declare variable $people-table-id := "tblJj0p0djKMxkGET";
declare variable $places-table-id := "tblGyzmMjH1A18evN";
declare variable $organizations-table-id := "tblkd0LONkYSivrgR";
declare variable $events-table-id := "tbljW32VQlug8N1ST";
declare variable $topics-table-id := "tbljhk4lm5Z7Qs7IE";
declare variable $agreements-or-mandates-table-id := "tblKYfd6ZKTA4eEa3";
declare variable $compound-subjects-table-id := "tbl617ssPfUwVj3Zo";
declare variable $programs-table-id := "tblGo1pT25lXFfl1D";
declare variable $works-table-id := "tblezNv4li1P4S5Ay";

(: ============================================================================
   2. UTILITY FUNCTIONS
   ============================================================================ :)

declare function local:serialize-map($map as map(*)) as xs:string {
    serialize($map, map { "method": "json", "indent": true() })
};

declare function local:format-error($err as item()*) as xs:string {
    if (empty($err)) then "[empty error]"
    else if ($err instance of map(*)) then
        if (map:contains($err, "body") and $err?body instance of map(*) and map:contains($err?body, "error")) then
            let $api-error := $err?body?error
            return 
                if ($api-error instance of map(*)) then
                    ($api-error?type, "UNKNOWN")[1] || ": " || ($api-error?message, "no message")[1]
                else
                    string($api-error)
        else
            local:serialize-map($err)
    else
        string($err)
};

declare function local:log($level as xs:string, $message as xs:string) {
    let $timestamp := format-dateTime(current-dateTime(), "[Y]-[M01]-[D01] [H01]:[m01]:[s01]")
    return util:log($level, $timestamp || " [" || $level || "] " || $message)
};

(: ============================================================================
   3. TABLE/TYPE MAPPING FUNCTIONS
   ============================================================================ :)

declare function local:get-entity-column-name($table-id as xs:string) as xs:string {
    switch($table-id)
        case $index-entries-table-id return "Index entry"
        case $people-table-id return "(1) Person"
        case $places-table-id return "(1) Place"
        case $organizations-table-id return "(1) Organization"
        case $events-table-id return "(1) Event"
        case $topics-table-id return "(1) Topic"
        case $agreements-or-mandates-table-id return "(1) Agreement and Mandate"
        case $compound-subjects-table-id return "(1) Compound Subject"
        case $programs-table-id return "(1) Program"
        case $works-table-id return "(1) Work"
        default return 
            let $_ := local:log("ERROR", "Unknown table ID: " || $table-id || ", using Topic as fallback")
            return "(1) Topic"
};

declare function local:is-main-only-entry-type($entry-type as xs:string?) as xs:boolean {
    let $main-only-types := ("Main-only entry", "Main-only")
    let $normalized-type := normalize-space($entry-type)
    
    return
        if (not(exists($entry-type))) then
            true()
        else
            some $type in $main-only-types satisfies 
                $normalized-type = $type or
                starts-with($normalized-type, $type) or
                contains($normalized-type, $type)
};

(: ============================================================================
   4. INDEX ENTRY MATCHING
   ============================================================================ :)

declare function local:find-matching-entry-comprehensive(
    $entity-id as xs:string, 
    $entity-name as xs:string,
    $entity-column as xs:string
) as map(*)? {
    (: Escape single quotes in entity name for Airtable formula :)
    let $escaped-name := replace($entity-name, "'", "\\'")
    
    (: Search by entity column display name — this finds Index Entries 
       where the linked entity (e.g., (1) Person) matches by display name :)
    let $filter-formula := "OR({" || $entity-column || "}='" || $escaped-name || "', {Index entry}='" || $escaped-name || "')"
    
    let $_ := local:log("INFO", "Searching with filter: " || $filter-formula)
    
    let $all-entries := airtable:list-records(
        $access-token, 
        $base-id, 
        $index-entries-table-id, 
        true(),  (: load-multiple-pages :)
        (),      (: fields :)
        $filter-formula,   (: filter-by-formula :)
        (),      (: max-records :)
        (),      (: page-size :)
        (),      (: sort :)
        (),      (: view :)
        ()       (: offset :)
    )
    
    let $_ := local:log("INFO", "Looking for match with entity name: " || $entity-name || 
                      ", in column: " || $entity-column ||
                      ", results: " || count($all-entries?records?*))
    
    (: Priority 1: Main-only entry type (updatable) :)
    let $updatable-matches := 
        for $record in $all-entries?records?*
        where 
            not(map:contains($record?fields, "Entry type")) or
            local:is-main-only-entry-type($record?fields?("Entry type"))
        return $record
    
    (: Priority 2: Any match :)
    let $any-matches := $all-entries?records?*
    
    (: Log what we found :)
    let $_ := 
        if (exists($updatable-matches)) then
            local:log("INFO", "Found " || count($updatable-matches) || " updatable matches")
        else if (exists($any-matches)) then
            local:log("INFO", "Found " || count($any-matches) || " matches but none updatable")
        else
            local:log("INFO", "No matches found for '" || $entity-name || "' in column " || $entity-column)
    
    (: Return best match :)
    return 
        if (exists($updatable-matches)) then $updatable-matches[1]
        else ()
};

(: ============================================================================
   5. CREATE/UPDATE INDEX ENTRIES
   ============================================================================ :)

declare function local:create-index-entry(
    $entity-id as xs:string, 
    $table-id as xs:string,
    $element as xs:string?,
    $type as xs:string?,
    $entity_name as xs:string,
    $document-ids as xs:string*,
    $project-id as xs:string
) as map(*) {
    let $entity-column := local:get-entity-column-name($table-id)
    
    let $_ := local:log("INFO", "Creating entry with entity column: " || $entity-column || 
                     " for entity ID: " || $entity-id || " with content: " || $entity_name)
    
    let $fields := map {
        $entity-column: array { $entity-id },
        "Documents": array { $document-ids },
        "Project IDs": array { $project-record-id }
    }
            
    let $_ := local:log("INFO", "Fields for new entry: " || local:serialize-map($fields))
    
    let $record := map { "fields": $fields }
    
    let $response := airtable:create-records(
        $access-token, 
        $base-id, 
        $index-entries-table-id, 
        $record
    )
    
    let $_ := local:log("INFO", "Create response: " || local:serialize-map($response))
    
    return $response
};

declare function local:update-index-entry-documents(
    $record-id as xs:string, 
    $document-ids as xs:string*, 
    $project-id as xs:string
) as map(*) {
    let $entry := airtable:retrieve-record($access-token, $base-id, $index-entries-table-id, $record-id)
    
    return
        if (empty($entry) or not($entry instance of map(*)) or not(map:contains($entry, "fields"))) then (
            local:log("WARN", "Record not found in Airtable: " || $record-id || ", skipping"),
            map { "error": "Record not found: " || $record-id }
        )
        else
            let $_ := local:log("INFO", "Found existing entry: " || $record-id || 
                             " with content: " || ($entry?fields?("Index entry"), "N/A")[1])
            
            let $existing-docs := 
                if (map:contains($entry?fields, "Documents")) then
                    $entry?fields?Documents?*
                else
                    ()
            
            let $existing-project-ids := 
                if (map:contains($entry?fields, "Project IDs")) then
                    $entry?fields?("Project IDs")?*
                else
                    ()
            
            (: Simply add new documents to existing ones - distinct-values handles dedup :)
            let $updated-docs := distinct-values(($existing-docs, $document-ids))
            
            let $updated-project-ids := distinct-values(($existing-project-ids, $project-record-id))
            
            let $_ := local:log("INFO", "Existing: " || count($existing-docs) || " docs, adding " || 
                            count($document-ids) || " new, total: " || count($updated-docs))
            
            let $record := map { 
                "id": $record-id,
                "fields": map { 
                    "Documents": array { $updated-docs },
                    "Project IDs": array { $updated-project-ids }
                }
            }
            
            let $_ := local:log("INFO", "Updating entry " || $record-id || 
                              " with " || count($updated-docs) || " total documents")
            
            let $response := airtable:update-records(
                $access-token, 
                $base-id, 
                $index-entries-table-id, 
                $record,
                false()
            )
            
            return $response
};

(: ============================================================================
   6. ENTRY PROCESSING
   ============================================================================ :)

declare function local:process-entry($entry as element(), $project-id as xs:string) as xs:string* {
    let $entity-id := $entry/recordID/text()
    let $table-id := $entry/tableID/text()
    let $annotation-text := $entry/annotation_content/text()
    let $entity-name := $entry/entity_name/text()
    
    let $_ := local:log("INFO", "Processing: " || $annotation-text || " / " || $entity-name || " (ID: " || $entity-id || ", table: " || $entry/table_name/text() || ")")
    
    let $element := string-join($entry/annotation_properties/elements/element/text(), ", ")
    let $type := string-join($entry/annotation_properties/types/type/text(), ", ")
    
    let $document-ids := 
        for $doc in $entry/documents/document
        where starts-with($doc/doc_number/text(), $project-id)
        return $doc/documentId/text()
    
    let $_ := local:log("INFO", "Found " || count($document-ids) || " documents for project " || $project-id)

    return
        if (not(exists($entity-id)) or string-length($entity-id) = 0) then
            "SKIPPED (no record ID): " || $annotation-text
        else if (empty($document-ids)) then
            "SKIPPED (no documents for project): " || $annotation-text
        else if ($table-id = $index-entries-table-id) then
            (: INDEX ENTRIES: @corresp IS the record ID — just update directly :)
            let $update := local:update-index-entry-documents(
                $entity-id, $document-ids, $project-id
            )
            return 
                if (map:contains($update, "error")) then
                    "ERROR updating: " || $annotation-text || " (ID: " || $entity-id || ") - " || local:format-error($update?error)
                else
                    "UPDATED: " || $annotation-text || " (" || count($document-ids) || " docs)"
        else
            (: ALL OTHER TYPES: find existing Index Entry or create new one :)
            let $entity-column := local:get-entity-column-name($table-id)
            let $matching-entry := local:find-matching-entry-comprehensive($entity-id, $entity-name, $entity-column)
            
            return
                if (exists($matching-entry)) then
                    if (map:contains($matching-entry?fields, "Entry type") and 
                        not(local:is-main-only-entry-type($matching-entry?fields?("Entry type")))) then
                        let $_ := local:log("INFO", "Match found but not updatable (type: " || 
                                          $matching-entry?fields?("Entry type") || "), creating new entry")
                        let $create := local:create-index-entry(
                            $entity-id, $table-id, $element, $type, 
                            $annotation-text, $document-ids, $project-id
                        )
                        return 
                            if (map:contains($create, "error")) then
                                "ERROR creating (non-updatable match): " || $annotation-text || " - " || local:format-error($create?error)
                            else
                                "CREATED (non-updatable match existed): " || $annotation-text || " (" || count($document-ids) || " docs)"
                    else
                        let $update := local:update-index-entry-documents(
                            $matching-entry?id, $document-ids, $project-id
                        )
                        return 
                            if (map:contains($update, "error")) then
                                "ERROR updating: " || $annotation-text || " - " || local:format-error($update?error)
                            else
                                "UPDATED: " || $annotation-text || " (" || count($document-ids) || " docs)"
                else
                    let $create := local:create-index-entry(
                        $entity-id, $table-id, $element, $type, 
                        $annotation-text, $document-ids, $project-id
                    )
                    return 
                        if (map:contains($create, "error")) then
                            "ERROR creating: " || $annotation-text || " - " || local:format-error($create?error)
                        else
                            "CREATED: " || $annotation-text || " (" || count($document-ids) || " docs)"
};

(: ============================================================================
   7. BATCH PROCESSING
   ============================================================================ :)

declare function local:process-batch(
    $entries as element()*, 
    $batch-num as xs:integer, 
    $project-id as xs:string
) as element() {
    let $start-time := current-dateTime()
    
    let $results := 
        for $entry at $pos in $entries
        let $_ := local:log("INFO", "Batch " || $batch-num || ", entry " || $pos || "/" || count($entries))
        return local:process-entry($entry, $project-id)
    
    let $duration := seconds-from-duration(current-dateTime() - $start-time)
    
    return
        <batch>
            <number>{$batch-num}</number>
            <entries-count>{count($entries)}</entries-count>
            <duration>{format-number($duration, "0.00")}s</duration>
            <results>
                {
                    for $result at $i in $results
                    return <result index="{$i}">{$result}</result>
                }
            </results>
        </batch>
};

declare function local:process-entries(
    $entries as element()*, 
    $batch-size as xs:integer, 
    $project-id as xs:string, 
    $start-batch as xs:integer
) as element() {
    let $total := count($entries)
    let $total-batches := xs:integer(ceiling($total div $batch-size))
    
    let $_ := local:log("INFO", "Processing " || $total || " entries in " || $total-batches || 
                       " batches, starting from batch " || $start-batch)
    
    return
        <results>
            <config>
                <total-entries>{$total}</total-entries>
                <batch-size>{$batch-size}</batch-size>
                <total-batches>{$total-batches}</total-batches>
                <starting-batch>{$start-batch}</starting-batch>
                <project-id>{$project-id}</project-id>
            </config>
            <start-time>{current-dateTime()}</start-time>
            {
                for $batch-num in $start-batch to $total-batches
                let $start-idx := ($batch-num - 1) * $batch-size + 1
                let $end-idx := min(($batch-num * $batch-size, $total))
                let $batch-entries := $entries[position() >= $start-idx and position() <= $end-idx]
                let $_ := local:log("INFO", "=== Batch " || $batch-num || "/" || $total-batches || 
                                 " (entries " || $start-idx || "-" || $end-idx || ") ===")
                return local:process-batch($batch-entries, $batch-num, $project-id)
            }
            <end-time>{current-dateTime()}</end-time>
        </results>
};

(: ============================================================================
   8. MAIN EXECUTION
   ============================================================================ :)

let $_ := local:log("INFO", "=== Starting Index Entry Push for " || $project-id || " ===")

let $annotations-xml-path := "/db/apps/hsg-annotate-data/tei/annotations_" || $project-id || ".xml"

(: Load ALL annotation entries for this project :)
let $all-entries := 
    for $entry in doc($annotations-xml-path)//entry
    return $entry

let $_ := local:log("INFO", "Found " || count($all-entries) || " entries for project " || $project-id)

let $doc-count-by-project := 
    for $doc in doc($annotations-xml-path)//document/doc_number
    let $doc-text := $doc/text()
    let $project := 
        if (contains($doc-text, "#")) then
            substring-before($doc-text, "#")
        else
            $doc-text
    group by $project
    return <project id="{$project}" docs="{count($doc)}" />

let $processing-result := local:process-entries($all-entries, $batch-size, $project-id, $restart-batch)

return
    <push-results>
        <summary>
            <project-id>{$project-id}</project-id>
            <entries-processed>{count($all-entries)}</entries-processed>
        </summary>
        <document-counts>
            {$doc-count-by-project}
        </document-counts>
        {$processing-result}
    </push-results>
