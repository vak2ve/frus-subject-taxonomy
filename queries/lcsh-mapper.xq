xquery version "3.1";

(:~
 : LCSH Mapper - Map annotation-derived subjects to Library of Congress Subject Headings
 :
 : This standalone XQuery 3.1 script replicates the logic of scripts/lcsh_mapper.py.
 : It queries the id.loc.gov suggest2 API for each subject, retrieves the best match,
 : then fetches the broader terms (BT) from the LCSH hierarchy to build a proper
 : hierarchical taxonomy.
 :
 : Outputs:
 :   - config/lcsh_mapping.json  (intermediate mapping data)
 :   - subject-taxonomy-lcsh.xml (final hierarchical taxonomy)
 :
 : Requirements:
 :   - XQuery 3.1 processor with EXPath HTTP Client (http:send-request)
 :   - EXPath File Module (file:write, file:read-text, file:exists)
 :   - JSON parsing support (parse-json, serialize with method "json")
 :
 : Tested with: BaseX, eXist-db (with adjustments to sleep function)
 :
 : Rate Limiting:
 :   LOC servers require polite request rates. This script includes a delay
 :   between API requests. XQuery has no native sleep function, so the delay
 :   mechanism is processor-specific:
 :
 :     - BaseX:   prof:sleep($ms)          — available in prof module
 :     - eXist:   util:wait($ms)           — available in util module
 :     - Saxon:   No built-in sleep; use extension or remove delays
 :
 :   Adjust the $request-delay-ms variable and the local:sleep() function
 :   below to match your processor.
 :
 : Usage:
 :   - Set $step to "all" to run the full pipeline
 :   - Set $step to "map" to only query the suggest2 API
 :   - Set $step to "hierarchy" to only fetch broader term hierarchies
 :   - Set $step to "xml" to only build the taxonomy XML from existing mapping
 :
 : @author  Generated from scripts/lcsh_mapper.py
 :)

(: ============================================================================
   MODULES
   ============================================================================ :)

(: EXPath HTTP Client for API calls :)
declare namespace http = "http://expath.org/ns/http-client";

(: EXPath File Module for filesystem I/O :)
declare namespace file = "http://expath.org/ns/file";

(: BaseX profiling module — used for sleep/delay :)
(: If using eXist-db, replace prof:sleep with util:wait :)
declare namespace prof = "http://basex.org/modules/prof";

(: ============================================================================
   CONFIGURATION
   ============================================================================ :)

(: Step to execute: "map", "hierarchy", "xml", or "all" :)
declare variable $step as xs:string := "all";

(: API endpoints :)
declare variable $suggest-url as xs:string := "https://id.loc.gov/authorities/subjects/suggest2";
declare variable $skos-url-template as xs:string := "https://id.loc.gov/authorities/subjects/{LCCN}.skos.json";

(: File paths — relative to the project root directory :)
(: Adjust $base-dir to the absolute path of your frus-subject-taxonomy checkout :)
declare variable $base-dir as xs:string := file:parent(file:parent(static-base-uri()));
declare variable $mapping-file as xs:string := $base-dir || "config/lcsh_mapping.json";
declare variable $input-taxonomy as xs:string :=
    if (file:exists($base-dir || "subject-taxonomy.xml"))
    then $base-dir || "subject-taxonomy.xml"
    else $base-dir || "subject-taxonomy-lcsh.xml";
declare variable $output-taxonomy as xs:string := $base-dir || "subject-taxonomy-lcsh.xml";

(: Rate limiting: milliseconds between API requests.
   Be polite to LOC servers — 300ms is a reasonable minimum. :)
declare variable $request-delay-ms as xs:integer := 300;

(: Maximum depth for broader-term hierarchy traversal :)
declare variable $max-hierarchy-depth as xs:integer := 5;

(: Save progress every N subjects :)
declare variable $save-interval as xs:integer := 50;

(: SKOS property URIs :)
declare variable $SKOS-BROADER as xs:string := "http://www.w3.org/2004/02/skos/core#broader";
declare variable $SKOS-PREFLABEL as xs:string := "http://www.w3.org/2004/02/skos/core#prefLabel";

(: ============================================================================
   RATE LIMITING / SLEEP
   ============================================================================ :)

(:~
 : Sleep for the configured delay between API requests.
 :
 : IMPORTANT: Adjust this function for your XQuery processor:
 :   - BaseX:   prof:sleep($request-delay-ms)
 :   - eXist:   util:wait($request-delay-ms)
 :   - Saxon:   No native sleep — comment out the body or use an extension
 :
 : If your processor lacks a sleep function, you can remove the call,
 : but be aware that rapid-fire requests may be throttled or blocked by LOC.
 :)
declare function local:sleep() as empty-sequence() {
    (: BaseX sleep — change to util:wait($request-delay-ms) for eXist-db :)
    prof:sleep($request-delay-ms)
};

(: ============================================================================
   TAXONOMY EXTRACTION
   ============================================================================ :)

(:~
 : Extract all subjects from the input taxonomy XML.
 : Returns a sequence of map objects, one per subject.
 :
 : Expected input structure:
 :   <taxonomy>
 :     <subject ref="recXXX" type="topic" count="N" volumes="N" resolved-from="...">
 :       <name>Subject Name</name>
 :       <appearsIn>vol1, vol2</appearsIn>
 :     </subject>
 :     ...or subjects nested inside <category>/<subcategory> elements...
 :   </taxonomy>
 :)
declare function local:extract-subjects($taxonomy-path as xs:string) as map(*)* {
    let $doc := doc($taxonomy-path)
    for $s in $doc//subject
    let $ref := string($s/@ref)
    let $name := string($s/name)
    let $type := ($s/@type/string(), "topic")[1]
    let $count := ($s/@count/string(), "0")[1]
    let $volumes := ($s/@volumes/string(), "")[1]
    let $appears-in := string($s/appearsIn)
    let $resolved-from := ($s/@resolved-from/string(), "")[1]
    where $ref ne "" and $name ne ""
    return map {
        "ref": $ref,
        "name": $name,
        "type": $type,
        "count": $count,
        "volumes": $volumes,
        "appears_in": $appears-in,
        "resolved_from": $resolved-from
    }
};

(: ============================================================================
   LOC SUGGEST2 API
   ============================================================================ :)

(:~
 : Query the LOC suggest2 API for a subject term.
 : Returns a sequence of maps with "label" and "uri" keys.
 :
 : The suggest2 API returns JSON:
 :   {"hits": [{"uri": "...", "aLabel": "...", ...}, ...]}
 :)
declare function local:query-suggest($term as xs:string) as map(*)* {
    let $encoded-term := encode-for-uri($term)
    let $url := $suggest-url || "?q=" || $encoded-term
    let $request :=
        <http:request method="GET" href="{$url}" timeout="15">
            <http:header name="Accept" value="application/json"/>
        </http:request>
    return
        try {
            let $response := http:send-request($request)
            let $status := $response[1]/@status/string()
            return
                if ($status = "200") then
                    let $body := $response[2]
                    (: The response body may be xs:string or xs:base64Binary depending on processor :)
                    let $json-string :=
                        if ($body instance of xs:string) then $body
                        else if ($body instance of xs:base64Binary) then bin:decode-string($body, "UTF-8")
                        else string($body)
                    let $data := parse-json($json-string)
                    let $hits := $data?hits
                    return
                        if (exists($hits)) then
                            let $hit-array :=
                                if ($hits instance of array(*)) then $hits
                                else array { $hits }
                            for $i in 1 to array:size($hit-array)
                            let $hit := $hit-array($i)
                            let $label := $hit?aLabel
                            let $uri := $hit?uri
                            where exists($label) and string($label) ne ""
                                  and exists($uri) and string($uri) ne ""
                            return map {
                                "label": string($label),
                                "uri": string($uri)
                            }
                        else ()
                else ()
        } catch * {
            (: Log error and return empty sequence :)
            trace((), "    API error for '" || $term || "': " || $err:description),
            ()
        }
};

(: ============================================================================
   SKOS BROADER TERM FETCHING
   ============================================================================ :)

(:~
 : Fetch the SKOS JSON-LD entry for a given LCSH URI.
 : Returns a map with "label" (prefLabel) and "broader_uris" (sequence of URI strings).
 :)
declare function local:fetch-skos($uri as xs:string) as map(*) {
    let $lccn := tokenize(replace($uri, "/+$", ""), "/")[last()]
    let $skos-url := replace($skos-url-template, "\{LCCN\}", $lccn)
    return local:fetch-skos-with-retries($skos-url, $uri, 3, 1)
};

(:~
 : Fetch SKOS entry with retry logic.
 :)
declare function local:fetch-skos-with-retries(
    $skos-url as xs:string,
    $uri as xs:string,
    $max-retries as xs:integer,
    $attempt as xs:integer
) as map(*) {
    try {
        let $request :=
            <http:request method="GET" href="{$skos-url}" timeout="30">
                <http:header name="Accept" value="application/json"/>
            </http:request>
        let $response := http:send-request($request)
        let $status := $response[1]/@status/string()
        return
            if ($status = "200") then
                let $body := $response[2]
                let $json-string :=
                    if ($body instance of xs:string) then $body
                    else if ($body instance of xs:base64Binary) then bin:decode-string($body, "UTF-8")
                    else string($body)
                let $data := parse-json($json-string)
                (: SKOS JSON-LD is an array of objects :)
                let $items :=
                    if ($data instance of array(*)) then $data
                    else array { $data }
                (: Find the entry matching our URI :)
                let $matching-item :=
                    (for $i in 1 to array:size($items)
                     let $item := $items($i)
                     where $item instance of map(*)
                           and replace(string($item?("@id")), "/+$", "") = replace($uri, "/+$", "")
                     return $item
                    )[1]
                return
                    if (exists($matching-item)) then
                        let $label := local:extract-pref-label($matching-item)
                        let $broader-uris := local:extract-broader-uris($matching-item)
                        return map {
                            "label": $label,
                            "broader_uris": $broader-uris
                        }
                    else
                        map { "label": (), "broader_uris": () }
            else
                map { "label": (), "broader_uris": () }
    } catch * {
        if ($attempt lt $max-retries) then (
            let $wait := $attempt * 2000
            return (
                trace((), "    Retry " || $attempt || "/" || $max-retries || " for " || $uri || ": " || $err:description),
                (: Wait before retry — adjust for your processor :)
                prof:sleep($wait),
                local:fetch-skos-with-retries($skos-url, $uri, $max-retries, $attempt + 1)
            )
        ) else (
            trace((), "    SKOS error for " || $uri || ": " || $err:description),
            map { "label": (), "broader_uris": () }
        )
    }
};

(:~
 : Extract prefLabel from a SKOS JSON-LD item.
 :)
declare function local:extract-pref-label($item as map(*)) as xs:string? {
    let $pref-labels := $item?($SKOS-PREFLABEL)
    return
        if (empty($pref-labels)) then ()
        else if ($pref-labels instance of array(*)) then
            let $first :=
                (for $i in 1 to array:size($pref-labels)
                 let $pl := $pref-labels($i)
                 let $val :=
                     if ($pl instance of map(*)) then string($pl?("@value"))
                     else string($pl)
                 where $val ne ""
                 return $val
                )[1]
            return $first
        else if ($pref-labels instance of map(*)) then
            string($pref-labels?("@value"))
        else
            string($pref-labels)
};

(:~
 : Extract broader term URIs from a SKOS JSON-LD item.
 : Only returns URIs containing "authorities/subjects".
 :)
declare function local:extract-broader-uris($item as map(*)) as xs:string* {
    let $bt-raw := $item?($SKOS-BROADER)
    return
        if (empty($bt-raw)) then ()
        else
            let $bt-list :=
                if ($bt-raw instance of array(*)) then
                    for $i in 1 to array:size($bt-raw)
                    return $bt-raw($i)
                else
                    $bt-raw
            for $bt in $bt-list
            let $bt-uri :=
                if ($bt instance of map(*)) then string($bt?("@id"))
                else string($bt)
            where contains($bt-uri, "authorities/subjects")
            return $bt-uri
};

(: ============================================================================
   BROADER-TERM HIERARCHY BUILDING
   ============================================================================ :)

(:~
 : Build full broader-term hierarchy for a given URI, up to $max-hierarchy-depth levels.
 : Returns an array of maps: [{"label": "...", "uri": "..."}, ...] from narrowest to broadest.
 :
 : Uses a cache (passed as a map) to avoid redundant API calls.
 : Returns a map with "result" (the hierarchy array) and "cache" (updated cache).
 :)
declare function local:build-hierarchy(
    $uri as xs:string,
    $cache as map(*),
    $depth as xs:integer
) as map(*) {
    if ($depth ge $max-hierarchy-depth) then
        map { "result": array {}, "cache": $cache }
    else if (map:contains($cache, $uri)) then
        map { "result": $cache($uri), "cache": $cache }
    else
        let $skos := local:fetch-skos($uri)
        let $_ := local:sleep()
        let $broader-uris := $skos?broader_uris
        return
            if (empty($broader-uris)) then
                let $empty-result := array {}
                let $updated-cache := map:put($cache, $uri, $empty-result)
                return map { "result": $empty-result, "cache": $updated-cache }
            else
                (: Process broader URIs and find the longest chain :)
                let $fold-result :=
                    fold-left($broader-uris, map { "best": array {}, "cache": $cache },
                        function($acc, $bt-uri) {
                            (: Fetch label for this broader term :)
                            let $current-cache := $acc?cache
                            let $bt-skos := local:fetch-skos($bt-uri)
                            let $_ := local:sleep()
                            let $bt-label :=
                                if (exists($bt-skos?label) and string($bt-skos?label) ne "") then
                                    string($bt-skos?label)
                                else
                                    tokenize($bt-uri, "/")[last()]
                            (: Recursively get ancestors :)
                            let $ancestors-result := local:build-hierarchy($bt-uri, $current-cache, $depth + 1)
                            let $ancestors := $ancestors-result?result
                            let $new-cache := $ancestors-result?cache
                            (: Build chain: this broader term + its ancestors :)
                            let $chain := array:join((
                                array { map { "label": $bt-label, "uri": $bt-uri } },
                                $ancestors
                            ))
                            (: Keep the longest chain :)
                            let $best :=
                                if (array:size($chain) gt array:size($acc?best)) then $chain
                                else $acc?best
                            return map { "best": $best, "cache": $new-cache }
                        }
                    )
                let $result := $fold-result?best
                let $final-cache := map:put($fold-result?cache, $uri, $result)
                return map { "result": $result, "cache": $final-cache }
};

(: ============================================================================
   BEST MATCH SELECTION
   ============================================================================ :)

(:~
 : Find the best match from suggest2 results for a given subject name.
 : Prefers exact match, then match ignoring parenthetical qualifiers, then first result.
 :)
declare function local:find-best-match($name as xs:string, $results as map(*)*) as map(*)? {
    let $lower-name := lower-case($name)
    (: Try exact match (case-insensitive) :)
    let $exact :=
        (for $r in $results
         where lower-case($r?label) eq $lower-name
         return $r)[1]
    return
        if (exists($exact)) then $exact
        else
            (: Try match ignoring parenthetical qualifiers :)
            let $close :=
                (for $r in $results
                 let $clean := normalize-space(replace($r?label, "\(.*\)", ""))
                 where lower-case($clean) eq $lower-name
                 return $r)[1]
            return
                if (exists($close)) then $close
                else
                    (: Fall back to first result :)
                    $results[1]
};

(: ============================================================================
   JSON MAPPING I/O
   ============================================================================ :)

(:~
 : Load existing mapping from JSON file, or return an empty map.
 :)
declare function local:load-mapping() as map(*) {
    if (file:exists($mapping-file)) then
        let $json-text := file:read-text($mapping-file)
        return
            if (string-length(normalize-space($json-text)) gt 0) then
                parse-json($json-text)
            else
                map {}
    else
        map {}
};

(:~
 : Save mapping to JSON file.
 :)
declare function local:save-mapping($mapping as map(*)) as empty-sequence() {
    let $json-output := serialize($mapping, map { "method": "json", "indent": true() })
    return file:write-text($mapping-file, $json-output)
};

(:~
 : Convert a broader-terms array from the hierarchy builder to a JSON-serializable array.
 :)
declare function local:broader-terms-to-array($terms as array(*)) as array(*) {
    array {
        for $i in 1 to array:size($terms)
        let $t := $terms($i)
        return map {
            "label": string($t?label),
            "uri": string($t?uri)
        }
    }
};

(: ============================================================================
   STEP 1: MAP SUBJECTS TO LCSH
   ============================================================================ :)

(:~
 : Map all subjects to LCSH via the suggest2 API.
 : Supports resume: skips subjects already present in the mapping file.
 : Saves progress every $save-interval subjects.
 :)
declare function local:map-subjects($subjects as map(*)*) as map(*) {
    let $existing := local:load-mapping()
    let $existing-count := map:size($existing)
    let $_ :=
        if ($existing-count gt 0) then
            trace((), "Loaded " || $existing-count || " existing mappings from " || $mapping-file)
        else ()
    let $total := count($subjects)
    (: Process subjects sequentially with fold-left to maintain mapping state :)
    let $result :=
        fold-left(
            for-each(1 to $total, function($i) { map { "index": $i, "subject": $subjects[$i] } }),
            map { "mapping": $existing, "matched": 0, "unmatched": 0, "processed": 0 },
            function($acc, $item) {
                let $i := $item?index
                let $subj := $item?subject
                let $ref := $subj?ref
                let $name := $subj?name
                let $mapping := $acc?mapping
                return
                    (: Skip if already mapped (resume support) :)
                    if (map:contains($mapping, $ref)) then
                        let $has-lcsh := exists($mapping($ref)?lcsh_uri) and string($mapping($ref)?lcsh_uri) ne ""
                        return map {
                            "mapping": $mapping,
                            "matched": $acc?matched + (if ($has-lcsh) then 1 else 0),
                            "unmatched": $acc?unmatched + (if ($has-lcsh) then 0 else 1),
                            "processed": $acc?processed
                        }
                    else
                        let $_ := trace((), "[" || $i || "/" || $total || "] Querying: " || $name)
                        let $results := local:query-suggest($name)
                        let $_ := local:sleep()
                        let $new-entry :=
                            if (exists($results)) then
                                let $best := local:find-best-match($name, $results)
                                let $suggestions := array {
                                    for $r at $pos in $results
                                    where $pos le 5
                                    return $r?label
                                }
                                return map {
                                    "name": $name,
                                    "type": $subj?type,
                                    "count": xs:integer($subj?count),
                                    "volumes": $subj?volumes,
                                    "appears_in": $subj?appears_in,
                                    "resolved_from": $subj?resolved_from,
                                    "lcsh_label": $best?label,
                                    "lcsh_uri": $best?uri,
                                    "exact_match": lower-case($best?label) eq lower-case($name),
                                    "all_suggestions": $suggestions
                                }
                            else
                                map {
                                    "name": $name,
                                    "type": $subj?type,
                                    "count": xs:integer($subj?count),
                                    "volumes": $subj?volumes,
                                    "appears_in": $subj?appears_in,
                                    "resolved_from": $subj?resolved_from,
                                    "lcsh_label": (),
                                    "lcsh_uri": (),
                                    "exact_match": false(),
                                    "all_suggestions": array {}
                                }
                        let $updated-mapping := map:put($mapping, $ref, $new-entry)
                        let $processed := $acc?processed + 1
                        let $has-match := exists($new-entry?lcsh_uri) and string($new-entry?lcsh_uri) ne ""
                        (: Save progress periodically :)
                        let $_ :=
                            if ($processed mod $save-interval eq 0) then (
                                local:save-mapping($updated-mapping),
                                trace((), "  -- Progress saved: " || $i || "/" || $total || " --")
                            ) else ()
                        return map {
                            "mapping": $updated-mapping,
                            "matched": $acc?matched + (if ($has-match) then 1 else 0),
                            "unmatched": $acc?unmatched + (if ($has-match) then 0 else 1),
                            "processed": $processed
                        }
            }
        )
    (: Final save :)
    let $_ := local:save-mapping($result?mapping)
    let $_ := trace((), "Mapping complete: " || $result?matched || " matched, "
                        || $result?unmatched || " unmatched out of " || $total)
    return $result?mapping
};

(: ============================================================================
   STEP 2: FETCH BROADER-TERM HIERARCHIES
   ============================================================================ :)

(:~
 : Fetch broader-term hierarchies for all matched subjects.
 : Supports resume: skips subjects that already have broader_terms populated.
 :)
declare function local:fetch-hierarchies($mapping as map(*)) as map(*) {
    let $refs := map:keys($mapping)
    let $lcsh-refs :=
        for $ref in $refs
        let $data := $mapping($ref)
        where exists($data?lcsh_uri) and string($data?lcsh_uri) ne ""
        return $ref
    let $total := count($lcsh-refs)
    (: Build initial cache from already-processed entries :)
    let $initial-cache :=
        fold-left($lcsh-refs, map {},
            function($cache, $ref) {
                let $data := $mapping($ref)
                let $bt := $data?broader_terms
                return
                    if (exists($bt) and $bt instance of array(*) and array:size($bt) gt 0) then
                        map:put($cache, string($data?lcsh_uri), $bt)
                    else
                        $cache
            }
        )
    (: Process each subject :)
    let $result :=
        fold-left(
            for-each(1 to count($lcsh-refs), function($i) { map { "index": $i, "ref": $lcsh-refs[$i] } }),
            map { "mapping": $mapping, "cache": $initial-cache, "done": 0 },
            function($acc, $item) {
                let $ref := $item?ref
                let $data := $acc?mapping($ref)
                let $existing-bt := $data?broader_terms
                return
                    (: Skip if already has broader terms :)
                    if (exists($existing-bt) and $existing-bt instance of array(*) and array:size($existing-bt) gt 0) then
                        map:put($acc, "done", $acc?done + 1)
                    else
                        let $done := $acc?done + 1
                        let $_ := trace((), "[" || $done || "/" || $total || "] Hierarchy: " || $data?name || " (" || $data?lcsh_uri || ")")
                        let $hierarchy-result := local:build-hierarchy(string($data?lcsh_uri), $acc?cache, 0)
                        let $broader := $hierarchy-result?result
                        let $new-cache := $hierarchy-result?cache
                        let $updated-data := map:put($data, "broader_terms", $broader)
                        let $updated-mapping := map:put($acc?mapping, $ref, $updated-data)
                        let $_ :=
                            if (array:size($broader) gt 0) then
                                let $chain := string-join(
                                    for $i in 1 to array:size($broader)
                                    return $broader($i)?label,
                                    " -> "
                                )
                                return trace((), "    BT: " || $chain)
                            else
                                trace((), "    (no broader terms)")
                        (: Save progress periodically :)
                        let $_ :=
                            if ($done mod $save-interval eq 0) then (
                                local:save-mapping($updated-mapping),
                                trace((), "  -- Progress saved: " || $done || "/" || $total || " --")
                            ) else ()
                        return map {
                            "mapping": $updated-mapping,
                            "cache": $new-cache,
                            "done": $done
                        }
            }
        )
    (: Final save :)
    let $_ := local:save-mapping($result?mapping)
    return $result?mapping
};

(: ============================================================================
   STEP 3: BUILD TAXONOMY XML
   ============================================================================ :)

(:~
 : Build hierarchical taxonomy XML based on LCSH broader terms.
 : Uses the broadest term as category and next level as subcategory.
 :)
declare function local:build-taxonomy-xml($mapping as map(*)) as element(taxonomy) {
    let $refs := map:keys($mapping)

    (: Categorize subjects by their broader term hierarchy :)
    let $categorized :=
        for $ref in $refs
        let $data := $mapping($ref)
        let $broader := $data?broader_terms
        let $bt-size :=
            if (exists($broader) and $broader instance of array(*)) then array:size($broader)
            else 0
        return
            if ($bt-size ge 2) then
                map {
                    "category": $broader($bt-size)?label,
                    "category-uri": $broader($bt-size)?uri,
                    "subcategory": $broader($bt-size - 1)?label,
                    "subcategory-uri": $broader($bt-size - 1)?uri,
                    "ref": $ref,
                    "data": $data
                }
            else if ($bt-size eq 1) then
                map {
                    "category": $broader(1)?label,
                    "category-uri": $broader(1)?uri,
                    "subcategory": "General",
                    "subcategory-uri": "",
                    "ref": $ref,
                    "data": $data
                }
            else
                map {
                    "category": "__uncategorized__",
                    "category-uri": "",
                    "subcategory": "",
                    "subcategory-uri": "",
                    "ref": $ref,
                    "data": $data
                }

    (: Group by category :)
    let $category-names := distinct-values(
        for $c in $categorized
        where $c?category ne "__uncategorized__"
        return $c?category
    )

    (: Identify small categories (< 3 subjects) for merging :)
    let $small-cats :=
        for $cat in $category-names
        let $cat-subjects := $categorized[?category eq $cat]
        where count($cat-subjects) lt 3
        return $cat

    let $regular-cats := $category-names[not(. = $small-cats)]

    (: Compute total annotations :)
    let $total-annotations := sum(
        for $ref in $refs
        return xs:integer(($mapping($ref)?count, 0)[1])
    )
    let $lcsh-matched := count(
        for $ref in $refs
        where exists($mapping($ref)?lcsh_uri) and string($mapping($ref)?lcsh_uri) ne ""
        return $ref
    )
    let $uncategorized-items := $categorized[?category eq "__uncategorized__"]

    return
        <taxonomy
            source="hsg-annotate-data"
            authority="Library of Congress Subject Headings (LCSH)"
            authority-uri="https://id.loc.gov/authorities/subjects"
            generated="{current-date()}"
            total-subjects="{count($refs)}"
            total-annotations="{$total-annotations}"
            lcsh-matched="{$lcsh-matched}"
            lcsh-unmatched="{count($refs) - $lcsh-matched}"
            categories="{count($regular-cats) + (if (exists($small-cats)) then 1 else 0)}"
            uncategorized="{count($uncategorized-items)}"
        >
        {
            (: Regular categories, sorted by total annotation count descending :)
            let $sorted-cats :=
                for $cat in $regular-cats
                let $cat-subjects := $categorized[?category eq $cat]
                let $cat-total := sum(
                    for $c in $cat-subjects
                    return xs:integer(($c?data?count, 0)[1])
                )
                order by $cat-total descending
                return map { "name": $cat, "subjects": $cat-subjects, "total": $cat-total }
            for $cat-info in $sorted-cats
            let $cat-subjects := $cat-info?subjects
            let $cat-uri := ($cat-subjects[1]?category-uri, "")[1]
            let $subcategory-names := distinct-values(
                for $c in $cat-subjects return $c?subcategory
            )
            return
                <category
                    label="{$cat-info?name}"
                    total-annotations="{$cat-info?total}"
                    total-subjects="{count($cat-subjects)}"
                >
                {
                    if ($cat-uri ne "") then attribute lcsh-uri { $cat-uri } else ()
                }
                {
                    (: Subcategories sorted by annotation count :)
                    let $sorted-subs :=
                        for $sub in $subcategory-names
                        let $sub-items := $cat-subjects[?subcategory eq $sub]
                        let $sub-total := sum(
                            for $s in $sub-items
                            return xs:integer(($s?data?count, 0)[1])
                        )
                        order by $sub-total descending
                        return map { "name": $sub, "items": $sub-items, "total": $sub-total }
                    for $sub-info in $sorted-subs
                    let $sub-items := $sub-info?items
                    let $sub-uri := ($sub-items[1]?subcategory-uri, "")[1]
                    return
                        <subcategory
                            label="{$sub-info?name}"
                            total-annotations="{$sub-info?total}"
                            total-subjects="{count($sub-items)}"
                        >
                        {
                            if ($sub-uri ne "") then attribute lcsh-uri { $sub-uri } else ()
                        }
                        {
                            (: Subjects sorted by count descending :)
                            for $item in $sub-items
                            let $data := $item?data
                            order by xs:integer(($data?count, 0)[1]) descending
                            return local:build-subject-element($item?ref, $data)
                        }
                        </subcategory>
                }
                </category>
        }
        {
            (: "Other topics" category for small categories merged together :)
            if (exists($small-cats)) then
                let $merged-subjects := $categorized[?category = $small-cats]
                let $merged-total := sum(
                    for $m in $merged-subjects
                    return xs:integer(($m?data?count, 0)[1])
                )
                return
                    <category
                        label="Other topics"
                        total-annotations="{$merged-total}"
                        total-subjects="{count($merged-subjects)}"
                    >
                    {
                        (: Group by original category as subcategory :)
                        let $orig-cats := distinct-values(
                            for $m in $merged-subjects return $m?category
                        )
                        for $orig in $orig-cats
                        let $orig-items := $merged-subjects[?category eq $orig]
                        let $orig-total := sum(
                            for $o in $orig-items
                            return xs:integer(($o?data?count, 0)[1])
                        )
                        let $sub-name :=
                            if ($orig-items[1]?subcategory ne "General") then
                                $orig || " -- " || $orig-items[1]?subcategory
                            else $orig
                        order by $orig-total descending
                        return
                            <subcategory
                                label="{$sub-name}"
                                total-annotations="{$orig-total}"
                                total-subjects="{count($orig-items)}"
                            >
                            {
                                for $item in $orig-items
                                let $data := $item?data
                                order by xs:integer(($data?count, 0)[1]) descending
                                return local:build-subject-element($item?ref, $data)
                            }
                            </subcategory>
                    }
                    </category>
            else ()
        }
        {
            (: Uncategorized subjects :)
            if (exists($uncategorized-items)) then
                <uncategorized
                    total-annotations="{sum(
                        for $u in $uncategorized-items
                        return xs:integer(($u?data?count, 0)[1])
                    )}"
                    total-subjects="{count($uncategorized-items)}"
                >
                {
                    for $item in $uncategorized-items
                    let $data := $item?data
                    order by xs:integer(($data?count, 0)[1]) descending
                    return local:build-subject-element($item?ref, $data)
                }
                </uncategorized>
            else ()
        }
        </taxonomy>
};

(:~
 : Build a <subject> element for the output taxonomy XML.
 :)
declare function local:build-subject-element($ref as xs:string, $data as map(*)) as element(subject) {
    <subject
        ref="{$ref}"
        type="{($data?type, 'topic')[1]}"
        count="{($data?count, 0)[1]}"
        volumes="{($data?volumes, '')[1]}"
        resolved-from="{($data?resolved_from, '')[1]}"
    >
    {
        if (exists($data?lcsh_uri) and string($data?lcsh_uri) ne "") then (
            attribute lcsh-uri { $data?lcsh_uri },
            attribute lcsh-match {
                if ($data?exact_match = true()) then "exact" else "close"
            }
        ) else ()
    }
        <name>{ string(($data?name, "")[1]) }</name>
    {
        if (exists($data?lcsh_label) and string($data?lcsh_label) ne ""
            and string($data?lcsh_label) ne string($data?name)) then
            <lcsh-authorized-form>{ string($data?lcsh_label) }</lcsh-authorized-form>
        else ()
    }
    {
        let $bt := $data?broader_terms
        return
            if (exists($bt) and $bt instance of array(*) and array:size($bt) gt 0) then
                <broader-terms>
                {
                    for $i in 1 to array:size($bt)
                    let $term := $bt($i)
                    return <term uri="{$term?uri}">{ string($term?label) }</term>
                }
                </broader-terms>
            else ()
    }
    {
        if (exists($data?appears_in) and string($data?appears_in) ne "") then
            <appearsIn>{ string($data?appears_in) }</appearsIn>
        else ()
    }
    </subject>
};

(: ============================================================================
   MAIN EXECUTION
   ============================================================================ :)

let $_ := trace((), "LCSH Mapper - XQuery 3.1")
let $_ := trace((), "Input taxonomy: " || $input-taxonomy)
let $_ := trace((), "Mapping file:   " || $mapping-file)
let $_ := trace((), "Output file:    " || $output-taxonomy)
let $_ := trace((), "Step: " || $step)

(: STEP 1: Map subjects to LCSH :)
let $mapping-after-step1 :=
    if ($step = ("map", "all")) then
        let $_ := trace((), string-join(for $i in 1 to 60 return "=", ""))
        let $_ := trace((), "STEP 1: Map subjects to LCSH")
        let $_ := trace((), string-join(for $i in 1 to 60 return "=", ""))
        let $subjects := local:extract-subjects($input-taxonomy)
        let $_ := trace((), "Extracted " || count($subjects) || " subjects from " || $input-taxonomy)
        return local:map-subjects($subjects)
    else ()

(: STEP 2: Fetch broader-term hierarchies :)
let $mapping-after-step2 :=
    if ($step = ("hierarchy", "all")) then
        let $_ := trace((), string-join(for $i in 1 to 60 return "=", ""))
        let $_ := trace((), "STEP 2: Fetch LCSH broader term hierarchies")
        let $_ := trace((), string-join(for $i in 1 to 60 return "=", ""))
        let $mapping :=
            if (exists($mapping-after-step1)) then $mapping-after-step1
            else if (file:exists($mapping-file)) then
                parse-json(file:read-text($mapping-file))
            else
                error(xs:QName("local:error"), $mapping-file || " not found. Run 'map' step first.")
        return local:fetch-hierarchies($mapping)
    else ()

(: STEP 3: Build taxonomy XML :)
let $output :=
    if ($step = ("xml", "all")) then
        let $_ := trace((), string-join(for $i in 1 to 60 return "=", ""))
        let $_ := trace((), "STEP 3: Build taxonomy XML")
        let $_ := trace((), string-join(for $i in 1 to 60 return "=", ""))
        let $mapping :=
            if (exists($mapping-after-step2)) then $mapping-after-step2
            else if (exists($mapping-after-step1)) then $mapping-after-step1
            else if (file:exists($mapping-file)) then
                parse-json(file:read-text($mapping-file))
            else
                error(xs:QName("local:error"), $mapping-file || " not found. Run 'map' or 'hierarchy' step first.")
        let $taxonomy := local:build-taxonomy-xml($mapping)
        let $serialized := serialize($taxonomy, map {
            "method": "xml",
            "indent": true(),
            "omit-xml-declaration": false()
        })
        let $_ := file:write-text($output-taxonomy, $serialized)
        let $_ := trace((), "Wrote taxonomy to " || $output-taxonomy)
        return $taxonomy
    else ()

return
    <result>
        <step>{ $step }</step>
        <input>{ $input-taxonomy }</input>
        <mapping-file>{ $mapping-file }</mapping-file>
        <output-taxonomy>{ $output-taxonomy }</output-taxonomy>
        {
            if ($step = ("map", "all") and exists($mapping-after-step1)) then
                <mapping-entries>{ map:size($mapping-after-step1) }</mapping-entries>
            else ()
        }
        <status>complete</status>
    </result>
