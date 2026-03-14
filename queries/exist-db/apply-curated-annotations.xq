xquery version "3.1";

(:~
 : Apply Curated Subject Annotations to FRUS TEI Documents (eXist-db version)
 :
 : This is the eXist-db adapted version of apply-curated-annotations.xq.
 : It uses eXist-db native modules (xmldb, util) instead of the EXPath file: module
 : for filesystem I/O, and reads/writes from eXist-db collections.
 :
 : It reads string_match_results and annotation_rejections files,
 : computes per-document annotation budgets, and applies accepted
 : annotations as tei:rs elements via recursive identity transform.
 :
 : Reads from eXist-db collections:
 :   - {app-root}/data/string_match_results_{volume-id}.json
 :   - {app-root}/config/annotation_rejections_{volume-id}.json (optional)
 :   - {app-root}/data/variant_groups.json (optional)
 :
 : For each document in {app-root}/data/documents/{volume-id}/:
 :   - Builds per-document vocabulary of accepted matches
 :   - Finds match positions using longest-match-first strategy
 :   - Inserts <rs corresp="recXXX" type="topic"> elements
 :   - Removes (unwraps) existing <rs> elements that were rejected
 :   - Stores the modified document back via xmldb:store
 :
 : Usage in eXist-db:
 :   Run via eXide or the REST API. Set $volume-id to the desired volume.
 :)

declare namespace tei = "http://www.tei-c.org/ns/1.0";
declare namespace xml = "http://www.w3.org/XML/1998/namespace";
declare namespace util = "http://exist-db.org/xquery/util";
declare namespace xmldb = "http://exist-db.org/xquery/xmldb";
declare namespace output = "http://www.w3.org/2010/xslt-xquery-serialization";

(: ============================================================================
   CONFIGURATION
   ============================================================================ :)

(: Root collection for the hsg-annotate-data app in eXist-db :)
declare variable $app-root as xs:string := "/db/apps/hsg-annotate-data";

(: Volume to process - override via external variable binding :)
declare variable $volume-id as xs:string external := "frus1981-88v41";

(: ============================================================================
   PATH RESOLUTION (eXist-db collections)
   ============================================================================ :)

(: String match results file :)
declare variable $results-path as xs:string :=
    $app-root || "/data/string_match_results_" || $volume-id || ".json";

(: Annotation rejections file :)
declare variable $rejections-path as xs:string :=
    $app-root || "/config/annotation_rejections_" || $volume-id || ".json";

(: Variant groups file :)
declare variable $variant-groups-path as xs:string :=
    $app-root || "/data/variant_groups.json";

(: Documents collection :)
declare variable $docs-collection as xs:string :=
    $app-root || "/data/documents/" || $volume-id;

(: ============================================================================
   ANCESTOR ELEMENT NAMES TO SKIP
   ============================================================================ :)

declare variable $skip-local-names := ("rs", "persName", "orgName", "placeName", "gloss");

(: ============================================================================
   JSON DATA LOADING
   ============================================================================ :)

(:~
 : Load the string_match_results JSON file from eXist-db.
 :)
declare function local:load-results() as map(*) {
    if (not(doc-available($results-path)))
    then error(
        xs:QName("apply:FILE_NOT_FOUND"),
        "String match results not found: " || $results-path ||
        ". Run annotate_documents.py first."
    )
    else
        let $data := json-doc($results-path)
        let $total := $data?metadata?total_matches
        let $docs := map:size($data?by_document)
        let $_ := util:log("INFO", "apply-curated: Loaded " || $results-path || ": " ||
            $total || " matches across " || $docs || " documents")
        return $data
};

(:~
 : Load annotation rejections from eXist-db.
 :)
declare function local:load-rejections() as map(xs:string, xs:boolean) {
    if (not(doc-available($rejections-path)))
    then (
        util:log("INFO", "apply-curated: No rejections file found - accepting all matches"),
        map {}
    )
    else
        let $data := json-doc($rejections-path)
        let $rejections-array := $data?rejections
        let $rejection-map := map:merge(
            for $i in 1 to array:size($rejections-array)
            let $r := $rejections-array($i)
            let $key := $r?key
            where $key
            return map:entry($key, true())
        )
        let $_ := util:log("INFO", "apply-curated: Loaded " || map:size($rejection-map) || " rejections")
        return $rejection-map
};

(:~
 : Load variant_groups.json from eXist-db.
 :)
declare function local:load-variant-groups() as map(xs:string, xs:string) {
    if (not(doc-available($variant-groups-path)))
    then map {}
    else
        let $data := json-doc($variant-groups-path)
        let $ref-to-canonical := $data?ref_to_canonical
        return
            if (empty($ref-to-canonical))
            then map {}
            else
                let $_ := util:log("INFO", "apply-curated: Loaded variant_groups: " ||
                    map:size($ref-to-canonical) || " ref mappings")
                return $ref-to-canonical
};

(: ============================================================================
   PER-DOCUMENT BUDGET COMPUTATION
   ============================================================================ :)

declare function local:budget-key($term-lower as xs:string, $ref as xs:string) as xs:string {
    $term-lower || "&#9;" || $ref
};

declare function local:build-document-budgets(
    $results-data as map(*),
    $rejections as map(xs:string, xs:boolean)
) as map(xs:string, map(*)) {
    let $by-doc := $results-data?by_document
    let $doc-ids := map:keys($by-doc)
    return map:merge(
        for $doc-id in $doc-ids
        let $doc-data := $by-doc($doc-id)
        let $matches-array := $doc-data?matches
        let $doc-budget := fold-left(
            (1 to array:size($matches-array)),
            map {},
            function($acc, $i) {
                let $match := $matches-array($i)
                let $ref := ($match?canonical_ref, $match?ref)[1]
                let $term := ($match?matched_text, $match?term)[1]
                let $position := $match?position
                let $rs-type := ($match?type, "topic")[1]
                let $key := $doc-id || ":" || $ref || ":" || $position
                return
                    if (map:contains($rejections, $key))
                    then $acc
                    else
                        let $bk := local:budget-key(lower-case($term), $ref)
                        let $existing := ($acc($bk), map { "allowed": 0, "type": $rs-type })[1]
                        return map:put($acc, $bk, map {
                            "allowed": $existing?allowed + 1,
                            "type": $rs-type
                        })
            }
        )
        where map:size($doc-budget) > 0
        return map:entry($doc-id, $doc-budget)
    )
};

declare function local:build-rejected-refs(
    $rejections as map(xs:string, xs:boolean)
) as map(xs:string, map(xs:string, xs:boolean)) {
    let $keys := map:keys($rejections)
    return map:merge(
        for $key in $keys
        let $parts := tokenize($key, ":")
        where count($parts) >= 2
        let $doc-id := $parts[1]
        let $ref := $parts[2]
        group by $doc-id
        return map:entry($doc-id, map:merge(
            for $r in $ref
            return map:entry($r, true())
        ))
    )
};

(: ============================================================================
   VOCABULARY CONSTRUCTION
   ============================================================================ :)

declare function local:build-vocab(
    $doc-budget as map(*),
    $ref-to-canonical as map(xs:string, xs:string)
) as map(xs:string, map(xs:string, xs:string)) {
    let $budget-keys := map:keys($doc-budget)
    return map:merge(
        for $bk in $budget-keys
        let $info := $doc-budget($bk)
        where $info?allowed > 0
        let $parts := tokenize($bk, "&#9;")
        let $term-lower := $parts[1]
        let $ref := $parts[2]
        let $canonical := ($ref-to-canonical($ref), $ref)[1]
        let $rs-type := ($info?type, "topic")[1]
        return map:entry($term-lower, map { "ref": $canonical, "type": $rs-type })
    )
};

(: ============================================================================
   TEXT NODE MATCHING
   ============================================================================ :)

declare function local:is-alnum($char as xs:string) as xs:boolean {
    matches($char, "[A-Za-z0-9]")
};

declare function local:find-matches(
    $text as xs:string,
    $sorted-terms as xs:string*,
    $vocab as map(*),
    $budget as map(*)
) as map(*) {
    let $text-lower := lower-case($text)
    let $text-len := string-length($text)
    return fold-left(
        $sorted-terms,
        map { "budget": $budget, "matches": (), "claimed": map {} },
        function($state, $term) {
            let $term-len := string-length($term)
            return
                if ($term-len > $text-len)
                then $state
                else
                    let $ref := $vocab($term)?ref
                    let $bk := local:budget-key($term, $ref)
                    let $current-budget := $state?budget
                    let $remaining := ($current-budget($bk)?allowed, 0)[1]
                    return
                        if ($remaining <= 0)
                        then $state
                        else
                            local:scan-for-term(
                                $text, $text-lower, $text-len,
                                $term, $term-len,
                                $ref, $bk,
                                1,
                                $state
                            )
        }
    )
};

declare function local:scan-for-term(
    $text as xs:string,
    $text-lower as xs:string,
    $text-len as xs:integer,
    $term as xs:string,
    $term-len as xs:integer,
    $ref as xs:string,
    $bk as xs:string,
    $pos as xs:integer,
    $state as map(*)
) as map(*) {
    let $remaining := ($state?budget($bk)?allowed, 0)[1]
    return
        if ($remaining <= 0 or $pos + $term-len - 1 > $text-len)
        then $state
        else
            let $search-area := substring($text-lower, $pos)
            let $idx-in-area := (
                if (contains($search-area, $term))
                then string-length(substring-before($search-area, $term)) + 1
                else 0
            )
            return
                if ($idx-in-area = 0)
                then $state
                else
                    let $match-start := $pos + $idx-in-area - 1
                    let $match-end := $match-start + $term-len - 1
                    let $claimed := $state?claimed
                    let $has-overlap := some $p in ($match-start to $match-end)
                        satisfies map:contains($claimed, $p)
                    return
                        if ($has-overlap)
                        then
                            local:scan-for-term(
                                $text, $text-lower, $text-len,
                                $term, $term-len, $ref, $bk,
                                $match-start + 1, $state
                            )
                        else
                            let $char-before :=
                                if ($match-start = 1) then ""
                                else substring($text, $match-start - 1, 1)
                            let $char-after :=
                                if ($match-end = $text-len) then ""
                                else substring($text, $match-end + 1, 1)
                            let $before-ok := $char-before = "" or not(local:is-alnum($char-before))
                            let $after-ok := $char-after = "" or not(local:is-alnum($char-after))
                            return
                                if ($before-ok and $after-ok)
                                then
                                    let $new-claimed := fold-left(
                                        ($match-start to $match-end),
                                        $state?claimed,
                                        function($c, $p) { map:put($c, $p, true()) }
                                    )
                                    let $current-budget := $state?budget
                                    let $budget-entry := $current-budget($bk)
                                    let $new-budget := map:put(
                                        $current-budget, $bk,
                                        map:put($budget-entry, "allowed", $budget-entry?allowed - 1)
                                    )
                                    let $new-match := map {
                                        "start": $match-start,
                                        "end": $match-end,
                                        "term": $term
                                    }
                                    let $new-state := map {
                                        "budget": $new-budget,
                                        "matches": ($state?matches, $new-match),
                                        "claimed": $new-claimed
                                    }
                                    return local:scan-for-term(
                                        $text, $text-lower, $text-len,
                                        $term, $term-len, $ref, $bk,
                                        $match-end + 1, $new-state
                                    )
                                else
                                    local:scan-for-term(
                                        $text, $text-lower, $text-len,
                                        $term, $term-len, $ref, $bk,
                                        $match-start + 1, $state
                                    )
};

declare function local:build-segments(
    $text as xs:string,
    $matches as map(*)*,
    $vocab as map(*)
) as map(*)* {
    if (empty($matches))
    then map { "type": "text", "value": $text }
    else
        let $sorted := for $m in $matches order by $m?start return $m
        let $text-len := string-length($text)
        return (
            let $result := fold-left(
                $sorted,
                map { "pos": 1, "segments": () },
                function($acc, $m) {
                    let $start := $m?start
                    let $end := $m?end
                    let $term := $m?term
                    let $ref := $vocab($term)?ref
                    let $rs-type := ($vocab($term)?type, "topic")[1]
                    let $before :=
                        if ($start > $acc?pos)
                        then map { "type": "text", "value": substring($text, $acc?pos, $start - $acc?pos) }
                        else ()
                    let $rs-seg := map {
                        "type": "rs",
                        "value": substring($text, $start, $end - $start + 1),
                        "ref": $ref,
                        "rs-type": $rs-type
                    }
                    return map {
                        "pos": $end + 1,
                        "segments": ($acc?segments, $before, $rs-seg)
                    }
                }
            )
            return (
                $result?segments,
                if ($result?pos <= $text-len)
                then map { "type": "text", "value": substring($text, $result?pos) }
                else ()
            )
        )
};

(: ============================================================================
   ANCESTOR CHECKING
   ============================================================================ :)

declare function local:is-source-note($el as element()) as xs:boolean {
    local-name($el) = "note"
    and namespace-uri($el) = "http://www.tei-c.org/ns/1.0"
    and $el/@type = "source"
};

declare function local:should-skip($node as node()) as xs:boolean {
    some $anc in ($node/ancestor-or-self::*)
    satisfies (
        (
            namespace-uri($anc) = "http://www.tei-c.org/ns/1.0"
            and local-name($anc) = $skip-local-names
        )
        or local:is-source-note($anc)
    )
};

declare function local:inside-body($node as node()) as xs:boolean {
    exists($node/ancestor-or-self::tei:body)
};

(: ============================================================================
   RECURSIVE IDENTITY TRANSFORM WITH ANNOTATION
   ============================================================================ :)

declare function local:make-rs(
    $text as xs:string,
    $ref as xs:string,
    $rs-type as xs:string
) as element(tei:rs) {
    element { QName("http://www.tei-c.org/ns/1.0", "rs") } {
        attribute corresp { $ref },
        attribute type { $rs-type },
        text { $text }
    }
};

declare function local:segments-to-nodes($segments as map(*)*) as node()* {
    for $seg in $segments
    return
        if ($seg?type = "text")
        then text { $seg?value }
        else local:make-rs($seg?value, $seg?ref, $seg?rs-type)
};

declare function local:process-text-node(
    $text-node as text(),
    $vocab as map(*),
    $sorted-terms as xs:string*,
    $budget as map(*)
) as map(*) {
    let $text := string($text-node)
    return
        if (not(normalize-space($text)))
        then map { "nodes": $text-node, "budget": $budget }
        else if (not(local:inside-body($text-node)) or local:should-skip($text-node))
        then map { "nodes": $text-node, "budget": $budget }
        else
            let $match-result := local:find-matches($text, $sorted-terms, $vocab, $budget)
            let $matches := $match-result?matches
            let $updated-budget := $match-result?budget
            return
                if (empty($matches))
                then map { "nodes": $text-node, "budget": $updated-budget }
                else
                    let $segments := local:build-segments($text, $matches, $vocab)
                    let $nodes := local:segments-to-nodes($segments)
                    return map { "nodes": $nodes, "budget": $updated-budget }
};

declare function local:transform-children(
    $children as node()*,
    $vocab as map(*),
    $sorted-terms as xs:string*,
    $budget as map(*),
    $rejected-refs as map(xs:string, xs:boolean)
) as map(*) {
    fold-left(
        $children,
        map { "nodes": (), "budget": $budget },
        function($acc, $child) {
            let $result := local:transform-node(
                $child, $vocab, $sorted-terms, $acc?budget, $rejected-refs
            )
            return map {
                "nodes": ($acc?nodes, $result?nodes),
                "budget": $result?budget
            }
        }
    )
};

declare function local:transform-node(
    $node as node(),
    $vocab as map(*),
    $sorted-terms as xs:string*,
    $budget as map(*),
    $rejected-refs as map(xs:string, xs:boolean)
) as map(*) {
    typeswitch ($node)

        case document-node() return
            let $result := local:transform-children(
                $node/node(), $vocab, $sorted-terms, $budget, $rejected-refs
            )
            return map {
                "nodes": document { $result?nodes },
                "budget": $result?budget
            }

        case text() return
            local:process-text-node($node, $vocab, $sorted-terms, $budget)

        case element(tei:rs) return
            let $corresp := string($node/@corresp)
            return
                if (map:contains($rejected-refs, $corresp))
                then
                    local:transform-children(
                        $node/node(), $vocab, $sorted-terms, $budget, $rejected-refs
                    )
                else
                    let $result := local:transform-children(
                        $node/node(), $vocab, $sorted-terms, $budget, $rejected-refs
                    )
                    return map {
                        "nodes": element { node-name($node) } {
                            $node/@*,
                            $result?nodes
                        },
                        "budget": $result?budget
                    }

        case element() return
            let $result := local:transform-children(
                $node/node(), $vocab, $sorted-terms, $budget, $rejected-refs
            )
            return map {
                "nodes": element { node-name($node) } {
                    $node/@*,
                    $result?nodes
                },
                "budget": $result?budget
            }

        default return map { "nodes": $node, "budget": $budget }
};

(: ============================================================================
   DOCUMENT PROCESSING
   ============================================================================ :)

declare function local:process-document(
    $doc-path as xs:string,
    $filename as xs:string,
    $vocab as map(*),
    $sorted-terms as xs:string*,
    $budget as map(*),
    $rejected-refs as map(xs:string, xs:boolean)
) as map(*) {
    let $doc := doc($doc-path)

    (: Count existing rs elements before transform :)
    let $existing-rs := count($doc//tei:rs)

    (: Perform the recursive transform :)
    let $result := local:transform-node(
        $doc, $vocab, $sorted-terms, $budget, $rejected-refs
    )
    let $transformed-doc := $result?nodes

    (: Count rs elements after transform :)
    let $new-rs := count($transformed-doc//tei:rs)

    let $rejected-count := map:size($rejected-refs)
    let $added := (
        if ($new-rs > $existing-rs)
        then $new-rs - $existing-rs + $rejected-count
        else 0
    )

    (: Write the modified document back to eXist-db :)
    let $_ := xmldb:store($docs-collection, $filename, $transformed-doc)

    return map {
        "new": $added,
        "existing": $new-rs,
        "removed": $rejected-count,
        "budget": $result?budget
    }
};

(: ============================================================================
   MAIN
   ============================================================================ :)

let $_ := util:log("INFO", "apply-curated: Volume: " || $volume-id)
let $_ := util:log("INFO", "apply-curated: Documents collection: " || $docs-collection)

(: Validate documents collection exists :)
let $_ :=
    if (not(xmldb:collection-available($docs-collection)))
    then error(
        xs:QName("apply:DIR_NOT_FOUND"),
        "Document collection not found: " || $docs-collection
    )
    else ()

(: Load data :)
let $_ := util:log("INFO", "apply-curated: Loading review data...")
let $results-data := local:load-results()
let $rejections := local:load-rejections()
let $ref-to-canonical := local:load-variant-groups()

(: Build per-document budgets :)
let $_ := util:log("INFO", "apply-curated: Building per-document annotation budgets...")
let $budgets := local:build-document-budgets($results-data, $rejections)
let $_ := util:log("INFO", "apply-curated: Documents with accepted matches: " || map:size($budgets))

(: Build per-document rejected ref sets :)
let $doc-rejected-refs := local:build-rejected-refs($rejections)

(: List document files from eXist-db collection :)
let $doc-files :=
    for $name in xmldb:get-child-resources($docs-collection)
    where ends-with($name, ".xml")
    order by $name
    return $name

let $_ := util:log("INFO", "apply-curated: Processing " || count($doc-files) || " documents...")

(: Process each document :)
let $stats := fold-left(
    $doc-files,
    map { "total-new": 0, "total-existing": 0, "total-removed": 0, "docs-changed": 0 },
    function($acc, $filename) {
        let $doc-id := replace($filename, "\.xml$", "")
        let $doc-path := $docs-collection || "/" || $filename

        let $doc-budget := ($budgets($doc-id), map {})[1]
        let $rejected-refs := ($doc-rejected-refs($doc-id), map {})[1]

        return
            if (map:size($doc-budget) = 0 and map:size($rejected-refs) = 0)
            then $acc
            else
                let $vocab := local:build-vocab($doc-budget, $ref-to-canonical)
                let $sorted-terms :=
                    for $term in map:keys($vocab)
                    order by string-length($term) descending, $term
                    return $term

                let $result := try {
                    local:process-document(
                        $doc-path, $filename, $vocab, $sorted-terms, $doc-budget, $rejected-refs
                    )
                } catch * {
                    util:log("ERROR", "apply-curated: ERROR processing " || $filename || ": " || $err:description),
                    map { "new": 0, "existing": 0, "removed": 0 }
                }

                let $_ :=
                    if ($result?new > 0 or $result?removed > 0)
                    then util:log("INFO", "apply-curated:   " || $filename || ": +" || $result?new || " new, -" ||
                        $result?removed || " removed")
                    else ()

                return map {
                    "total-new": $acc?total-new + $result?new,
                    "total-existing": $acc?total-existing + $result?existing,
                    "total-removed": $acc?total-removed + $result?removed,
                    "docs-changed":
                        if ($result?new > 0 or $result?removed > 0)
                        then $acc?docs-changed + 1
                        else $acc?docs-changed
                }
    }
)

(: Output summary :)
let $_ := util:log("INFO", "apply-curated: ========================================")
let $_ := util:log("INFO", "apply-curated: Summary for " || $volume-id || ":")
let $_ := util:log("INFO", "apply-curated:   Documents processed: " || count($doc-files))
let $_ := util:log("INFO", "apply-curated:   Documents with changes: " || $stats?docs-changed)
let $_ := util:log("INFO", "apply-curated:   Rejected <rs> annotations removed: " || $stats?total-removed)
let $_ := util:log("INFO", "apply-curated:   New <rs> annotations added: " || $stats?total-new)
let $_ := util:log("INFO", "apply-curated:   Total <rs> annotations now: " || $stats?total-existing)

return
    <apply-result>
        <volume>{$volume-id}</volume>
        <documents-collection>{$docs-collection}</documents-collection>
        <documents-processed>{count($doc-files)}</documents-processed>
        <documents-changed>{$stats?docs-changed}</documents-changed>
        <annotations-removed>{$stats?total-removed}</annotations-removed>
        <annotations-added>{$stats?total-new}</annotations-added>
        <annotations-total>{$stats?total-existing}</annotations-total>
    </apply-result>
