xquery version "3.1";

(:~
 : Apply Curated Subject Annotations to FRUS TEI Documents
 :
 : This standalone XQuery script replicates the core logic of
 : scripts/apply_curated_annotations.py.
 :
 : It reads string_match_results and annotation_rejections files,
 : computes per-document annotation budgets, and applies accepted
 : annotations as tei:rs elements via recursive identity transform.
 :
 : Reads:
 :   - string_match_results_{volume-id}.json  (repo root)
 :   - config/annotation_rejections_{volume-id}.json (optional)
 :   - variant_groups.json (repo root, optional)
 :
 : For each document in data/documents/{volume-id}/:
 :   - Builds per-document vocabulary of accepted matches
 :   - Finds match positions using longest-match-first strategy
 :   - Inserts <rs corresp="recXXX" type="topic"> elements
 :   - Removes (unwraps) existing <rs> elements that were rejected
 :   - Writes the modified document back
 :
 : Usage (BaseX):
 :   basex -b volume-id=frus1969-76v19p2 apply-curated-annotations.xq
 :)

declare namespace tei = "http://www.tei-c.org/ns/1.0";
declare namespace xml = "http://www.w3.org/XML/1998/namespace";
declare namespace file = "http://expath.org/ns/file";
declare namespace output = "http://www.w3.org/2010/xslt-xquery-serialization";

(: ============================================================================
   CONFIGURATION
   ============================================================================ :)

(: Volume to process - override via external variable binding :)
declare variable $volume-id as xs:string external := "frus1981-88v41";

(: Base directory - the project root (parent of the queries/ directory) :)
declare variable $base-dir as xs:string external :=
    file:parent(file:parent(static-base-uri()));

(: ============================================================================
   PATH RESOLUTION
   ============================================================================ :)

(: String match results file (produced by annotate_documents.py) :)
declare variable $results-path :=
    file:resolve-path("string_match_results_" || $volume-id || ".json", $base-dir);

(: Annotation rejections file (exported from string-match-review.html) :)
declare variable $rejections-path :=
    file:resolve-path("config/annotation_rejections_" || $volume-id || ".json", $base-dir);

(: Variant groups file for canonical ref mapping :)
declare variable $variant-groups-path :=
    file:resolve-path("variant_groups.json", $base-dir);

(: Documents directory :)
declare variable $docs-dir :=
    file:resolve-path("data/documents/" || $volume-id || "/", $base-dir);

(: ============================================================================
   ANCESTOR ELEMENT NAMES TO SKIP
   ============================================================================
   Text inside these elements should not be annotated. Includes rs, persName,
   orgName, placeName, and gloss. note[@type='source'] is checked separately.
   ============================================================================ :)

declare variable $skip-local-names := ("rs", "persName", "orgName", "placeName", "gloss");

(: ============================================================================
   JSON DATA LOADING
   ============================================================================ :)

(:~
 : Load the string_match_results JSON file.
 : Returns the parsed JSON map, or raises an error if not found.
 :)
declare function local:load-results() as map(*) {
    if (not(file:exists($results-path)))
    then error(
        xs:QName("apply:FILE_NOT_FOUND"),
        "String match results not found: " || $results-path ||
        ". Run annotate_documents.py first."
    )
    else
        let $text := file:read-text($results-path)
        let $data := parse-json($text)
        let $total := $data?metadata?total_matches
        let $docs := map:size($data?by_document)
        let $_ := trace("Loaded " || $results-path || ": " ||
            $total || " matches across " || $docs || " documents", "apply")
        return $data
};

(:~
 : Load annotation rejections. Returns a map of rejection keys to true,
 : or an empty map if the file does not exist.
 :)
declare function local:load-rejections() as map(xs:string, xs:boolean) {
    if (not(file:exists($rejections-path)))
    then (
        trace("No rejections file found — accepting all matches", "apply"),
        map {}
    )
    else
        let $text := file:read-text($rejections-path)
        let $data := parse-json($text)
        let $rejections-array := $data?rejections
        let $rejection-map := map:merge(
            for $i in 1 to array:size($rejections-array)
            let $r := $rejections-array($i)
            let $key := $r?key
            where $key
            return map:entry($key, true())
        )
        let $_ := trace("Loaded " || map:size($rejection-map) || " rejections", "apply")
        return $rejection-map
};

(:~
 : Load variant_groups.json for ref-to-canonical mapping.
 : Returns a map of ref -> canonical ref, or empty map if file not found.
 :)
declare function local:load-variant-groups() as map(xs:string, xs:string) {
    if (not(file:exists($variant-groups-path)))
    then map {}
    else
        let $text := file:read-text($variant-groups-path)
        let $data := parse-json($text)
        let $ref-to-canonical := $data?ref_to_canonical
        return
            if (empty($ref-to-canonical))
            then map {}
            else
                let $_ := trace("Loaded variant_groups: " ||
                    map:size($ref-to-canonical) || " ref mappings", "apply")
                return $ref-to-canonical
};

(: ============================================================================
   PER-DOCUMENT BUDGET COMPUTATION
   ============================================================================
   For each document, compute the allowed occurrence count per
   (matched_text_lower, canonical_ref) pair:
       allowed = total_matches - rejected_matches

   Returns a map:
     { doc-id: { "term_lower\tref": { "allowed": N, "type": str } } }

   The budget key uses tab as separator since it cannot appear in terms or refs.
   ============================================================================ :)

(:~
 : Build a composite budget key from term and ref.
 :)
declare function local:budget-key($term-lower as xs:string, $ref as xs:string) as xs:string {
    $term-lower || "&#9;" || $ref
};

(:~
 : Build per-document annotation budgets from match results and rejections.
 : Returns a map keyed by doc-id, each value being a map of budget-key to
 : map with "allowed" count and "type".
 :)
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
        (: Build the budget for this document :)
        let $doc-budget := fold-left(
            (1 to array:size($matches-array)),
            map {},
            function($acc, $i) {
                let $match := $matches-array($i)
                let $ref := ($match?canonical_ref, $match?ref)[1]
                let $term := ($match?matched_text, $match?term)[1]
                let $position := $match?position
                let $rs-type := ($match?type, "topic")[1]
                (: Check if this specific match was rejected :)
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

(:~
 : Build per-document rejected ref sets from rejection keys.
 : Rejection keys have format "docId:ref:position".
 : Returns { doc-id: set-of-refs-as-map }
 :)
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

(:~
 : Build the vocabulary for a document from its budget.
 : Returns a map of { term-lower: { "ref": canonical-ref, "type": rs-type } }.
 :
 : If multiple refs map to the same term, the last one wins (matching Python
 : dict-overwrite behaviour).
 :)
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
        (: Map ref through variant groups to get current canonical :)
        let $canonical := ($ref-to-canonical($ref), $ref)[1]
        let $rs-type := ($info?type, "topic")[1]
        return map:entry($term-lower, map { "ref": $canonical, "type": $rs-type })
    )
};

(: ============================================================================
   TEXT NODE MATCHING
   ============================================================================
   Longest-match-first, case-insensitive, word-boundary-checked matching.
   Uses a "claimed positions" approach: once characters are matched, they
   cannot overlap with later matches.
   ============================================================================ :)

(:~
 : Check if a codepoint is alphanumeric (a-z, A-Z, 0-9).
 :)
declare function local:is-alnum($char as xs:string) as xs:boolean {
    matches($char, "[A-Za-z0-9]")
};

(:~
 : Find all non-overlapping matches in a text string.
 :
 : Returns a sequence of maps with keys: "start", "end", "term".
 : Matches are sorted by start position.
 :
 : Parameters:
 :   $text         - the text to search
 :   $sorted-terms - terms sorted by length descending
 :   $vocab        - { term-lower: { ref, type } }
 :   $budget       - mutable-via-rebuild budget map (budget-key -> { allowed: N })
 :
 : Returns: (updated-budget, matches-sequence) encoded as a map:
 :   { "budget": ..., "matches": array of { start, end, term } }
 :)
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
                            (: Scan for all occurrences of term in text :)
                            local:scan-for-term(
                                $text, $text-lower, $text-len,
                                $term, $term-len,
                                $ref, $bk,
                                1, (: start searching at position 1 :)
                                $state
                            )
        }
    )
};

(:~
 : Recursively scan text for occurrences of a term, starting at $pos.
 :)
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
    (: Check remaining budget :)
    let $remaining := ($state?budget($bk)?allowed, 0)[1]
    return
        if ($remaining <= 0 or $pos + $term-len - 1 > $text-len)
        then $state
        else
            (: Find next occurrence :)
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
                    let $match-start := $pos + $idx-in-area - 1  (: 1-based position in full text :)
                    let $match-end := $match-start + $term-len - 1
                    (: Check for overlap with already-claimed positions :)
                    let $claimed := $state?claimed
                    let $has-overlap := some $p in ($match-start to $match-end)
                        satisfies map:contains($claimed, $p)
                    return
                        if ($has-overlap)
                        then
                            (: Skip this occurrence, continue searching after it :)
                            local:scan-for-term(
                                $text, $text-lower, $text-len,
                                $term, $term-len, $ref, $bk,
                                $match-start + 1, $state
                            )
                        else
                            (: Word boundary check :)
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
                                    (: Valid match: claim positions, decrement budget, record match :)
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
                                    (: Not a word boundary match, continue :)
                                    local:scan-for-term(
                                        $text, $text-lower, $text-len,
                                        $term, $term-len, $ref, $bk,
                                        $match-start + 1, $state
                                    )
};

(:~
 : Given a text string and its matches, produce a sequence of segments.
 : Each segment is either:
 :   map { "type": "text", "value": "..." }
 :   map { "type": "rs", "value": "...", "ref": "...", "rs-type": "..." }
 :)
declare function local:build-segments(
    $text as xs:string,
    $matches as map(*)*,
    $vocab as map(*)
) as map(*)* {
    if (empty($matches))
    then map { "type": "text", "value": $text }
    else
        (: Sort matches by start position :)
        let $sorted := for $m in $matches order by $m?start return $m
        let $text-len := string-length($text)
        return (
            (: Process each match, emitting text-before and the rs segment :)
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

(:~
 : Check whether an element is a note[@type='source'].
 :)
declare function local:is-source-note($el as element()) as xs:boolean {
    local-name($el) = "note"
    and namespace-uri($el) = "http://www.tei-c.org/ns/1.0"
    and $el/@type = "source"
};

(:~
 : Check if a node should be skipped for annotation.
 : Returns true if the node itself or any ancestor is in the skip list
 : or is a source note.
 :)
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

(:~
 : Check if we are inside the tei:body element.
 :)
declare function local:inside-body($node as node()) as xs:boolean {
    exists($node/ancestor-or-self::tei:body)
};

(: ============================================================================
   UNWRAP REJECTED RS ELEMENTS
   ============================================================================
   For each rejected <rs> element, replace it with its contents (text and
   children), effectively removing the <rs> wrapper while preserving content.
   This is handled during the recursive transform.
   ============================================================================ :)

(: ============================================================================
   RECURSIVE IDENTITY TRANSFORM WITH ANNOTATION
   ============================================================================
   Walk the tree node by node:
   1. For text nodes inside body and not inside skip ancestors:
      find matches, split text, insert rs elements.
   2. For existing rejected rs elements: unwrap them (emit content only).
   3. Copy everything else as-is.

   Because XQuery values are immutable, the budget must be threaded through
   the transform. We use a stateful approach: the transform function returns
   a map { "nodes": ..., "budget": ... } so the updated budget propagates
   to subsequent siblings.
   ============================================================================ :)

(:~
 : Create a tei:rs element with the given attributes and text content.
 :)
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

(:~
 : Convert a sequence of segment maps into a sequence of nodes
 : (text nodes and rs elements).
 :)
declare function local:segments-to-nodes($segments as map(*)*) as node()* {
    for $seg in $segments
    return
        if ($seg?type = "text")
        then text { $seg?value }
        else local:make-rs($seg?value, $seg?ref, $seg?rs-type)
};

(:~
 : Process a text node: find matches and return segments as nodes.
 : Returns map { "nodes": node()*, "budget": map(*) }
 :)
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

(:~
 : Recursively transform a sequence of child nodes, threading the budget.
 : Returns map { "nodes": node()*, "budget": map(*) }
 :)
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

(:~
 : Main recursive transform function.
 : Returns map { "nodes": node()*, "budget": map(*) }
 :)
declare function local:transform-node(
    $node as node(),
    $vocab as map(*),
    $sorted-terms as xs:string*,
    $budget as map(*),
    $rejected-refs as map(xs:string, xs:boolean)
) as map(*) {
    typeswitch ($node)

        (: Document node: transform children :)
        case document-node() return
            let $result := local:transform-children(
                $node/node(), $vocab, $sorted-terms, $budget, $rejected-refs
            )
            return map {
                "nodes": document { $result?nodes },
                "budget": $result?budget
            }

        (: Text nodes: search for matches and split :)
        case text() return
            local:process-text-node($node, $vocab, $sorted-terms, $budget)

        (: RS elements: check if rejected and should be unwrapped :)
        case element(tei:rs) return
            let $corresp := string($node/@corresp)
            return
                if (map:contains($rejected-refs, $corresp))
                then
                    (: Unwrap: emit the rs element's content without the wrapper.
                       The children still need to be transformed for matches. :)
                    local:transform-children(
                        $node/node(), $vocab, $sorted-terms, $budget, $rejected-refs
                    )
                else
                    (: Keep the rs element, but still transform its children
                       (though skip-ancestor logic will prevent annotation inside it) :)
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

        (: Other elements: copy attributes, transform children :)
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

        (: Comments, PIs, etc.: copy as-is :)
        default return map { "nodes": $node, "budget": $budget }
};

(: ============================================================================
   DOCUMENT PROCESSING
   ============================================================================ :)

(:~
 : Process a single TEI document file.
 : Returns a map with statistics: { "new": N, "removed": N }
 :)
declare function local:process-document(
    $doc-path as xs:string,
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

    (: Compute how many were added vs removed :)
    let $rejected-count := map:size($rejected-refs)
    let $added := (
        if ($new-rs > $existing-rs)
        then $new-rs - $existing-rs + $rejected-count
        else 0
    )

    (: Write the modified document back :)
    let $serialization-params :=
        <output:serialization-parameters
            xmlns:output="http://www.w3.org/2010/xslt-xquery-serialization">
            <output:method value="xml"/>
            <output:encoding value="UTF-8"/>
            <output:indent value="no"/>
            <output:omit-xml-declaration value="no"/>
        </output:serialization-parameters>

    let $_ := file:write($doc-path, $transformed-doc, $serialization-params)

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

let $_ := trace("Volume: " || $volume-id, "apply")
let $_ := trace("Documents dir: " || $docs-dir, "apply")

(: Validate documents directory exists :)
let $_ :=
    if (not(file:is-dir($docs-dir)))
    then error(
        xs:QName("apply:DIR_NOT_FOUND"),
        "Document directory not found: " || $docs-dir
    )
    else ()

(: Load data :)
let $_ := trace("Loading review data...", "apply")
let $results-data := local:load-results()
let $rejections := local:load-rejections()
let $ref-to-canonical := local:load-variant-groups()

(: Build per-document budgets :)
let $_ := trace("Building per-document annotation budgets...", "apply")
let $budgets := local:build-document-budgets($results-data, $rejections)
let $_ := trace("Documents with accepted matches: " || map:size($budgets), "apply")

(: Build per-document rejected ref sets :)
let $doc-rejected-refs := local:build-rejected-refs($rejections)

(: List document files :)
let $doc-files :=
    for $name in file:list($docs-dir)
    where ends-with($name, ".xml")
    order by $name
    return $name

let $_ := trace("Processing " || count($doc-files) || " documents...", "apply")

(: Process each document, threading budget state through :)
let $stats := fold-left(
    $doc-files,
    map { "total-new": 0, "total-existing": 0, "total-removed": 0, "docs-changed": 0 },
    function($acc, $filename) {
        let $doc-id := replace($filename, "\.xml$", "")
        let $doc-path := $docs-dir || $filename

        (: Get this document's budget and rejected refs :)
        let $doc-budget := ($budgets($doc-id), map {})[1]
        let $rejected-refs := ($doc-rejected-refs($doc-id), map {})[1]

        return
            (: Skip if nothing to do :)
            if (map:size($doc-budget) = 0 and map:size($rejected-refs) = 0)
            then $acc
            else
                (: Build vocabulary for this document from its budget :)
                let $vocab := local:build-vocab($doc-budget, $ref-to-canonical)
                let $sorted-terms :=
                    for $term in map:keys($vocab)
                    order by string-length($term) descending, $term
                    return $term

                let $result := try {
                    local:process-document(
                        $doc-path, $vocab, $sorted-terms, $doc-budget, $rejected-refs
                    )
                } catch * {
                    trace("ERROR processing " || $filename || ": " || $err:description, "apply"),
                    map { "new": 0, "existing": 0, "removed": 0 }
                }

                let $_ :=
                    if ($result?new > 0 or $result?removed > 0)
                    then trace(
                        "  " || $filename || ": +" || $result?new || " new, -" ||
                        $result?removed || " removed", "apply"
                    )
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
let $_ := trace("========================================", "apply")
let $_ := trace("Summary for " || $volume-id || ":", "apply")
let $_ := trace("  Documents processed: " || count($doc-files), "apply")
let $_ := trace("  Documents with changes: " || $stats?docs-changed, "apply")
let $_ := trace("  Rejected <rs> annotations removed: " || $stats?total-removed, "apply")
let $_ := trace("  New <rs> annotations added: " || $stats?total-new, "apply")
let $_ := trace("  Total <rs> annotations now: " || $stats?total-existing, "apply")

return
    <apply-result>
        <volume>{$volume-id}</volume>
        <documents-dir>{$docs-dir}</documents-dir>
        <documents-processed>{count($doc-files)}</documents-processed>
        <documents-changed>{$stats?docs-changed}</documents-changed>
        <annotations-removed>{$stats?total-removed}</annotations-removed>
        <annotations-added>{$stats?total-new}</annotations-added>
        <annotations-total>{$stats?total-existing}</annotations-total>
    </apply-result>
