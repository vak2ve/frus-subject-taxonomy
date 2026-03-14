xquery version "3.1";

(:~
 : Extract Existing Annotations from TEI XML Documents (eXist-db version)
 :
 : This is the eXist-db adapted version of extract-existing-annotations.xq.
 : It uses eXist-db native modules (xmldb, util) instead of the EXPath file: module
 : for filesystem I/O, and reads/writes from eXist-db collections.
 :
 : Reads <rs corresp="recXXX"> tags from TEI documents and outputs a JSON file
 : in the same format as string_match_results_*.json so it can be used with the
 : string-match-review tool.
 :
 : Usage in eXist-db:
 :   Run via eXide or the REST API. Set $volume-id to the desired volume.
 :)

declare namespace tei = "http://www.tei-c.org/ns/1.0";
declare namespace util = "http://exist-db.org/xquery/util";
declare namespace xmldb = "http://exist-db.org/xquery/xmldb";
declare namespace output = "http://www.w3.org/2010/xslt-xquery-serialization";

declare option output:method "json";

(: ── Configuration ───────────────────────────────────────────────────── :)

(: Root collection for the hsg-annotate-data app in eXist-db :)
declare variable $app-root as xs:string := "/db/apps/hsg-annotate-data";

(: Volume ID to process (required) :)
declare variable $volume-id as xs:string external;

(: ── Derived paths (eXist-db collections) ────────────────────────────── :)

declare variable $taxonomy-path as xs:string := $app-root || "/data/subject-taxonomy-lcsh.xml";
declare variable $lcsh-mapping-path as xs:string := $app-root || "/config/lcsh_mapping.json";
declare variable $documents-collection as xs:string := $app-root || "/data/documents/" || $volume-id;

(: ── Taxonomy loading ─────────────────────────────────────────────────── :)

(:~
 : Load subject-taxonomy-lcsh.xml into a map keyed by @ref.
 :)
declare function local:load-taxonomy() as map(*) {
    let $doc := doc($taxonomy-path)
    let $root := $doc/*
    return map:merge(
        for $cat in $root/category
        let $cat-label := ($cat/@label/string(), "Uncategorized")[1]
        for $sub in $cat/subcategory
        let $sub-label := ($sub/@label/string(), "General")[1]
        for $subj in $sub/subject
        let $name-el := $subj/name
        let $ref := $subj/@ref/string()
        where $ref and $name-el and normalize-space($name-el)
        return map:entry($ref, map {
            "term": normalize-space($name-el),
            "type": ($subj/@type/string(), "topic")[1],
            "count": xs:integer(($subj/@count/string(), "0")[1]),
            "category": $cat-label,
            "subcategory": $sub-label,
            "lcsh_uri": ($subj/@lcsh-uri/string(), "")[1],
            "lcsh_match": ($subj/@lcsh-match/string(), "")[1]
        })
    )
};

(:~
 : Load config/lcsh_mapping.json into a map keyed by ref.
 :)
declare function local:load-lcsh-mapping() as map(*) {
    if (doc-available($lcsh-mapping-path)) then
        let $json := json-doc($lcsh-mapping-path)
        return map:merge(
            for $ref in map:keys($json)
            let $entry := map:get($json, $ref)
            let $uri := $entry?lcsh_uri
            where $uri and $uri != ""
            return map:entry($ref, map {
                "lcsh_uri": string($uri),
                "lcsh_label": string(($entry?lcsh_label, "")[1]),
                "match_quality": string(($entry?match_quality, "")[1])
            })
        )
    else
        map {}
};

(: ── Text extraction helpers ──────────────────────────────────────────── :)

declare function local:get-text-content($elem as element()) as xs:string {
    normalize-space(string-join($elem//text(), ""))
};

declare function local:extract-body-text($doc as document-node()) as xs:string {
    let $body := $doc//tei:text/tei:body
    return
        if ($body) then
            normalize-space(string-join($body//text(), " "))
        else
            ""
};

declare function local:is-sentence-boundary($text as xs:string, $i as xs:integer, $len as xs:integer) as xs:boolean {
    let $ch := substring($text, $i, 1)
    return
        if (not($ch = (".", "!", "?"))) then false()
        else if ($i + 2 > $len) then false()
        else if (substring($text, $i + 1, 1) != " ") then false()
        else
            let $next-char := substring($text, $i + 2, 1)
            return
                if (not(matches($next-char, "[A-Z]"))) then false()
                else
                    let $prev-char := if ($i > 1) then substring($text, $i - 1, 1) else ""
                    let $prev-prev := if ($i > 2) then substring($text, $i - 2, 1) else ""
                    return
                        if (matches($prev-char, "[A-Z]") and ($i < 3 or not(matches($prev-prev, "[a-zA-Z]")))) then false()
                        else if (matches($prev-char, "[0-9]")) then false()
                        else true()
};

declare function local:extract-context(
    $full-text as xs:string,
    $matched-text as xs:string,
    $max-chars as xs:integer
) as xs:string {
    if ($full-text = "" or $matched-text = "") then
        ($matched-text, "")[1]
    else
        let $ft-lower := lower-case($full-text)
        let $mt-lower := lower-case($matched-text)
        let $idx := (
            let $pos := string-length(substring-before($ft-lower, $mt-lower)) + 1
            return if (contains($ft-lower, $mt-lower)) then $pos else 0
        )
        let $normalized-match := normalize-space($matched-text)
        let $nm-lower := lower-case($normalized-match)
        let $idx := (
            if ($idx > 0) then $idx
            else
                let $pos := string-length(substring-before($ft-lower, $nm-lower)) + 1
                return if (contains($ft-lower, $nm-lower)) then $pos else 0
        )
        return
            if ($idx = 0) then $matched-text
            else
                let $match-len := string-length($matched-text)
                let $match-end := $idx + $match-len - 1
                let $text-len := string-length($full-text)
                let $sent-start := local:find-sentence-start($full-text, $idx, $text-len)
                let $sent-end := local:find-sentence-end($full-text, $match-end, $text-len)
                let $sentence := normalize-space(substring($full-text, $sent-start, $sent-end - $sent-start + 1))
                return
                    if (string-length($sentence) <= $max-chars) then $sentence
                    else
                        let $match-offset := $idx - $sent-start
                        let $mid := $match-offset + ($match-len idiv 2)
                        let $half := $max-chars idiv 2
                        let $trunc-start := max((1, $mid - $half))
                        let $trunc-end := min((string-length($sentence), $mid + $half))
                        let $prefix := if ($trunc-start > 1) then "..." else ""
                        let $suffix := if ($trunc-end < string-length($sentence)) then "..." else ""
                        return $prefix || substring($sentence, $trunc-start, $trunc-end - $trunc-start + 1) || $suffix
};

declare function local:find-sentence-start($text as xs:string, $pos as xs:integer, $len as xs:integer) as xs:integer {
    local:scan-backward($text, $pos - 1, $len)
};

declare function local:scan-backward($text as xs:string, $i as xs:integer, $len as xs:integer) as xs:integer {
    if ($i < 1) then 1
    else if (local:is-sentence-boundary($text, $i, $len)) then $i + 2
    else local:scan-backward($text, $i - 1, $len)
};

declare function local:find-sentence-end($text as xs:string, $pos as xs:integer, $len as xs:integer) as xs:integer {
    local:scan-forward($text, $pos, $len)
};

declare function local:scan-forward($text as xs:string, $i as xs:integer, $len as xs:integer) as xs:integer {
    if ($i > $len) then $len
    else if (local:is-sentence-boundary($text, $i, $len)) then $i
    else local:scan-forward($text, $i + 1, $len)
};

(: ── Document metadata extraction ─────────────────────────────────────── :)

declare function local:extract-doc-metadata($doc as document-node()) as map(*) {
    let $title-el := $doc//tei:titleStmt/tei:title
    let $title := if ($title-el and normalize-space($title-el)) then normalize-space($title-el) else ""
    let $date-el := $doc//tei:settingDesc//tei:date
    let $date-text := if ($date-el and normalize-space($date-el)) then normalize-space($date-el) else ""
    let $subtype-el := $doc//tei:bibl[@type = "frus-div-subtype"]
    let $doc-type := if ($subtype-el and normalize-space($subtype-el)) then normalize-space($subtype-el) else ""
    return map {
        "title": $title,
        "date": $date-text,
        "type": $doc-type
    }
};

(: ── Annotation extraction ────────────────────────────────────────────── :)

declare function local:extract-rs-annotations($doc as document-node()) as map(*)* {
    let $body := $doc//tei:text/tei:body
    return
        if (not($body)) then ()
        else
            for $rs at $pos in $body//tei:rs[@corresp]
            let $corresp := $rs/@corresp/string()
            where $corresp != ""
            let $matched-text := normalize-space(string-join($rs//text(), " "))
            where $matched-text != ""
            return map {
                "ref": $corresp,
                "type": ($rs/@type/string(), "topic")[1],
                "matched_text": $matched-text,
                "position": $pos - 1
            }
};

(: ── Main processing ──────────────────────────────────────────────────── :)

let $_ := util:log("INFO", "extract-existing-annotations: Processing volume " || $volume-id)

let $taxonomy := local:load-taxonomy()
let $lcsh-mapping := local:load-lcsh-mapping()
let $taxonomy-refs := map:keys($taxonomy)
let $total-taxonomy-terms := count($taxonomy-refs)

let $_ := util:log("INFO", "extract-existing-annotations: Loaded taxonomy with " || $total-taxonomy-terms || " terms")

(: List all XML files in the volume documents collection :)
let $all-files :=
    if (xmldb:collection-available($documents-collection)) then
        sort(xmldb:get-child-resources($documents-collection)[ends-with(., ".xml")])
    else (
        util:log("ERROR", "extract-existing-annotations: Documents collection not available: " || $documents-collection),
        ()
    )

let $_ := util:log("INFO", "extract-existing-annotations: Found " || count($all-files) || " document files")

(: Process each document file :)
let $doc-results :=
    for $filename in $all-files
    let $doc-id := replace($filename, "\.xml$", "")
    let $doc := doc($documents-collection || "/" || $filename)
    let $meta := local:extract-doc-metadata($doc)
    let $annotations := local:extract-rs-annotations($doc)
    return map {
        "doc_id": $doc-id,
        "filename": $filename,
        "meta": $meta,
        "annotations": array { $annotations },
        "doc": $doc
    }

(: Build by_document and by_term indices :)
let $total-documents := count($doc-results)

let $processing :=
    fold-left($doc-results, map {
        "by_document": map {},
        "by_term": map {},
        "total_matches": 0,
        "docs_with_matches": 0,
        "unknown_refs": map {}
    }, function($state, $doc-result) {
        let $doc-id := $doc-result?doc_id
        let $meta := $doc-result?meta
        let $annotations := $doc-result?annotations?*
        let $doc := $doc-result?doc

        return
            if (count($annotations) = 0) then
                map:merge((
                    $state,
                    map {
                        "by_document": map:merge((
                            $state?by_document,
                            map:entry($doc-id, map {
                                "title": $meta?title,
                                "date": $meta?date,
                                "doc_type": $meta?type,
                                "match_count": 0,
                                "unique_terms": 0,
                                "matches": array {},
                                "body_length": 0
                            })
                        ))
                    }
                ))
            else
                let $full-text := local:extract-body-text($doc)

                let $ann-processing := fold-left(
                    $annotations,
                    map {
                        "doc_matches": array {},
                        "doc_refs": map {},
                        "by_term": $state?by_term,
                        "match_count": 0,
                        "unknown_refs": $state?unknown_refs
                    },
                    function($astate, $ann) {
                        let $ref := $ann?ref
                        let $tax-info := $taxonomy($ref)
                        return
                            if (empty($tax-info)) then
                                map:merge((
                                    $astate,
                                    map { "unknown_refs": map:merge(($astate?unknown_refs, map:entry($ref, true()))) }
                                ))
                            else
                                let $lcsh-info := ($lcsh-mapping($ref), map {})[1]
                                let $lcsh-uri := (
                                    if ($tax-info?lcsh_uri != "") then $tax-info?lcsh_uri
                                    else ($lcsh-info?lcsh_uri, "")[1]
                                )
                                let $lcsh-match-quality := (
                                    if ($tax-info?lcsh_match != "") then $tax-info?lcsh_match
                                    else ($lcsh-info?match_quality, "")[1]
                                )
                                let $sentence := local:extract-context($full-text, $ann?matched_text, 300)

                                let $match-entry := map {
                                    "term": $tax-info?term,
                                    "ref": $ref,
                                    "canonical_ref": $ref,
                                    "matched_ref": $ref,
                                    "type": $ann?type,
                                    "category": $tax-info?category,
                                    "subcategory": $tax-info?subcategory,
                                    "lcsh_uri": $lcsh-uri,
                                    "lcsh_match": $lcsh-match-quality,
                                    "position": $ann?position,
                                    "matched_text": $ann?matched_text,
                                    "sentence": $sentence,
                                    "is_variant_form": false(),
                                    "is_consolidated": false()
                                }

                                let $existing-term := $astate?by_term($ref)
                                let $term-entry :=
                                    if (empty($existing-term)) then
                                        map {
                                            "term": $tax-info?term,
                                            "type": $ann?type,
                                            "category": $tax-info?category,
                                            "subcategory": $tax-info?subcategory,
                                            "lcsh_uri": $lcsh-uri,
                                            "lcsh_match": $lcsh-match-quality,
                                            "documents": map:entry($doc-id, array {
                                                map {
                                                    "position": $ann?position,
                                                    "matched_text": $ann?matched_text,
                                                    "sentence": $sentence,
                                                    "is_consolidated": false()
                                                }
                                            }),
                                            "total_occurrences": 1,
                                            "document_count": 1,
                                            "variant_names": array {},
                                            "variant_refs": array {}
                                        }
                                    else
                                        let $existing-doc-entries := $existing-term?documents($doc-id)
                                        let $new-occ := map {
                                            "position": $ann?position,
                                            "matched_text": $ann?matched_text,
                                            "sentence": $sentence,
                                            "is_consolidated": false()
                                        }
                                        let $updated-docs :=
                                            if (empty($existing-doc-entries)) then
                                                map:merge((
                                                    $existing-term?documents,
                                                    map:entry($doc-id, array { $new-occ })
                                                ))
                                            else
                                                map:merge((
                                                    $existing-term?documents,
                                                    map:entry($doc-id, array { $existing-doc-entries?*, $new-occ })
                                                ))
                                        let $doc-count-inc :=
                                            if (empty($existing-doc-entries)) then 1 else 0
                                        return map:merge((
                                            $existing-term,
                                            map {
                                                "documents": $updated-docs,
                                                "total_occurrences": $existing-term?total_occurrences + 1,
                                                "document_count": $existing-term?document_count + $doc-count-inc
                                            }
                                        ))

                                return map:merge((
                                    $astate,
                                    map {
                                        "doc_matches": array { $astate?doc_matches?*, $match-entry },
                                        "doc_refs": map:merge(($astate?doc_refs, map:entry($ref, true()))),
                                        "by_term": map:merge((
                                            $astate?by_term,
                                            map:entry($ref, $term-entry)
                                        )),
                                        "match_count": $astate?match_count + 1
                                    }
                                ))
                    }
                )

                let $doc-match-count := $ann-processing?match_count
                let $doc-unique := count(map:keys($ann-processing?doc_refs))

                return map:merge((
                    $state,
                    map {
                        "by_document": map:merge((
                            $state?by_document,
                            map:entry($doc-id, map {
                                "title": $meta?title,
                                "date": $meta?date,
                                "doc_type": $meta?type,
                                "match_count": $doc-match-count,
                                "unique_terms": $doc-unique,
                                "matches": $ann-processing?doc_matches,
                                "body_length": string-length($full-text)
                            })
                        )),
                        "by_term": $ann-processing?by_term,
                        "total_matches": $state?total_matches + $doc-match-count,
                        "docs_with_matches": $state?docs_with_matches + (if ($doc-match-count > 0) then 1 else 0),
                        "unknown_refs": $ann-processing?unknown_refs
                    }
                ))
    })

(: Collect variant names per term :)
let $by-term-with-variants := map:merge(
    for $ref in map:keys($processing?by_term)
    let $term-data := $processing?by_term($ref)
    let $all-texts :=
        distinct-values(
            for $doc-id in map:keys($term-data?documents)
            let $occs := $term-data?documents($doc-id)?*
            for $occ in $occs
            return $occ?matched_text
        )
    let $variant-names :=
        if (count($all-texts) > 1) then array { sort($all-texts) }
        else $term-data?variant_names
    return map:entry($ref, map:merge((
        $term-data,
        map { "variant_names": $variant-names }
    )))
)

(: Build unmatched terms list :)
let $matched-refs := map:keys($processing?by_term)
let $unmatched-terms := array {
    for $ref in $taxonomy-refs
    where not($ref = $matched-refs)
    let $info := $taxonomy($ref)
    order by $info?category, $info?term
    return map {
        "term": $info?term,
        "ref": $ref,
        "category": $info?category,
        "subcategory": $info?subcategory
    }
}

let $_ := util:log("INFO", "extract-existing-annotations: Completed - " ||
    $processing?total_matches || " matches, " ||
    count(map:keys($processing?by_term)) || " unique terms across " ||
    $total-documents || " documents")

(: Build final result :)
return map {
    "metadata": map {
        "volume_id": $volume-id,
        "generated": string(current-dateTime()),
        "source": "extracted_annotations",
        "total_documents": $total-documents,
        "documents_with_matches": $processing?docs_with_matches,
        "total_matches": $processing?total_matches,
        "unique_terms_matched": count(map:keys($processing?by_term)),
        "total_terms_searched": $total-taxonomy-terms,
        "terms_not_matched": array:size($unmatched-terms)
    },
    "by_document": $processing?by_document,
    "by_term": $by-term-with-variants,
    "unmatched_terms": $unmatched-terms
}
