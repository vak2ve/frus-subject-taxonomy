xquery version "3.1";

(:~
 : Extract Document Appearances for Subject Annotations (eXist-db version)
 :
 : This is the eXist-db adapted version of ../extract-doc-appearances.xq.
 : It uses eXist-db native modules (xmldb, util) instead of the EXPath file: module.
 :
 : For each annotated volume (*-annotated.xml) in the TEI collection, finds all
 : tei:rs elements with @type="topic" or @type="compound-subject" within
 : tei:div[@type="document"] elements, and builds a mapping of
 : rec_id -> {volume_id -> [doc_ids]}.
 :
 : Output: JSON stored to the data collection, with structure:
 :   { "recXXX": { "frus1977-80v11p1": ["d1", "d5", "d10"], ... }, ... }
 :
 : Usage in eXist-db:
 :   Deploy to eXist-db and run via the Java admin client or eXide.
 :)

declare namespace tei = "http://www.tei-c.org/ns/1.0";
declare namespace util = "http://exist-db.org/xquery/util";
declare namespace xmldb = "http://exist-db.org/xquery/xmldb";
declare namespace output = "http://www.w3.org/2010/xslt-xquery-serialization";

declare option output:method "json";

(: ── Configuration ─────────────────────────────────────────────────────── :)

(: Root collection for the hsg-annotate-data app :)
declare variable $app-root as xs:string := "/db/apps/hsg-annotate-data";

(: Collection containing annotated TEI volumes :)
declare variable $annotated-volumes-collection as xs:string := $app-root || "/tei";

(: Output collection for results :)
declare variable $output-collection as xs:string := $app-root || "/data";

(: Output filename :)
declare variable $output-filename as xs:string := "document_appearances.json";

(:~
 : Natural sort key for document IDs like "d1", "d2", "d10".
 : Extracts the numeric portion so d10 sorts after d2, not before.
 :)
declare function local:doc-id-sort-key($doc-id as xs:string) as xs:integer {
    let $num := replace($doc-id, "^d(\d+).*$", "$1")
    return
        if ($num castable as xs:integer)
        then xs:integer($num)
        else 999999999
};

(:~
 : Extract the volume ID from a TEI document.
 : Uses the root TEI element's @xml:id, falling back to the resource name.
 :)
declare function local:volume-id($doc as document-node(), $resource-name as xs:string) as xs:string {
    let $root-id := $doc/*/@xml:id/string()
    return
        if ($root-id)
        then $root-id
        else replace($resource-name, "-annotated\.xml$", "")
};

(:~
 : Process a single annotated volume document.
 : Returns a sequence of maps: { "rec-id": ..., "volume-id": ..., "doc-id": ... }
 :)
declare function local:process-volume($doc as document-node(), $resource-name as xs:string) as map(*)* {
    let $volume-id := local:volume-id($doc, $resource-name)
    for $div in $doc//tei:div[@type = "document"]
    let $doc-id := $div/@xml:id/string()
    where $doc-id
    for $rs in $div//tei:rs[@type = ("topic", "compound-subject")]
    let $corresp := $rs/@corresp/string()
    where $corresp
    return map {
        "rec-id": $corresp,
        "volume-id": $volume-id,
        "doc-id": $doc-id
    }
};

(:~
 : Main logic: read all *-annotated.xml files from the eXist-db collection,
 : extract annotations, group by rec-id and volume-id, and produce sorted
 : JSON output.
 :)

let $_ := util:log("INFO", "extract-doc-appearances: Starting extraction from " || $annotated-volumes-collection)

(: Get all annotated volume documents from the collection :)
let $annotated-resources :=
    for $resource in xmldb:get-child-resources($annotated-volumes-collection)
    where ends-with($resource, "-annotated.xml")
    return $resource

let $_ := util:log("INFO", "extract-doc-appearances: Found " || count($annotated-resources) || " annotated volumes")

(: Parse each document and extract annotation triples :)
let $triples :=
    for $resource in $annotated-resources
    let $doc := doc($annotated-volumes-collection || "/" || $resource)
    return local:process-volume($doc, $resource)

(: Group by rec-id, then by volume-id, with sorted doc-ids :)
let $rec-ids := distinct-values($triples ! ?rec-id)

let $result-map := map:merge(
    for $rec-id in sort($rec-ids)
    let $rec-triples := $triples[?rec-id = $rec-id]
    let $volume-ids := distinct-values($rec-triples ! ?volume-id)
    return map:entry(
        $rec-id,
        map:merge(
            for $vol-id in sort($volume-ids)
            let $doc-ids := distinct-values(
                $rec-triples[?volume-id = $vol-id] ! ?doc-id
            )
            let $sorted-doc-ids := array {
                for $d in $doc-ids
                order by local:doc-id-sort-key($d), $d
                return $d
            }
            return map:entry($vol-id, $sorted-doc-ids)
        )
    )
)

(: Store the result as JSON in eXist-db :)
let $json-output := serialize($result-map, map { "method": "json", "indent": true() })
let $_ := xmldb:store($output-collection, $output-filename, $json-output, "application/json")
let $_ := util:log("INFO", "extract-doc-appearances: Wrote results to " || $output-collection || "/" || $output-filename)

return $result-map
