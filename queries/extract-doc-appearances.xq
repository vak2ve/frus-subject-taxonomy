xquery version "3.1";

(:~
 : Extract Document Appearances for Subject Annotations
 :
 : Replicates the logic of scripts/extract_doc_appearances.py in XQuery 3.1.
 :
 : For each annotated volume (*-annotated.xml), finds all tei:rs elements with
 : @type="topic" or @type="compound-subject" within tei:div[@type="document"]
 : elements, and builds a mapping of rec_id -> {volume_id -> [doc_ids]}.
 :
 : Output: JSON with structure:
 :   { "recXXX": { "frus1977-80v11p1": ["d1", "d5", "d10"], ... }, ... }
 :
 : Usage with BaseX:
 :   basex -b input-dir=/path/to/frus-subject-taxonomy extract-doc-appearances.xq
 :
 : Usage with Saxon (EE, with EXPath file module):
 :   java -cp saxon-ee.jar net.sf.saxon.Query -q:extract-doc-appearances.xq input-dir=..
 :
 : The input-dir parameter should point to the directory containing *-annotated.xml files.
 : Defaults to ".." (parent directory, assuming this script lives in queries/).
 :)

declare namespace tei = "http://www.tei-c.org/ns/1.0";
declare namespace file = "http://expath.org/ns/file";
declare namespace output = "http://www.w3.org/2010/xslt-xquery-serialization";

declare option output:method "json";

(: Input directory containing *-annotated.xml files :)
declare variable $input-dir as xs:string external := "..";

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
 : Uses the root TEI element's @xml:id, falling back to the filename.
 :)
declare function local:volume-id($doc as document-node(), $filename as xs:string) as xs:string {
    let $root-id := $doc/*/@xml:id/string()
    return
        if ($root-id)
        then $root-id
        else replace($filename, "-annotated\.xml$", "")
};

(:~
 : Process a single annotated volume document.
 : Returns a sequence of maps: { "rec-id": ..., "volume-id": ..., "doc-id": ... }
 :)
declare function local:process-volume($doc as document-node(), $filename as xs:string) as map(*)* {
    let $volume-id := local:volume-id($doc, $filename)
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
 : Main logic: read all *-annotated.xml files, extract annotations,
 : group by rec-id and volume-id, and produce sorted JSON output.
 :)
let $dir := if (ends-with($input-dir, "/")) then $input-dir else $input-dir || "/"

(: List all *-annotated.xml files in the input directory :)
let $all-files := file:list($dir, false(), "*-annotated.xml")

(: Parse each file and extract annotation triples :)
let $triples :=
    for $filename in $all-files
    let $filepath := $dir || $filename
    let $doc := doc($filepath)
    return local:process-volume($doc, $filename)

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

return $result-map
