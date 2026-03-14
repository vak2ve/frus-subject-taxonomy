xquery version "3.1";

(:~
 : Merge Annotated Documents into TEI Volume (eXist-db version)
 :
 : This is the eXist-db adapted version of merge-annotations.xq.
 : It uses eXist-db native modules (xmldb, util) instead of the EXPath file: module
 : for filesystem I/O, and reads/writes from eXist-db collections.
 :
 : It reads individual annotated document files from data/documents/{volume-id}/
 : and replaces the corresponding <div type="document"> content in the main TEI
 : volume file with the annotated version.
 :
 : Usage in eXist-db:
 :   Run via eXide or the REST API. Set $volume-id to the desired volume.
 :)

declare namespace tei = "http://www.tei-c.org/ns/1.0";
declare namespace xml = "http://www.w3.org/XML/1998/namespace";
declare namespace util = "http://exist-db.org/xquery/util";
declare namespace xmldb = "http://exist-db.org/xquery/xmldb";

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

(: TEI volume collection :)
declare variable $tei-collection as xs:string := $app-root || "/tei";

(: Primary TEI file in the collection :)
declare variable $tei-primary-path as xs:string := $tei-collection || "/" || $volume-id || ".xml";

(: Fallback path (alternative location) :)
declare variable $tei-fallback-collection as xs:string := "/db/apps/frus/volumes";
declare variable $tei-fallback-path as xs:string := $tei-fallback-collection || "/" || $volume-id || ".xml";

(: Resolve which TEI file to use :)
declare variable $tei-file-path as xs:string :=
    if (doc-available($tei-primary-path))
    then $tei-primary-path
    else if (doc-available($tei-fallback-path))
    then $tei-fallback-path
    else error(
        xs:QName("merge:FILE_NOT_FOUND"),
        "TEI file not found at " || $tei-primary-path || " or " || $tei-fallback-path
    );

(: Documents collection :)
declare variable $docs-collection as xs:string := $app-root || "/data/documents/" || $volume-id;

(: Output path :)
declare variable $output-filename as xs:string := $volume-id || "-annotated.xml";

(: ============================================================================
   BUILD ANNOTATION INDEX
   ============================================================================ :)

declare function local:build-annotation-index() as map(xs:string, element(tei:body)) {
    if (not(xmldb:collection-available($docs-collection)))
    then error(
        xs:QName("merge:DIR_NOT_FOUND"),
        "Documents collection not found: " || $docs-collection
    )
    else
        let $files :=
            for $name in xmldb:get-child-resources($docs-collection)
            where ends-with($name, ".xml")
            order by $name
            return $name
        return fold-left($files, map {}, function($acc, $filename) {
            let $doc-id := replace($filename, "\.xml$", "")
            let $doc-path := $docs-collection || "/" || $filename
            return try {
                let $doc := doc($doc-path)
                let $body := ($doc//tei:body)[1]
                return
                    if ($body)
                    then map:merge(($acc, map:entry($doc-id, $body)))
                    else (
                        util:log("WARN", "merge-annotations: SKIP - No <body> found in " || $filename),
                        $acc
                    )
            } catch * {
                util:log("ERROR", "merge-annotations: ERROR parsing " || $filename || ": " || $err:description),
                $acc
            }
        })
};

(: ============================================================================
   RECURSIVE IDENTITY TRANSFORM
   ============================================================================ :)

declare function local:transform($node as node(), $annotations as map(xs:string, element(tei:body))) as node()* {
    typeswitch ($node)
        case document-node() return
            document { $node/node() ! local:transform(., $annotations) }

        case element(tei:div) return
            if ($node/@type = "document" and $node/@xml:id and map:contains($annotations, string($node/@xml:id)))
            then
                let $doc-id := string($node/@xml:id)
                let $body := $annotations($doc-id)
                return element { node-name($node) } {
                    $node/@*,
                    $body/node()
                }
            else
                element { node-name($node) } {
                    $node/@*,
                    $node/node() ! local:transform(., $annotations)
                }

        case element() return
            element { node-name($node) } {
                $node/@*,
                $node/node() ! local:transform(., $annotations)
            }

        case comment() return $node
        case processing-instruction() return $node
        case text() return $node

        default return $node
};

(: ============================================================================
   MAIN
   ============================================================================ :)

let $_ := util:log("INFO", "merge-annotations: Volume: " || $volume-id)
let $_ := util:log("INFO", "merge-annotations: TEI file: " || $tei-file-path)
let $_ := util:log("INFO", "merge-annotations: Documents collection: " || $docs-collection)

(: Build the annotation index :)
let $annotations := local:build-annotation-index()
let $annotation-count := map:size($annotations)
let $_ := util:log("INFO", "merge-annotations: Loaded " || $annotation-count || " annotated documents")

(: Parse the main TEI file :)
let $tei-doc := doc($tei-file-path)

(: Count document divs in the main TEI :)
let $doc-div-count := count($tei-doc//tei:div[@type = "document"][@xml:id])
let $_ := util:log("INFO", "merge-annotations: Found " || $doc-div-count || " document divs in main TEI")

(: Apply the transform :)
let $result := local:transform($tei-doc, $annotations)

(: Write output to eXist-db collection :)
let $_ := xmldb:store($tei-collection, $output-filename, $result)
let $_ := util:log("INFO", "merge-annotations: Wrote annotated TEI to: " || $tei-collection || "/" || $output-filename)

return
    <merge-result>
        <volume>{$volume-id}</volume>
        <tei-source>{$tei-file-path}</tei-source>
        <documents-collection>{$docs-collection}</documents-collection>
        <output>{$tei-collection}/{$output-filename}</output>
        <document-divs>{$doc-div-count}</document-divs>
        <annotations-loaded>{$annotation-count}</annotations-loaded>
    </merge-result>
