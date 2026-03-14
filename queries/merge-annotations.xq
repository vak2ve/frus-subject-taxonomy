xquery version "3.1";

(:~
 : Merge Annotated Documents into TEI Volume
 :
 : This standalone XQuery script replicates the logic of scripts/merge_annotations.py.
 : It reads individual annotated document files from data/documents/{volume-id}/
 : and replaces the corresponding <div type="document"> content in the main TEI
 : volume file with the annotated version.
 :
 : Usage:
 :   Set the $volume-id external variable to the desired volume identifier.
 :   The script will:
 :     1. Read the main TEI volume file from tei/{volume-id}.xml
 :        (falling back to ../frus/volumes/{volume-id}.xml)
 :     2. Read annotated document files from data/documents/{volume-id}/
 :     3. Replace each document div's body content with the annotated version
 :     4. Write the result to tei/{volume-id}-annotated.xml
 :)

declare namespace tei = "http://www.tei-c.org/ns/1.0";
declare namespace xml = "http://www.w3.org/XML/1998/namespace";
declare namespace file = "http://expath.org/ns/file";

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

(: Resolve the main TEI volume file path :)
declare variable $tei-primary-path := file:resolve-path("tei/" || $volume-id || ".xml", $base-dir);
declare variable $tei-fallback-path := file:resolve-path("../frus/volumes/" || $volume-id || ".xml", $base-dir);

declare variable $tei-file-path :=
    if (file:exists($tei-primary-path))
    then $tei-primary-path
    else if (file:exists($tei-fallback-path))
    then $tei-fallback-path
    else error(
        xs:QName("merge:FILE_NOT_FOUND"),
        "TEI file not found at " || $tei-primary-path || " or " || $tei-fallback-path
    );

(: Documents directory :)
declare variable $docs-dir := file:resolve-path("data/documents/" || $volume-id || "/", $base-dir);

(: Output path :)
declare variable $output-path := file:resolve-path("tei/" || $volume-id || "-annotated.xml", $base-dir);

(: ============================================================================
   BUILD ANNOTATION INDEX
   ============================================================================
   Read all annotated document files and build a map keyed by document ID
   (filename without .xml extension, e.g., "d1"). Each entry holds the
   children of the <body> element from the annotated file.
   ============================================================================ :)

declare function local:build-annotation-index() as map(xs:string, element(tei:body)) {
    if (not(file:is-dir($docs-dir)))
    then error(
        xs:QName("merge:DIR_NOT_FOUND"),
        "Documents directory not found: " || $docs-dir
    )
    else
        let $files :=
            for $name in file:list($docs-dir)
            where ends-with($name, ".xml")
            order by $name
            return $name
        return fold-left($files, map {}, function($acc, $filename) {
            let $doc-id := replace($filename, "\.xml$", "")
            let $doc-path := $docs-dir || $filename
            return try {
                let $doc := doc($doc-path)
                let $body := ($doc//tei:body)[1]
                return
                    if ($body)
                    then map:merge(($acc, map:entry($doc-id, $body)))
                    else (
                        trace("SKIP: No <body> found in " || $filename, "merge"),
                        $acc
                    )
            } catch * {
                trace("ERROR parsing " || $filename || ": " || $err:description, "merge"),
                $acc
            }
        })
};

(: ============================================================================
   RECURSIVE IDENTITY TRANSFORM
   ============================================================================
   Copy the entire tree node-by-node. When a div[@type="document"] is
   encountered and a corresponding annotated file exists, replace the
   div's children with the annotated body's children while preserving the
   div's original attributes.
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

let $_ := trace("Volume: " || $volume-id, "merge")
let $_ := trace("TEI file: " || $tei-file-path, "merge")
let $_ := trace("Documents dir: " || $docs-dir, "merge")

(: Build the annotation index :)
let $annotations := local:build-annotation-index()
let $annotation-count := map:size($annotations)
let $_ := trace("Loaded " || $annotation-count || " annotated documents", "merge")

(: Parse the main TEI file :)
let $tei-doc := doc($tei-file-path)

(: Count document divs in the main TEI :)
let $doc-div-count := count($tei-doc//tei:div[@type = "document"][@xml:id])
let $_ := trace("Found " || $doc-div-count || " document divs in main TEI", "merge")

(: Apply the transform :)
let $result := local:transform($tei-doc, $annotations)

(: Write output :)
let $serialization-params :=
    <output:serialization-parameters xmlns:output="http://www.w3.org/2010/xslt-xquery-serialization">
        <output:method value="xml"/>
        <output:encoding value="UTF-8"/>
        <output:indent value="no"/>
        <output:omit-xml-declaration value="no"/>
    </output:serialization-parameters>

let $_ := file:write($output-path, $result, $serialization-params)
let $_ := trace("Wrote annotated TEI to: " || $output-path, "merge")

return
    <merge-result>
        <volume>{$volume-id}</volume>
        <tei-source>{$tei-file-path}</tei-source>
        <documents-dir>{$docs-dir}</documents-dir>
        <output>{$output-path}</output>
        <document-divs>{$doc-div-count}</document-divs>
        <annotations-loaded>{$annotation-count}</annotations-loaded>
    </merge-result>
