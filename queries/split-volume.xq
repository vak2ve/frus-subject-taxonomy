xquery version "3.1";

(:~
 : Split a Monolithic FRUS TEI/XML Volume into Individual Document Files
 :
 : Reads a monolithic TEI volume from volumes/<volume-id>.xml and writes each
 : historical document (<div type="document">) as a separate file d<N>.xml into
 : data/documents/<volume-id>/.
 :
 : Each output file is a well-formed TEI XML fragment wrapped in a minimal
 : <TEI> / <text> / <body> envelope so it can be parsed independently by
 : annotate_documents.py and other pipeline scripts.
 :
 : Usage with BaseX:
 :   basex -b volume-id=frus1969-76v19p2 split-volume.xq
 :   basex -b volume-id=frus1969-76v19p2 -b base-dir=/path/to/frus-subject-taxonomy split-volume.xq
 :
 : The base-dir parameter should point to the root of the frus-subject-taxonomy directory.
 : Defaults to ".." (parent directory, assuming this script lives in queries/).
 :)

declare namespace tei = "http://www.tei-c.org/ns/1.0";
declare namespace frus = "http://history.state.gov/frus/ns/1.0";
declare namespace file = "http://expath.org/ns/file";
declare namespace output = "http://www.w3.org/2010/xslt-xquery-serialization";

(: ── External variables ───────────────────────────────────────────────────── :)

(: Volume ID to process (required) :)
declare variable $volume-id as xs:string external;

(: Base directory of the frus-subject-taxonomy project :)
declare variable $base-dir as xs:string external := "..";

(: ── Derived paths ────────────────────────────────────────────────────────── :)

declare variable $base as xs:string :=
    if (ends-with($base-dir, "/")) then $base-dir else $base-dir || "/";

declare variable $volume-path as xs:string := $base || "volumes/" || $volume-id || ".xml";
declare variable $output-dir as xs:string := $base || "data/documents/" || $volume-id || "/";

(: ── Serialization options for output files ───────────────────────────────── :)

declare variable $ser-params :=
    <output:serialization-parameters>
        <output:method value="xml"/>
        <output:indent value="yes"/>
        <output:omit-xml-declaration value="no"/>
    </output:serialization-parameters>;

(: ── Main logic ───────────────────────────────────────────────────────────── :)

(:~
 : Wrap a document div in a minimal TEI envelope so it can be parsed
 : as a standalone TEI document.
 :)
declare function local:wrap-document($div as element(tei:div)) as element() {
    <TEI xmlns="http://www.tei-c.org/ns/1.0"
         xmlns:frus="http://history.state.gov/frus/ns/1.0">
        <text>
            <body>
                { $div }
            </body>
        </text>
    </TEI>
};

let $_ := (
    (: Verify volume file exists :)
    if (not(file:exists($volume-path))) then
        error(xs:QName("local:not-found"),
              "Volume file not found: " || $volume-path)
    else (),

    (: Create output directory if needed :)
    if (not(file:exists($output-dir))) then
        file:create-dir($output-dir)
    else ()
)

let $vol := doc($volume-path)

(: Find all historical document divs :)
let $docs := $vol//tei:div[@type = "document"][@subtype = "historical-document"]

let $count := count($docs)

return (
    if ($count = 0) then
        error(xs:QName("local:no-docs"),
              "No historical-document divs found in " || $volume-id)
    else (),

    for $doc in $docs
    let $id := $doc/@xml:id/string()
    (: Use the xml:id as filename (e.g., "d1" -> "d1.xml") :)
    let $filename := $id || ".xml"
    let $filepath := $output-dir || $filename
    let $wrapped := local:wrap-document($doc)
    return (
        file:write($filepath, $wrapped, $ser-params),
        $id
    ),

    "Split " || $volume-id || ": " || $count || " documents written to " || $output-dir
)
