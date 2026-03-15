xquery version "3.1";

(:~
 : Split a Monolithic FRUS TEI/XML Volume into Individual Document Files (eXist-db version)
 :
 : This is the eXist-db adapted version of split-volume.xq.
 : It uses eXist-db native modules (xmldb, util) instead of the EXPath file: module,
 : and reads/writes from eXist-db collections.
 :
 : Reads a monolithic TEI volume from the volumes collection and stores each
 : historical document (<div type="document">) as a separate resource d<N>.xml
 : in the documents/<volume-id>/ collection.
 :
 : Each output resource is a well-formed TEI XML fragment wrapped in a minimal
 : <TEI> / <text> / <body> envelope so it can be parsed independently by
 : downstream pipeline queries.
 :
 : Usage in eXist-db:
 :   Run via eXide or the REST API. Set $volume-id to the desired volume.
 :)

declare namespace tei = "http://www.tei-c.org/ns/1.0";
declare namespace frus = "http://history.state.gov/frus/ns/1.0";
declare namespace util = "http://exist-db.org/xquery/util";
declare namespace xmldb = "http://exist-db.org/xquery/xmldb";
declare namespace output = "http://www.w3.org/2010/xslt-xquery-serialization";

(: ── Configuration ───────────────────────────────────────────────────── :)

(: Root collection for the hsg-annotate-data app in eXist-db :)
declare variable $app-root as xs:string := "/db/apps/hsg-annotate-data";

(: Volume ID to process (required) :)
declare variable $volume-id as xs:string external;

(: ── Derived paths (eXist-db collections) ────────────────────────────── :)

declare variable $volume-path as xs:string := $app-root || "/volumes/" || $volume-id || ".xml";
declare variable $output-collection as xs:string := $app-root || "/data/documents/" || $volume-id;

(: ── Main logic ───────────────────────────────────────────────────────── :)

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

(:~
 : Ensure the output collection exists, creating parent collections as needed.
 :)
declare function local:ensure-collection($path as xs:string) {
    if (xmldb:collection-available($path)) then ()
    else
        let $parts := tokenize($path, "/")[. ne ""]
        let $_ :=
            for $i in 1 to count($parts)
            let $current := "/" || string-join(subsequence($parts, 1, $i), "/")
            return
                if (xmldb:collection-available($current)) then ()
                else
                    let $parent := "/" || string-join(subsequence($parts, 1, $i - 1), "/")
                    let $name := $parts[$i]
                    return xmldb:create-collection($parent, $name)
        return ()
};

let $_ := local:ensure-collection($output-collection)

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
    let $filename := $id || ".xml"
    let $wrapped := local:wrap-document($doc)
    return (
        xmldb:store($output-collection, $filename, $wrapped),
        $id
    ),

    "Split " || $volume-id || ": " || $count || " documents stored in " || $output-collection
)
