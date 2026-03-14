xquery version "3.1";

(:~
 : Build Subject Taxonomy using LCSH Broader Terms and HSG Topic Headings
 :
 : Replicates the logic of scripts/build_taxonomy_lcsh.py in XQuery 3.1.
 :
 : Reads config/lcsh_mapping.json, document_appearances.json, config/dedup_decisions.json,
 : and config/category_overrides.json to build subject-taxonomy-lcsh.xml with subjects
 : categorized into the 11 official Office of the Historian topic headings.
 :
 : Usage with BaseX:
 :   basex -b base-dir=/path/to/frus-subject-taxonomy build-taxonomy-lcsh.xq
 :   basex -b base-dir=/path/to/frus-subject-taxonomy -b fetch-hierarchies=true build-taxonomy-lcsh.xq
 :
 : The base-dir parameter should point to the repository root.
 : Set fetch-hierarchies=true to fetch LCSH broader terms from id.loc.gov (slow, uses cache).
 :)

declare namespace file = "http://expath.org/ns/file";
declare namespace http = "http://expath.org/ns/http-client";
declare namespace output = "http://www.w3.org/2010/xslt-xquery-serialization";

declare option output:method "xml";
declare option output:indent "yes";

(: ══════════════════════════════════════════════════════════════
   External parameters
   ══════════════════════════════════════════════════════════════ :)

(: Repository root directory :)
declare variable $base-dir as xs:string external := "..";

(: Whether to fetch LCSH broader terms from id.loc.gov :)
declare variable $fetch-hierarchies as xs:string external := "false";

(: ══════════════════════════════════════════════════════════════
   File paths (derived from base-dir)
   ══════════════════════════════════════════════════════════════ :)

declare variable $dir := if (ends-with($base-dir, "/")) then $base-dir else $base-dir || "/";
declare variable $MAPPING-FILE := $dir || "config/lcsh_mapping.json";
declare variable $DOC-APPEARANCES-FILE := $dir || "document_appearances.json";
declare variable $DEDUP-DECISIONS-FILE := $dir || "config/dedup_decisions.json";
declare variable $CATEGORY-OVERRIDES-FILE := $dir || "config/category_overrides.json";
declare variable $BT-CACHE-FILE := $dir || "lcsh_broader_cache.json";
declare variable $OUTPUT-FILE := $dir || "subject-taxonomy-lcsh.xml";

(: SKOS constants :)
declare variable $SKOS-URL-TEMPLATE := "https://id.loc.gov/authorities/subjects/{LCCN}.skos.json";
declare variable $SKOS-BROADER := "http://www.w3.org/2004/02/skos/core#broader";
declare variable $SKOS-PREFLABEL := "http://www.w3.org/2004/02/skos/core#prefLabel";

(: ══════════════════════════════════════════════════════════════
   HSG Taxonomy: official Office of the Historian topic headings
   with keyword lists for categorization.
   ══════════════════════════════════════════════════════════════ :)

declare variable $HSG-TAXONOMY := map {
    "Arms Control and Disarmament": map {
        "keywords": [
            "arms control", "disarmament", "nonproliferation",
            "SALT", "START", "ABM", "INF", "MBFR", "CFE",
            "anti-ballistic missile",
            "verification", "test ban", "moratorium",
            "arms limitation", "arms reduction", "arms race",
            "non-proliferation", "nuclear non-proliferation",
            "strategic arms", "strategic offensive", "strategic defenses",
            "strategic force", "strategic nuclear",
            "warhead", "missile", "ballistic", "ICBM",
            "cruise missile", "nuclear testing", "nuclear fuel",
            "mobile missile", "throw-weight", "reentry vehicle",
            "zero option", "first strike",
            "SLBM", "ALCM", "delivery vehicle",
            "backfire bomber", "gravity bomb",
            "force modernization", "on-site inspection",
            "national technical means", "counting rule",
            "interim agreement", "safeguard", "overflight",
            "SS-20", "Trident", "bomber aircraft"
        ],
        "subcategories": map {
            "Arms Embargoes": ["arms embargo"],
            "Arms Transfers": ["arms transfer", "arms sale"],
            "Chemical and Bacteriological Warfare": [
                "chemical weapon", "bacteriological warfare",
                "biological weapon", "CW", "BWC"
            ],
            "Collective Security": ["collective security"],
            "Confidence-Building Measures": ["confidence-building"],
            "Nuclear Nonproliferation": [
                "nonproliferation", "non-proliferation",
                "nuclear nonproliferation", "nuclear non-proliferation",
                "nuclear fuel", "safeguard", "IAEA"
            ],
            "Nuclear Weapons": [
                "nuclear weapon", "nuclear testing", "warhead",
                "missile", "ballistic", "ICBM",
                "strategic arms", "strategic offensive",
                "bomber aircraft", "mobile missile",
                "cruise missile", "throw-weight",
                "reentry vehicle", "re-entry vehicle",
                "SLBM", "ALCM", "delivery vehicle",
                "backfire bomber", "gravity bomb",
                "SS-20", "Trident",
                "first strike", "zero option",
                "sublimit", "counting rule"
            ]
        }
    },
    "Department of State": map {
        "keywords": [
            "department of state", "state department",
            "foreign service", "civil service",
            "ambassador", "embassy", "consulate", "consular",
            "congressional relations", "protocol",
            "protection of americans abroad",
            "locally employed staff", "visa"
        ],
        "subcategories": map {
            "Buildings: Domestic": [],
            "Buildings: Foreign": ["embassy", "consulate"],
            "Congressional Relations": ["congressional relations"],
            "Organization and Management": ["organization", "management"],
            "Personnel: Civil Service": ["civil service"],
            "Personnel: Demographics": ["demographics"],
            "Personnel: Foreign Service": ["foreign service"],
            "Personnel: Locally Employed Staff": ["locally employed"],
            "Protection of Americans Abroad": ["protection of americans"],
            "Protocol": ["protocol"]
        }
    },
    "Foreign Economic Policy": map {
        "keywords": [
            "agriculture", "economic sanction", "sanction",
            "economic summit", "energy", "natural resources",
            "financial", "monetary", "fiscal",
            "foreign aid", "foreign investment",
            "new international economic order",
            "trade", "commercial",
            "economic", "tariff", "embargo",
            "export", "import", "investment",
            "debt", "loan", "aid",
            "development aid", "development assistance",
            "food aid", "PL 480",
            "oil", "petroleum", "gas",
            "commodity", "market",
            "budget", "private sector",
            "hunger", "drought", "famine",
            "sea bed mining", "seabed mining",
            "north-south dialogue",
            "least developed countries", "developing countries"
        ],
        "subcategories": map {
            "Agriculture": [
                "agriculture", "agricultural", "food aid",
                "grain", "sugar", "food", "hunger", "drought", "famine",
                "PL 480"
            ],
            "Economic Sanctions": ["economic sanction", "sanction"],
            "Economic Summit Meetings": ["economic summit"],
            "Energy and Natural Resources": [
                "energy", "oil", "petroleum", "gas",
                "natural resources", "mining", "minerals"
            ],
            "Financial and Monetary Policy": [
                "financial", "monetary", "fiscal",
                "debt", "loan", "budget"
            ],
            "Foreign Aid": [
                "foreign aid", "development aid", "development assistance",
                "PL 480", "donor", "least developed countries"
            ],
            "Foreign Investment": ["foreign investment", "investment"],
            "Labor": ["labor"],
            "New International Economic Order": [
                "new international economic order",
                "north-south dialogue"
            ],
            "Trade and Commercial Policy/Agreements": [
                "trade", "commercial", "tariff",
                "export", "import", "commodity", "market"
            ]
        }
    },
    "Global Issues": map {
        "keywords": [
            "border administration", "decolonization",
            "election", "immigration", "narcotics", "drug",
            "outer space", "space program",
            "peace", "polar affairs",
            "population", "self-determination",
            "migration", "environment", "climate",
            "pollution", "conservation",
            "ozone", "whaling", "whale",
            "family planning", "abortion",
            "continental shelf", "seabed",
            "regional issue",
            "national independence",
            "d&#xe9;tente", "detente",
            "aviation security",
            "public health"
        ],
        "subcategories": map {
            "Air Safety": ["aviation security"],
            "Border Administration": ["border administration", "border"],
            "Decolonization": ["decolonization", "national independence"],
            "Elections": ["election"],
            "Immigration": ["immigration", "migration"],
            "Narcotics": ["narcotics", "drug"],
            "Outer Space": ["outer space", "space program"],
            "Peace": ["peace", "d&#xe9;tente", "detente"],
            "Polar Affairs": ["polar affairs", "polar"],
            "Population Demographics": [
                "population", "family planning", "abortion"
            ],
            "Public Health": ["public health"],
            "Self-Determination": ["self-determination"]
        }
    },
    "Human Rights": map {
        "keywords": [
            "human rights", "antisemitism",
            "asylum", "civil rights",
            "detainee", "disability rights",
            "genocide", "HIV", "AIDS",
            "political prisoner", "refugee",
            "discrimination", "apartheid", "persecution",
            "emigration", "dissident", "freedom",
            "liberty", "humanitarian",
            "prisoner release", "emergency relief",
            "trial", "torture", "psychiatric abuse",
            "religious freedom"
        ],
        "subcategories": map {
            "Antisemitism": [
                "antisemitism", "anti-jewish", "anti-semit",
                "pogrom", "jewish persecution"
            ],
            "Asylum": ["asylum"],
            "Chinese Exclusion Act (1882)": ["chinese exclusion"],
            "Civil Rights": ["civil rights"],
            "Detainees": ["detainee"],
            "Disability Rights": ["disability rights", "disability"],
            "Genocide": ["genocide"],
            "HIV/AIDS": ["HIV", "AIDS"],
            "Political Prisoners": [
                "political prisoner", "prisoner release",
                "dissident"
            ],
            "Refugees": ["refugee", "emergency relief"],
            "Religious Freedom": ["religious freedom"]
        }
    },
    "Information Programs": map {
        "keywords": [
            "information program", "propaganda", "media", "press",
            "broadcast", "radio", "cultural exchange",
            "public diplomacy", "USIA", "Voice of America",
            "exchange program"
        ],
        "subcategories": map {}
    },
    "International Law": map {
        "keywords": [
            "international law", "law of the sea",
            "property claims", "jurisdiction", "sovereignty",
            "protest against U.S.", "treaty", "convention",
            "legal", "compliance", "regulation",
            "legislation", "judicial",
            "extradition", "claims tribunal",
            "international court"
        ],
        "subcategories": map {
            "Domestic Protest against U.S. Activity": ["domestic protest"],
            "Foreign Protest against U.S. Activity": ["foreign protest"],
            "Law of the Sea": [
                "law of the sea", "territorial sea",
                "continental shelf", "seabed"
            ],
            "Property Claims": [
                "property claims", "claims tribunal",
                "iranian assets"
            ]
        }
    },
    "International Organizations": map {
        "keywords": [
            "United Nations", "NATO",
            "Association of Southeast Asian Nations", "ASEAN",
            "Conference on Security and Cooperation in Europe", "CSCE",
            "European Advisory Commission",
            "European Economic Community", "European Community",
            "Far Eastern Commission",
            "General Agreement on Tariffs and Trade", "GATT",
            "International Monetary Fund", "IMF",
            "League of Nations",
            "International Atomic Energy Agency", "IAEA",
            "non-governmental organization", "NGO",
            "Organization of American States", "OAS",
            "Organization of Petroleum Exporting Countries", "OPEC",
            "Southeast Asia Treaty Organization", "SEATO",
            "Universal Postal Union",
            "World Trade Organization", "WTO",
            "OAU", "OECD", "World Bank", "G-7", "G-8",
            "international organization",
            "Non-aligned Movement", "NAM"
        ],
        "subcategories": map {
            "Association of Southeast Asian Nations": ["ASEAN"],
            "Conference on Security and Cooperation in Europe": ["CSCE"],
            "European Advisory Commission": ["European Advisory Commission"],
            "European Economic Community": [
                "European Economic Community", "European Community"
            ],
            "Far Eastern Commission": ["Far Eastern Commission"],
            "General Agreement on Tariffs and Trade": ["GATT"],
            "International Monetary Fund": ["IMF"],
            "League of Nations": ["League of Nations"],
            "International Atomic Energy Agency": ["IAEA"],
            "Non-governmental Organizations": [
                "non-governmental organization", "NGO"
            ],
            "North Atlantic Treaty Organization": ["NATO"],
            "Organization of American States": ["OAS"],
            "Organization of Petroleum Exporting Countries": ["OPEC"],
            "Southeast Asia Treaty Organization": ["SEATO"],
            "United Nations": ["United Nations"],
            "Universal Postal Union": ["Universal Postal Union"],
            "World Trade Organization": ["WTO"]
        }
    },
    "Politico-Military Issues": map {
        "keywords": [
            "alliance", "armistice", "covert action", "covert operation",
            "diplomatic recognition", "military base",
            "military intervention", "military presence",
            "military withdrawal",
            "national security council", "NSC",
            "national security policy",
            "quarantine", "blockade",
            "terrorism",
            "military", "defense", "armed forces",
            "army", "navy", "air force",
            "intelligence", "CIA", "KGB",
            "security", "deterrence",
            "reconnaissance", "espionage",
            "relations", "foreign policy",
            "diplomacy", "diplomatic",
            "bilateral", "multilateral",
            "negotiation", "summit",
            "normalization", "rapprochement",
            "brigade"
        ],
        "subcategories": map {
            "Alliances": ["alliance"],
            "Armistices": ["armistice"],
            "Covert Action": ["covert action", "covert operation"],
            "Diplomatic Recognition": ["diplomatic recognition"],
            "Military Bases": ["military base"],
            "Military Intervention, Presence and Withdrawal": [
                "military intervention", "military presence",
                "military withdrawal"
            ],
            "National Security Council": [
                "national security council", "NSC"
            ],
            "National Security Policy": ["national security policy"],
            "Quarantine (Blockade)": ["quarantine", "blockade"],
            "Terrorism": ["terrorism"]
        }
    },
    "Science and Technology": map {
        "keywords": [
            "science", "technology", "research",
            "atomic energy", "nuclear energy",
            "telecommunications", "computer", "satellite",
            "space science", "biodiversity", "fisheries",
            "ocean", "maritime"
        ],
        "subcategories": map {
            "Atomic Energy": ["atomic energy", "nuclear energy"]
        }
    },
    "Warfare": map {
        "keywords": [
            "war", "conflict", "ceasefire", "hostilities",
            "invasion", "occupation",
            "insurgency", "guerrilla",
            "hostage", "coup", "crisis",
            "peacekeeping", "mediation",
            "prisoners of war", "war crimes", "neutrality",
            "Korean War", "Vietnam", "World War",
            "Arab-Israeli", "Cuban Missile Crisis",
            "Geneva Convention",
            "dispute"
        ],
        "subcategories": map {
            "Afghanistan Conflict (2001)": ["Afghanistan conflict", "Afghanistan war"],
            "American Revolutionary War": [
                "American Revolution", "Revolutionary War",
                "war of independence"
            ],
            "Arab-Israeli Dispute": ["Arab-Israeli"],
            "Civil War (U.S.)": ["Civil War", "Confedera"],
            "Cuban Missile Crisis": ["Cuban Missile Crisis"],
            "Geneva Convention": ["Geneva Convention"],
            "Iraq War (2003)": ["Iraq war", "Iraq conflict"],
            "Korean War": ["Korean War"],
            "Mexican-American War": ["Mexican-American War", "Mexican War"],
            "Neutrality": ["neutrality"],
            "Prisoners of War": ["prisoners of war"],
            "Spanish-American War": ["Spanish-American War"],
            "Suez Canal": ["Suez Canal", "Suez Crisis"],
            "Vietnam Conflict": ["Vietnam"],
            "War Crimes and War Criminals": ["war crimes", "war criminal"],
            "War of 1812": ["War of 1812"],
            "World War I": ["World War I"],
            "World War II": ["World War II"]
        }
    },
    "Bilateral Relations": map {
        "keywords": [
            "bilateral relations", "bilateral issues",
            "east-west relations", "diplomatic relations"
        ],
        "subcategories": map {
            "U.S.-Soviet/Russian Relations": [
                "soviet union relations", "soviet union bilateral",
                "soviet union trade relations", "soviet cultural exchanges"
            ],
            "NATO and European Relations": [
                "nato relations", "germany relations", "west germany relations",
                "spain relations", "poland relations", "norwegian relations"
            ],
            "East Asian Relations": [
                "china relations", "japan relations",
                "chinese relationship", "china normalization"
            ],
            "South Asian Relations": [
                "india relations", "indo-pak", "sino-indian"
            ],
            "Middle East and North African Relations": [
                "iran relations", "libya relations",
                "algeria relations", "morocco relations",
                "tunisia relations", "egypt relations",
                "israeli", "iraq relations",
                "saudi arabia relations", "turkey relations"
            ],
            "Western Hemisphere Relations": [
                "cuba relations", "mexico relations",
                "jamaica relations", "guyana relations",
                "haiti relations", "barbados relations",
                "dominican republic relations", "bahamas relations",
                "grenada relations", "trinidad relations",
                "dominica relations", "latin america"
            ],
            "Sub-Saharan African Relations": [
                "ethiopian relations"
            ]
        }
    }
};

(: Category names in display order :)
declare variable $CATEGORY-NAMES := map:keys($HSG-TAXONOMY);

(: ══════════════════════════════════════════════════════════════
   Utility functions
   ══════════════════════════════════════════════════════════════ :)

(:~
 : Read a JSON file and return its parsed content as a map/array.
 : Returns an empty map if the file does not exist.
 :)
declare function local:read-json-file($path as xs:string) as item()? {
    if (file:exists($path))
    then json-doc($path)
    else ()
};

(:~
 : Compute keyword score: for each keyword, check if it appears (case-insensitive)
 : in any of the search texts. Score += length of keyword for each match.
 : This mirrors the Python _keyword_score function.
 :)
declare function local:keyword-score(
    $keywords as xs:string*,
    $texts as xs:string*
) as xs:integer {
    let $lower-texts := for $t in $texts return lower-case($t)
    return
        sum(
            for $kw in $keywords
            let $kw-lower := lower-case($kw)
            return
                if (some $text in $lower-texts satisfies contains($text, $kw-lower))
                then string-length($kw)
                else 0
        )
};

(:~
 : Get all keywords for a category, including subcategory keywords.
 : This ensures subjects matching a subcategory also match the parent.
 :)
declare function local:all-category-keywords($cat-data as map(*)) as xs:string* {
    let $main-kws := array:flatten($cat-data?keywords)
    let $sub-map := $cat-data?subcategories
    let $sub-kws :=
        for $sub-name in map:keys($sub-map)
        return array:flatten($sub-map($sub-name))
    return ($main-kws, $sub-kws)
};

(:~
 : Categorize a subject into an HSG topic category and subcategory.
 : Returns a map with "category" and "subcategory" keys.
 : If no match, returns map with category = () (empty).
 :)
declare function local:categorize-by-hsg(
    $name as xs:string,
    $lcsh-label as xs:string?
) as map(*) {
    let $texts := ($name, $lcsh-label[. ne ""])

    (: Step 1: find best top-level category :)
    let $scored-cats :=
        for $cat-name in map:keys($HSG-TAXONOMY)
        let $cat-data := $HSG-TAXONOMY($cat-name)
        let $all-kw := local:all-category-keywords($cat-data)
        let $score := local:keyword-score($all-kw, $texts)
        where $score gt 0
        order by $score descending
        return map { "name": $cat-name, "score": $score }

    return
        if (empty($scored-cats))
        then map { "category": (), "subcategory": () }
        else
            let $best-cat := $scored-cats[1]?name
            let $cat-data := $HSG-TAXONOMY($best-cat)
            let $sub-map := $cat-data?subcategories

            (: Step 2: find best subcategory within matched category :)
            let $scored-subs :=
                for $sub-name in map:keys($sub-map)
                let $sub-kws := array:flatten($sub-map($sub-name))
                let $score := local:keyword-score($sub-kws, $texts)
                where $score gt 0
                order by $score descending
                return map { "name": $sub-name, "score": $score }

            let $best-sub :=
                if (empty($scored-subs))
                then "General"
                else $scored-subs[1]?name

            return map { "category": $best-cat, "subcategory": $best-sub }
};

(:~
 : Get the count for a subject entry, defaulting to 0.
 :)
declare function local:get-count($data as map(*)) as xs:integer {
    let $c := $data?count
    return
        if (empty($c)) then 0
        else if ($c instance of xs:integer) then $c
        else if ($c castable as xs:integer) then xs:integer($c)
        else 0
};

(:~
 : Get the volumes count for a subject entry.
 :)
declare function local:get-volumes($data as map(*)) as xs:string {
    let $v := $data?volumes
    return
        if (empty($v)) then ""
        else string($v)
};

(:~
 : Split a comma-separated string into a sequence of trimmed tokens.
 :)
declare function local:split-csv($s as xs:string?) as xs:string* {
    if (empty($s) or $s eq "")
    then ()
    else
        for $token in tokenize($s, ",\s*")
        let $trimmed := normalize-space($token)
        where $trimmed ne ""
        return $trimmed
};

(: ══════════════════════════════════════════════════════════════
   LCSH hierarchy fetching (optional)
   ══════════════════════════════════════════════════════════════ :)

(:~
 : Fetch label and broader term URIs for an LCSH URI via SKOS JSON API.
 : Returns a map { "label": xs:string?, "broader_uris": array(*) }
 :)
declare function local:fetch-label-and-broader($uri as xs:string) as map(*) {
    let $lccn := tokenize(replace($uri, "/+$", ""), "/")[last()]
    let $skos-url := replace($SKOS-URL-TEMPLATE, "\{LCCN\}", $lccn)
    return
        try {
            let $response := http:send-request(
                <http:request method="GET" href="{$skos-url}">
                    <http:header name="Accept" value="application/json"/>
                </http:request>
            )
            let $status := $response[1]/@status/string()
            return
                if ($status eq "200")
                then
                    let $body := $response[2]
                    let $data :=
                        if ($body instance of xs:string) then parse-json($body)
                        else if ($body instance of document-node()) then parse-json(serialize($body))
                        else parse-json(string($body))
                    return
                        if ($data instance of array(*))
                        then
                            let $items := array:flatten($data)
                            let $matching-item :=
                                (for $item in $items
                                 where $item instance of map(*)
                                   and replace($item?("@id"), "/+$", "") eq replace($uri, "/+$", "")
                                 return $item
                                )[1]
                            return
                                if (exists($matching-item))
                                then
                                    let $pref-labels := $matching-item($SKOS-PREFLABEL)
                                    let $label :=
                                        if ($pref-labels instance of array(*))
                                        then
                                            let $first := array:get($pref-labels, 1)
                                            return
                                                if ($first instance of map(*)) then $first?("@value")
                                                else string($first)
                                        else if ($pref-labels instance of map(*))
                                        then $pref-labels?("@value")
                                        else string($pref-labels)

                                    let $bt-raw := $matching-item($SKOS-BROADER)
                                    let $bt-list :=
                                        if ($bt-raw instance of array(*)) then array:flatten($bt-raw)
                                        else if (exists($bt-raw)) then $bt-raw
                                        else ()
                                    let $broader-uris :=
                                        for $bt in $bt-list
                                        where $bt instance of map(*) and exists($bt?("@id"))
                                          and contains($bt?("@id"), "authorities/subjects")
                                        return $bt?("@id")
                                    return map {
                                        "label": $label,
                                        "broader_uris": array { $broader-uris }
                                    }
                                else map { "label": (), "broader_uris": array {} }
                        else map { "label": (), "broader_uris": array {} }
                else map { "label": (), "broader_uris": array {} }
        } catch * {
            map { "label": (), "broader_uris": array {} }
        }
};

(:~
 : Fetch two-level LCSH broader-term hierarchy for all subjects with LCSH URIs.
 : Uses and updates the broader-term cache file.
 : Returns the updated cache as a map.
 :)
declare function local:fetch-two-level-hierarchy(
    $mapping as map(*),
    $bt-cache as map(*)
) as map(*) {
    (: Collect URIs needing fetch :)
    let $uris-to-fetch :=
        distinct-values(
            for $ref in map:keys($mapping)
            let $data := $mapping($ref)
            let $quality := ($data?match_quality, "no_match")[1]
            where $quality = ("exact", "good_close")
              and exists($data?lcsh_uri)
              and not(map:contains($bt-cache, $data?lcsh_uri))
            return string($data?lcsh_uri)
        )

    (: Level 1: fetch broader terms for each subject URI :)
    let $level1-cache := fold-left($uris-to-fetch, $bt-cache,
        function($cache, $uri) {
            if (map:contains($cache, $uri))
            then $cache
            else
                let $result := local:fetch-label-and-broader($uri)
                return map:merge(($cache, map:entry($uri, $result)))
        }
    )

    (: Collect level-2 URIs :)
    let $level2-uris :=
        distinct-values(
            for $uri in $uris-to-fetch
            let $entry := $level1-cache($uri)
            where exists($entry) and $entry instance of map(*)
            for $bt-uri in array:flatten($entry?broader_uris)
            where not(map:contains($level1-cache, $bt-uri))
            return string($bt-uri)
        )

    (: Level 2: fetch broader terms for BT entries :)
    let $level2-cache := fold-left($level2-uris, $level1-cache,
        function($cache, $uri) {
            if (map:contains($cache, $uri))
            then $cache
            else
                let $result := local:fetch-label-and-broader($uri)
                return map:merge(($cache, map:entry($uri, $result)))
        }
    )

    return $level2-cache
};

(: ══════════════════════════════════════════════════════════════
   Deduplication
   ══════════════════════════════════════════════════════════════ :)

(:~
 : Merge appears_in volume lists from multiple entries.
 :)
declare function local:merge-appears-in($entries as map(*)*) as xs:string {
    let $all-vols :=
        distinct-values(
            for $d in $entries
            return local:split-csv($d?appears_in)
        )
    return string-join(sort($all-vols), ", ")
};

(:~
 : Merge document_appearances maps from multiple entries.
 : Each entry's document_appearances is a map of vol-id -> array of doc-ids.
 :)
declare function local:merge-doc-appearances($entries as map(*)*) as map(*) {
    let $all-vol-ids :=
        distinct-values(
            for $d in $entries
            let $da := $d?document_appearances
            where exists($da) and $da instance of map(*)
            return map:keys($da)
        )
    return map:merge(
        for $vol-id in $all-vol-ids
        let $all-docs :=
            distinct-values(
                for $d in $entries
                let $da := $d?document_appearances
                where exists($da) and $da instance of map(*) and map:contains($da, $vol-id)
                let $docs := $da($vol-id)
                return
                    if ($docs instance of array(*)) then array:flatten($docs)
                    else string($docs)
            )
        return map:entry($vol-id, array { sort($all-docs) })
    )
};

(:~
 : Choose best LCSH match from a sequence of entries.
 : Prefers exact > good_close.
 :)
declare function local:best-lcsh-match($entries as map(*)*) as map(*) {
    let $exact :=
        (for $d in $entries
         where $d?match_quality eq "exact" and exists($d?lcsh_uri) and string($d?lcsh_uri) ne ""
         return $d
        )[1]
    let $good-close :=
        (for $d in $entries
         where $d?match_quality eq "good_close" and exists($d?lcsh_uri) and string($d?lcsh_uri) ne ""
         return $d
        )[1]
    let $best := ($exact, $good-close)[1]
    return
        if (exists($best))
        then map {
            "lcsh_uri": $best?lcsh_uri,
            "lcsh_label": ($best?lcsh_label, "")[1],
            "match_quality": $best?match_quality,
            "exact_match": ($best?match_quality eq "exact")
        }
        else map {}
};

(:~
 : Apply global dedup decisions from dedup_decisions.json.
 : Returns updated mapping with secondary refs merged into primaries.
 :)
declare function local:apply-dedup-decisions($mapping as map(*)) as map(*) {
    let $decisions-raw := local:read-json-file($DEDUP-DECISIONS-FILE)
    return
        if (empty($decisions-raw))
        then $mapping
        else
            let $decisions :=
                if ($decisions-raw instance of map(*))
                then $decisions-raw
                else map {}
            let $merge-groups-raw := $decisions?merge
            let $merge-groups :=
                if ($merge-groups-raw instance of array(*))
                then array:flatten($merge-groups-raw)
                else ()
            return
                if (empty($merge-groups))
                then $mapping
                else
                    fold-left($merge-groups, $mapping,
                        function($current-mapping, $group) {
                            let $primary-ref := string($group?primary_ref)
                            let $all-refs-raw := $group?all_refs
                            let $all-refs :=
                                if ($all-refs-raw instance of array(*))
                                then for $r in array:flatten($all-refs-raw) return string($r)
                                else string($all-refs-raw)
                            let $secondary-refs := $all-refs[. ne $primary-ref]
                            return
                                if (not(map:contains($current-mapping, $primary-ref)))
                                then $current-mapping
                                else
                                    (: Collect all entries that exist :)
                                    let $entries :=
                                        for $ref in $all-refs
                                        where map:contains($current-mapping, $ref)
                                        return $current-mapping($ref)
                                    return
                                        if (count($entries) le 1)
                                        then $current-mapping
                                        else
                                            let $primary-data := $current-mapping($primary-ref)
                                            let $combined-count := sum(for $d in $entries return local:get-count($d))
                                            let $merged-appears := local:merge-appears-in($entries)
                                            let $merged-docs := local:merge-doc-appearances($entries)
                                            let $lcsh-data := local:best-lcsh-match($entries)
                                            let $vol-count := count(local:split-csv($merged-appears))

                                            let $updated-primary := map:merge((
                                                $primary-data,
                                                map {
                                                    "count": $combined-count,
                                                    "appears_in": $merged-appears,
                                                    "volumes": $vol-count,
                                                    "document_appearances": $merged-docs,
                                                    "merged_refs": array { $all-refs }
                                                },
                                                $lcsh-data
                                            ), map { "duplicates": "use-last" })

                                            (: Remove secondary refs, update primary :)
                                            let $without-secondary := fold-left($secondary-refs, $current-mapping,
                                                function($m, $ref) {
                                                    if (map:contains($m, $ref))
                                                    then map:remove($m, $ref)
                                                    else $m
                                                }
                                            )
                                            return map:merge((
                                                $without-secondary,
                                                map:entry($primary-ref, $updated-primary)
                                            ), map { "duplicates": "use-last" })
                        }
                    )
};

(: ══════════════════════════════════════════════════════════════
   Category overrides
   ══════════════════════════════════════════════════════════════ :)

(:~
 : Load category overrides from config file.
 : Returns a map of ref -> map { "to_category", "to_subcategory" }
 :)
declare function local:load-category-overrides() as map(*) {
    let $raw := local:read-json-file($CATEGORY-OVERRIDES-FILE)
    return
        if (empty($raw))
        then map {}
        else if ($raw instance of array(*))
        then map:merge(
            for $entry in array:flatten($raw)
            where $entry instance of map(*) and exists($entry?ref)
            return map:entry(
                string($entry?ref),
                map {
                    "to_category": string($entry?to_category),
                    "to_subcategory": string($entry?to_subcategory)
                }
            )
        )
        else map {}
};

(: ══════════════════════════════════════════════════════════════
   Build taxonomy XML
   ══════════════════════════════════════════════════════════════ :)

(:~
 : Build a <subject> element for the taxonomy.
 :)
declare function local:build-subject-element(
    $ref as xs:string,
    $data as map(*)
) as element(subject) {
    let $count := local:get-count($data)
    let $volumes := local:get-volumes($data)
    let $name := string(($data?name, "")[1])
    let $subject-type := string(($data?type, "topic")[1])
    let $match-quality := string(($data?match_quality, "")[1])
    let $lcsh-uri := string(($data?lcsh_uri, "")[1])
    let $lcsh-label := string(($data?lcsh_label, "")[1])
    let $appears-in := string(($data?appears_in, "")[1])
    let $doc-apps := $data?document_appearances
    return
        element subject {
            attribute ref { $ref },
            attribute type { $subject-type },
            attribute count { $count },
            attribute volumes { $volumes },
            if ($lcsh-uri ne "" and $match-quality = ("exact", "good_close"))
            then (
                attribute lcsh-uri { $lcsh-uri },
                attribute lcsh-match { $match-quality }
            )
            else (),

            element name { $name },

            if ($lcsh-label ne "" and $lcsh-label ne $name
                and $match-quality = ("exact", "good_close"))
            then element lcsh-authorized-form { $lcsh-label }
            else (),

            if ($appears-in ne "")
            then element appearsIn { $appears-in }
            else (),

            if (exists($doc-apps) and $doc-apps instance of map(*) and map:size($doc-apps) gt 0)
            then
                element documents {
                    for $vol-id in sort(map:keys($doc-apps))
                    let $doc-ids := $doc-apps($vol-id)
                    let $doc-strings :=
                        if ($doc-ids instance of array(*))
                        then for $d in array:flatten($doc-ids) return string($d)
                        else string($doc-ids)
                    return element volume {
                        attribute id { $vol-id },
                        string-join($doc-strings, ", ")
                    }
                }
            else ()
        }
};

(:~
 : Main entry point: build the taxonomy.
 :)

(: ── Step 1: Load mapping ── :)
let $raw-mapping := local:read-json-file($MAPPING-FILE)
let $mapping-loaded :=
    if (empty($raw-mapping) or not($raw-mapping instance of map(*)))
    then error(xs:QName("local:error"), "Cannot load " || $MAPPING-FILE)
    else $raw-mapping

(: ── Step 2: Merge document appearances ── :)
let $doc-apps-raw := local:read-json-file($DOC-APPEARANCES-FILE)
let $mapping-with-docs :=
    if (empty($doc-apps-raw) or not($doc-apps-raw instance of map(*)))
    then $mapping-loaded
    else
        map:merge(
            for $ref in map:keys($mapping-loaded)
            let $data := $mapping-loaded($ref)
            let $updated :=
                if (map:contains($doc-apps-raw, $ref))
                then map:merge(($data, map:entry("document_appearances", $doc-apps-raw($ref))),
                               map { "duplicates": "use-last" })
                else $data
            return map:entry($ref, $updated)
        )

(: ── Step 3: Apply dedup decisions ── :)
let $mapping-deduped := local:apply-dedup-decisions($mapping-with-docs)

(: ── Step 4: Optionally fetch LCSH hierarchies ── :)
let $mapping-final :=
    if ($fetch-hierarchies eq "true")
    then
        let $existing-cache :=
            let $c := local:read-json-file($BT-CACHE-FILE)
            return if ($c instance of map(*)) then $c else map {}
        let $updated-cache := local:fetch-two-level-hierarchy($mapping-deduped, $existing-cache)
        (: Save updated cache :)
        let $_ := file:write-text($BT-CACHE-FILE,
            serialize($updated-cache, map { "method": "json", "indent": true() }))
        (: Attach broader_chain_2lvl to each subject :)
        return map:merge(
            for $ref in map:keys($mapping-deduped)
            let $data := $mapping-deduped($ref)
            let $quality := ($data?match_quality, "no_match")[1]
            let $has-lcsh := $quality = ("exact", "good_close") and exists($data?lcsh_uri)
            let $chain :=
                if ($has-lcsh)
                then
                    let $uri := string($data?lcsh_uri)
                    let $entry := if (map:contains($updated-cache, $uri)) then $updated-cache($uri) else map {}
                    let $bt1-uris :=
                        if (exists($entry?broader_uris)) then array:flatten($entry?broader_uris) else ()
                    let $bt1-uri := $bt1-uris[1]
                    let $bt1 :=
                        if (exists($bt1-uri) and map:contains($updated-cache, $bt1-uri))
                        then
                            let $bt1-entry := $updated-cache($bt1-uri)
                            let $bt1-label := ($bt1-entry?label, tokenize($bt1-uri, "/")[last()])[1]
                            let $bt2-uris :=
                                if (exists($bt1-entry?broader_uris)) then array:flatten($bt1-entry?broader_uris) else ()
                            let $bt2-uri := $bt2-uris[1]
                            let $bt2 :=
                                if (exists($bt2-uri) and map:contains($updated-cache, $bt2-uri))
                                then
                                    let $bt2-entry := $updated-cache($bt2-uri)
                                    let $bt2-label := ($bt2-entry?label, tokenize($bt2-uri, "/")[last()])[1]
                                    return array { map { "label": $bt1-label, "uri": $bt1-uri },
                                                   map { "label": $bt2-label, "uri": $bt2-uri } }
                                else array { map { "label": $bt1-label, "uri": $bt1-uri } }
                            return $bt2
                        else array {}
                    return $bt1
                else array {}
            return map:entry($ref,
                map:merge(($data, map:entry("broader_chain_2lvl", $chain)),
                          map { "duplicates": "use-last" }))
        )
    else $mapping-deduped

(: ── Step 5: Load category overrides ── :)
let $cat-overrides := local:load-category-overrides()

(: ── Step 6: Categorize all subjects ── :)
(: Build a sequence of maps: { ref, data, category, subcategory } :)
let $categorized :=
    for $ref in map:keys($mapping-final)
    let $data := $mapping-final($ref)
    let $name := string(($data?name, "")[1])
    let $match-quality := string(($data?match_quality, "")[1])
    let $lcsh-label :=
        if ($match-quality = ("exact", "good_close"))
        then string(($data?lcsh_label, "")[1])
        else ""

    (: Check for manual override first :)
    let $categorization :=
        if (map:contains($cat-overrides, $ref))
        then
            let $override := $cat-overrides($ref)
            return map {
                "category": $override?to_category,
                "subcategory": $override?to_subcategory
            }
        else local:categorize-by-hsg($name, $lcsh-label)

    let $cat := $categorization?category
    let $sub := $categorization?subcategory
    return map {
        "ref": $ref,
        "data": $data,
        "category": if (exists($cat) and string($cat) ne "" and string($cat) ne "Uncategorized")
                    then string($cat) else "Uncategorized",
        "subcategory": if (exists($sub) and string($sub) ne "") then string($sub) else "General"
    }

(: ── Step 7: Group into categories and subcategories ── :)
let $cat-names := distinct-values($categorized?category)

(: Build nested structure: cat -> sub -> sorted subjects :)
let $taxonomy-data :=
    for $cat-name in $cat-names
    let $cat-subjects := $categorized[?category eq $cat-name]
    let $sub-names := distinct-values($cat-subjects?subcategory)
    let $subcategories :=
        for $sub-name in $sub-names
        let $sub-subjects := $cat-subjects[?subcategory eq $sub-name]
        let $sub-total := sum(for $s in $sub-subjects return local:get-count($s?data))
        let $sorted-subjects :=
            for $s in $sub-subjects
            let $cnt := local:get-count($s?data)
            order by $cnt descending
            return $s
        return map {
            "name": $sub-name,
            "total-annotations": $sub-total,
            "total-subjects": count($sub-subjects),
            "subjects": $sorted-subjects
        }
    let $cat-total := sum(for $sc in $subcategories return $sc?total-annotations)
    let $cat-subject-count := sum(for $sc in $subcategories return $sc?total-subjects)
    (: Sort subcategories by annotation count descending :)
    let $sorted-subs :=
        for $sc in $subcategories
        order by $sc?total-annotations descending
        return $sc
    return map {
        "name": $cat-name,
        "total-annotations": $cat-total,
        "total-subjects": $cat-subject-count,
        "subcategories": $sorted-subs
    }

(: Sort categories: non-Uncategorized by annotation count desc, Uncategorized last :)
let $sorted-cats :=
    let $regular := $taxonomy-data[?name ne "Uncategorized"]
    let $uncat := $taxonomy-data[?name eq "Uncategorized"]
    return (
        for $c in $regular
        order by $c?total-annotations descending
        return $c
        ,
        $uncat
    )

(: ── Step 8: Build XML ── :)
let $total-subjects := map:size($mapping-final)
let $today := format-date(current-date(), "[Y0001]-[M01]-[D01]")

let $taxonomy-xml :=
    element taxonomy {
        attribute source { "hsg-annotate-data" },
        attribute authority { "Office of the Historian (history.state.gov)" },
        attribute authority-uri { "https://history.state.gov/tags/all" },
        attribute generated { $today },
        attribute total-subjects { $total-subjects },

        for $cat in $sorted-cats
        return
            element category {
                attribute label { $cat?name },
                attribute total-annotations { $cat?total-annotations },
                attribute total-subjects { $cat?total-subjects },

                for $sub in $cat?subcategories
                return
                    element subcategory {
                        attribute label { $sub?name },
                        attribute total-annotations { $sub?total-annotations },
                        attribute total-subjects { $sub?total-subjects },

                        for $s in $sub?subjects
                        return local:build-subject-element($s?ref, $s?data)
                    }
            }
    }

(: ── Step 9: Write output ── :)
let $_ := file:write(
    $OUTPUT-FILE,
    $taxonomy-xml,
    map {
        "method": "xml",
        "indent": "yes",
        "omit-xml-declaration": "no",
        "encoding": "UTF-8"
    }
)

return
    <result>
        <message>Taxonomy written to {$OUTPUT-FILE}</message>
        <total-subjects>{$total-subjects}</total-subjects>
        <categories>{count($sorted-cats)}</categories>
        {
            for $cat in $sorted-cats
            return
                <category name="{$cat?name}" subjects="{$cat?total-subjects}"
                          annotations="{$cat?total-annotations}">
                {
                    for $sub in $cat?subcategories
                    return
                        <subcategory name="{$sub?name}" subjects="{$sub?total-subjects}"/>
                }
                </category>
        }
    </result>
