"""
Search query builder for the outline_toot_backend Elasticsearch index.

Provides a single public function :func:`build_search_query` that takes a
Tibetan Unicode query string and returns an elaborate OpenSearch / Elasticsearch
query body targeting the fields defined in ``mappings.json``.

Basics for non-specialists
--------------------------
- **Input**: A search string in Tibetan Unicode (e.g. བཀའ་འགྱུར). Words are
  separated by *tshegs* (U+0F0B and related characters), which act like
  spaces between syllables. The code splits on tshegs to build sub-queries.

- **Honorifics**: Titles and polite formulas (e.g. “Rinpoche”, “Lama”) are
  stripped from the query before search so that “Milarepa” and “Rinpoche
  Milarepa” behave similarly.

- **Index shape**: The index has *instance* documents (work-level metadata:
  preferred/alternate titles, segments with titles and author names) and
  *etext* child documents (full text in chunks). The query searches titles
  and segment metadata on the instance, and full text in etext chunks.

- **Output**: A dict with ``query`` and ``highlight`` that you send as the
  body of an Elasticsearch/OpenSearch ``_search`` request.

- **Simple query as input**: You can pass either a plain string or a small
  query object. If you pass a dict with a ``query`` key (the search text) and
  an optional ``filter`` key (a list of Elasticsearch filter clauses), the
  result will combine the elaborated full-text query with your filters. For
  example, to restrict to a given ``type``, pass
  ``{"query": "བཀའ་འགྱུར", "filter": [{"term": {"type": "work"}}]}``.
"""

import re

from pyewts import pyewts

CONVERTER = pyewts()

# Tsheg characters used to split Tibetan Unicode into tokens (syllables)
TSHEG_CHARS = "\u0f0b\u0f0c\u0f14"  # U+0F0B tsheg, U+0F0C, U+0F14
TSHEG_PATTERN = re.compile(f"[{TSHEG_CHARS}\\s]+")

# Number of etext inner hits to return per result
INNER_HITS_SIZE = 3

# Slop: allow missing tokens every SLOP_VALUE tokens
SLOP_VALUE = 6
SLOP_MAX_VALUE = 5

# ---------------------------------------------------------------------------
# Field definitions
# ---------------------------------------------------------------------------

# Top-level text fields on instance documents
TOP_LEVEL_FIELDS = {
    "prefLabel_bo": 1.0,
    "prefLabel_bo.tibetan-phonetic": 0.90,
    "altLabel_bo": 0.95,
    "altLabel_bo.tibetan-phonetic": 0.85,
}

# Nested segment text fields (inside "segments" nested object)
SEGMENT_FIELDS = {
    "segments.title_bo": 0.9,
    "segments.title_bo.tibetan-phonetic": 0.85,
    "segments.author_name_bo": 0.7,
    "segments.author_name_bo.tibetan-phonetic": 0.65,
}

# Common Tibetan particles that should not form a standalone second phrase
# in the two-phrase query optimisation (Unicode).
TWO_PHRASE_STOPS_BO = {
    "ཏུ",
    "དུ",
    "སུ",
    "གི",
    "ཀྱི",
    "གྱི",
    "གིས",
    "ཀྱིས",
    "གྱིས",
    "ཀྱང",
    "ཡང",
    "སྟེ",
    "དེ",
    "ཏེ",
    "གོ",
    "ངོ",
    "དོ",
    "ནོ",
    "བོ",
    "རོ",
    "སོ",
    "འོ",
    "ཏོ",
    "པ",
    "བ",
    "གིན",
    "ཀྱིན",
    "གྱིན",
    "ཡིན",
    "པས",
    "པའི",
    "པའོ",
    "བས",
    "བའི",
    "ལ",
}

# ---------------------------------------------------------------------------
# Honorific prefix/suffix stripping (EWTS in source → Unicode patterns at load)
# ---------------------------------------------------------------------------

# EWTS honorific prefixes; [pm] means p or m (expanded to two literals at load).
_PREFIXES_EWTS = [
    "mkhan [pm]o ",
    "rgya gar kyi ",
    "mkhan chen ",
    "a lag ",
    "a khu ",
    "rgan ",
    "rgan lags ",
    "zhabs drung ",
    "mkhas grub ",
    "mkhas dbang ",
    "mkhas pa ",
    "bla ma ",
    "sman pa ",
    "em chi ",
    "yongs 'dzin ",
    "ma hA ",
    "sngags pa ",
    "sngags mo ",
    "sngags pa'i rgyal po ",
    "sems dpa' chen po ",
    "rnal 'byor [pm]a ",
    "rje ",
    "rje btsun ",
    "rje btsun [pm]a ",
    "kun mkhyen ",
    "lo tsA ba ",
    "lo tswa ba ",
    "lo cA ba ",
    "lo chen ",
    "slob dpon ",
    "paN\\+Di ta ",
    "paN chen ",
    "srI ",
    "dpal ",
    "dge slong ",
    "dge slong ma ",
    "dge bshes ",
    "dge ba'i bshes gnyen ",
    "shAkya'i dge slong ",
    "'phags pa ",
    "A rya ",
    "gu ru ",
    "sprul sku ",
    "a ni ",
    "a ni lags ",
    "rig 'dzin ",
    "chen [pm]o ",
    "A tsar\\+yA ",
    "gter ston ",
    "gter chen ",
    "thams cad mkhyen pa ",
    "rgyal dbang ",
    "rgyal ba ",
    "btsun [pm]a ",
    "dge rgan ",
    "theg pa chen po'i ",
    "hor ",
    "sog [pm]o ",
    "sog ",
    "a lags sha ",
    "khal kha ",
    "cha har ",
    "jung gar ",
    "o rad ",
    "hor chin ",
    "thu med ",
    "hor pa ",
    "na'i man ",
    "ne nam ",
    "su nyid ",
    "har chen ",
    "bdrc[^a-zA-Z0-9]*",
    "bdr: *",
    "tbrc[^a-zA-Z0-9]*",
]
# EWTS honorific suffixes; (c|[sz]) expanded to c, s, z at load.
_SUFFIXES_EWTS = [
    " dpal bzang po",
    " lags",
    " rin po che",
    " sprul sku",
    " le'u",
    " rgyud kyi rgyal po",
    " bzhugs so",
    " sku gzhogs",
    " (c|[sz])es bya ba",
]


def _expand_ewts_literals(ewts_list: list[str]) -> list[str]:
    """Expand [pm] / [sz] etc. to multiple literal EWTS strings."""
    out = []
    for s in ewts_list:
        if "[pm]" in s:
            out.append(s.replace("[pm]", "p"))
            out.append(s.replace("[pm]", "m"))
        elif "(c|[sz])" in s:
            out.append(s.replace("(c|[sz])", "c"))
            out.append(s.replace("(c|[sz])", "s"))
            out.append(s.replace("(c|[sz])", "z"))
        else:
            out.append(s)
    return out


def _ewts_to_unicode_patterns(ewts_list: list[str], *, suffix: bool = False) -> re.Pattern[str] | None:
    """Convert EWTS literal strings to a single regex pattern in Unicode."""
    expanded = _expand_ewts_literals(ewts_list)
    unicode_parts = []
    ascii_regex_parts = []  # e.g. bdrc[^a-zA-Z0-9]*
    for s in expanded:
        # ASCII-only patterns (catalog codes) stay as regex
        if re.match(r"^[a-zA-Z\[\]\^: *\\]+$", s):
            ascii_regex_parts.append(s)
            continue
        # Tibetan EWTS: strip \+, expand [xy] to literal for conversion
        s_literal = re.sub(r"\\\+", "+", s)
        s_literal = re.sub(r"\[[^\]]*\]", "", s_literal)
        if not s_literal.strip():
            continue
        try:
            u = CONVERTER.toUnicode(s_literal)
            unicode_parts.append(re.escape(u))
        except Exception:
            unicode_parts.append(re.escape(s_literal))
    all_parts = unicode_parts + ascii_regex_parts
    if not all_parts:
        return None
    pattern = "|".join(all_parts)
    if suffix:
        return re.compile("(" + pattern + ")$")
    return re.compile("^(" + pattern + ")")


_PREFIX_PAT = _ewts_to_unicode_patterns(_PREFIXES_EWTS, suffix=False)
_SUFFIX_PAT = _ewts_to_unicode_patterns(_SUFFIXES_EWTS, suffix=True)


def _strip_stopwords(query_str_unicode: str) -> str:
    """Strip common Tibetan honorific prefixes and suffixes (Unicode input)."""
    s = query_str_unicode
    if _PREFIX_PAT:
        s = _PREFIX_PAT.sub("", s)
    if _SUFFIX_PAT:
        s = _SUFFIX_PAT.sub("", s)
    return s.strip()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _split_tshegs(s: str) -> list[str]:
    """Split Tibetan Unicode string on tshegs (and spaces); return list of tokens."""
    if not s or not s.strip():
        return []
    return [w for w in TSHEG_PATTERN.split(s.strip()) if w]


def _fields_weighted(fields: dict[str, float]) -> list[str]:
    """Return ``['field^weight', ...]`` from a ``{field: weight}`` dict."""
    return [f"{f}^{w}" for f, w in fields.items()]


# ---------------------------------------------------------------------------
# Query-building blocks
# ---------------------------------------------------------------------------


def _etext_query(query_str_bo: str, *, exact: bool = False) -> dict:
    """``has_child`` query targeting etext chunk content."""
    field = "chunks.text_bo" + (".exact" if exact else "")
    match = {"match_phrase": {field: query_str_bo}}
    hl_field = {
        field: {
            "highlight_query": match,
        }
    }
    return {
        "has_child": {
            "type": "etext",
            "score_mode": "none",
            "query": {
                "nested": {
                    "path": "chunks",
                    "score_mode": "none",
                    "query": match,
                    "inner_hits": {
                        "_source": True,
                        "highlight": {"fields": hl_field},
                    },
                }
            },
            "inner_hits": {
                "size": INNER_HITS_SIZE,
                "_source": {"includes": ["volume_number"]},
            },
        }
    }


def _segments_query(query_str_bo: str, fields_weighted: list[str], *, slop: int = 0) -> dict:
    """Nested query matching text inside ``segments`` (title, author name)."""
    return {
        "nested": {
            "path": "segments",
            "score_mode": "max",
            "query": {
                "multi_match": {
                    "type": "phrase",
                    "query": query_str_bo,
                    "fields": fields_weighted,
                    "slop": slop,
                }
            },
            "inner_hits": {
                "highlight": {
                    "fields": {
                        "segments.title_bo": {},
                        "segments.author_name_bo": {},
                    }
                }
            },
        }
    }


def _highlight_json(strings_bo: list[str]) -> dict:
    """Build the ``highlight`` section of the OpenSearch request."""
    field_names = list(TOP_LEVEL_FIELDS.keys())
    should = []
    for s in strings_bo:
        n_tokens = len(_split_tshegs(s))
        slop = min(int(n_tokens / SLOP_VALUE), SLOP_MAX_VALUE) if n_tokens else 0
        should.append(
            {
                "multi_match": {
                    "type": "phrase",
                    "query": s,
                    "fields": field_names,
                    "slop": slop,
                }
            }
        )
    return {
        "highlight_query": {"bool": {"should": should}},
        "fields": {
            "*": {},
            "*Label*": {"number_of_fragments": 0},
        },
    }


# ---------------------------------------------------------------------------
# Main query builder
# ---------------------------------------------------------------------------


def _big_query(query_str_bo: str) -> tuple[dict, dict]:
    """Assemble the primary multi-clause search query (Unicode only)."""
    query_str_bo = query_str_bo.strip()
    if not query_str_bo:
        return {"match_all": {}}, {}

    words_bo = _split_tshegs(query_str_bo)
    n_tokens = len(words_bo)

    dis_max = []
    hl_strings = [query_str_bo]

    top_w = _fields_weighted(TOP_LEVEL_FIELDS)
    seg_w = _fields_weighted(SEGMENT_FIELDS)
    slop = min(int(n_tokens / SLOP_VALUE), SLOP_MAX_VALUE) if n_tokens else 0

    # 1. Full phrase match on top-level fields
    dis_max.append(
        {
            "bool": {
                "must": [
                    {
                        "multi_match": {
                            "type": "phrase",
                            "query": query_str_bo,
                            "fields": top_w,
                            "slop": slop,
                        }
                    }
                ],
                "boost": 1.1,
            }
        }
    )

    # 2. Full phrase match on nested segments
    dis_max.append(_segments_query(query_str_bo, seg_w, slop=slop))

    # 3. Etext child match
    dis_max.append(_etext_query(query_str_bo))

    # 4. Two-phrase combinations (split on tshegs)
    if n_tokens > 1:
        mid = n_tokens // 2
        cuts = []
        for n in range(mid + 1):
            if not n:
                cuts.append(mid)
            else:
                for lr in [-1, 1]:
                    c = mid + n * lr
                    if 0 < c < n_tokens:
                        cuts.append(c)

        for cut in cuts:
            if len(dis_max) >= 18 - n_tokens * 0.9:
                break

            p1_bo = "\u0f0b".join(words_bo[:cut])
            p2_bo = "\u0f0b".join(words_bo[cut:])
            p2_single = words_bo[cut] if cut < n_tokens else ""

            # Skip if the second phrase is a single common particle
            if p2_single in TWO_PHRASE_STOPS_BO:
                continue

            pair_must = []
            for p_bo in [p1_bo, p2_bo]:
                pair_must.append(
                    {
                        "bool": {
                            "should": [
                                {
                                    "multi_match": {
                                        "type": "phrase",
                                        "query": p_bo,
                                        "fields": top_w,
                                    }
                                }
                            ]
                        }
                    }
                )
                hl_strings.append(p_bo)

            dis_max.append({"bool": {"must": pair_must, "boost": 0.2}})

    big = {"bool": {"must": [{"dis_max": {"queries": dis_max}}]}}
    highlight = _highlight_json(hl_strings)
    return big, highlight


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def build_search_query(query_input: str | dict) -> dict:
    """Build an elaborate OpenSearch query from a string or a simple query object.

    Args:
        query_input: Either:
            - A Tibetan Unicode string (the search text), or
            - A dict with:
              - ``query`` (str): the search text.
              - ``filter`` (list, optional): list of Elasticsearch filter
                clauses (e.g. ``[{"term": {"type": "work"}}]``). They are
                combined with the full-text query in a ``bool`` so that
                only documents matching both the text and the filters are
                returned.

    Returns:
        A ``dict`` with ``query`` and ``highlight`` keys suitable for an
        OpenSearch ``_search`` request body.
    """
    if isinstance(query_input, dict):
        query_str = query_input.get("query") or query_input.get("q") or ""
        filters = query_input.get("filter") or []
    else:
        query_str = query_input or ""
        filters = []

    query_str = query_str.strip() if isinstance(query_str, str) else ""
    if not query_str:
        if filters:
            return {
                "query": {"bool": {"filter": filters}},
                "highlight": _highlight_json([]),
            }
        return {"query": {"match_all": {}}}

    query_str = _strip_stopwords(query_str)
    if not query_str:
        if filters:
            return {
                "query": {"bool": {"filter": filters}},
                "highlight": _highlight_json([]),
            }
        return {"query": {"match_all": {}}}

    query, highlight = _big_query(query_str)

    if filters:
        query = {"bool": {"must": [query], "filter": filters}}

    return {"query": query, "highlight": highlight}


# ---------------------------------------------------------------------------
# Quick test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import json

    for test in ["བཀའ་འགྱུར", "མི་ལ་རས་པ"]:
        print(f"\n{'=' * 60}")  # noqa: T201
        print(f"Query: {test}")  # noqa: T201
        print("=" * 60)  # noqa: T201
        result = build_search_query(test)
        print(json.dumps(result, indent=2, ensure_ascii=False))  # noqa: T201
