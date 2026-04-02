#!/usr/bin/env python3
"""
Generate FORRT Knowledge Loom nanopublication TriG files (claims, study, outcomes)
from TIB Knowledge Loom records, enriched with dtreg JSON-LD analysis metadata.

Usage:
    python loom2nanopub.py vjea9aobg7           # Single record by resource ID
    python loom2nanopub.py --all                 # All non-DONE records
    python loom2nanopub.py --all --dry-run       # Preview what would be processed

Reads loom_records.txt for the mapping between resource IDs and GitLab repos.
Outputs unsigned TriG files into outputs/<RESOURCE_ID>/ for review.
"""

import argparse
import json
import re
import sys
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from rdflib import Dataset, Namespace, URIRef, Literal
from rdflib.namespace import RDF, RDFS, XSD, FOAF
from nanopub import load_profile

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
PUBLISHER = "https://sciencelive4all.org/"
KL_API = "https://knowledgeloom.tib.eu/api/v1"
GITLAB_API = "https://gitlab.com/api/v4"
LOOM_GROUP = "TIBHannover/lki/knowledge-loom/loom-records"

# Namespaces
TEMP_NP = Namespace("https://w3id.org/sciencelive/np")
NP = Namespace("http://www.nanopub.org/nschema#")
DCT = Namespace("http://purl.org/dc/terms/")
NT = Namespace("https://w3id.org/np/o/ntemplate/")
NPX = Namespace("http://purl.org/nanopub/x/")
PROV = Namespace("http://www.w3.org/ns/prov#")
SCHEMA = Namespace("http://schema.org/")
SCIENCELIVE = Namespace("https://w3id.org/sciencelive/o/terms/")

# Template URIs
FORRT_CLAIM_TEMPLATE = URIRef("https://w3id.org/np/RAu5uTahAxc0OLBB3vaGwK3OQDDZV7QuWtDlBk0Ea3bco")
FORRT_KL_STUDY_TEMPLATE = URIRef("https://w3id.org/np/RALIq4JelUP-q9BuWONcKMJ87B5n59ppcwhQjl-1dheO4")
FORRT_KL_OUTCOME_TEMPLATE = URIRef("https://w3id.org/np/RAw3XdUhxQJfKBaU-cQhV6c7au4rLd5CSUdbMKTS_FB8g")
PROV_TEMPLATE = URIRef("https://w3id.org/np/RA7lSq6MuK_TIC6JMSHvLtee3lpLoZDOqLJCLXevnrPoU")
PUBINFO_TEMPLATE_1 = URIRef("https://w3id.org/np/RACJ58Gvyn91LqCKIO9zu1eijDQIeEff28iyDrJgjSJF8")
PUBINFO_TEMPLATE_2 = URIRef("https://w3id.org/np/RAukAcWHRDlkqxk7H2XNSegc1WnHI569INvNr-xdptDGI")

# dtreg ePIC hash -> SCIENCELIVE analysis type URI
DTREG_TYPE_MAP = {
    "feeb33ad3e4440682a4d": SCIENCELIVE["dtreg-DataAnalysis"],
    "37182ecfb4474942e255": SCIENCELIVE["dtreg-DataPreprocessing"],
    "5b66cb584b974b186f37": SCIENCELIVE["dtreg-DescriptiveStatistics"],
    "b9335ce2c99ed87735a6": SCIENCELIVE["dtreg-GroupComparison"],
    "286991b26f02d58ee490": SCIENCELIVE["dtreg-RegressionAnalysis"],
    "3f64a93eef69d721518f": SCIENCELIVE["dtreg-CorrelationAnalysis"],
    "c6b413ba96ba477b5dca": SCIENCELIVE["dtreg-MultilevelAnalysis"],
    "6e3e29ce3ba5a0b9abfe": SCIENCELIVE["dtreg-ClassPrediction"],
    "c6e19df3b52ab8d855a9": SCIENCELIVE["dtreg-ClassDiscovery"],
    "5e782e67e70d0b2a022a": SCIENCELIVE["dtreg-AlgorithmEvaluation"],
    "437807f8d1a81b5138a3": SCIENCELIVE["dtreg-FactorAnalysis"],
}

DEFAULT_PROFILE_PATH = "/Users/annef/Documents/ScienceLive/annefou-profile/profile.yml"


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------
@dataclass
class AnalysisInfo:
    """Extracted info from one dtreg JSON-LD analysis step."""
    label: str = ""
    method_label: str = ""
    method_call: str = ""
    package_name: str = ""
    package_version: str = ""
    runtime_name: str = ""
    runtime_version: str = ""
    input_label: str = ""
    input_source_url: str = ""
    input_rows: int = 0
    input_cols: int = 0
    json_file: str = ""


@dataclass
class StepOutputInfo:
    """Output data for a single analysis step (maps to one KL statement)."""
    step_label: str = ""
    target_variable: str = ""
    output_label: str = ""
    output_columns: list = None
    output_values: dict = None  # {col_name: value} for first row
    output_rows: int = 0
    output_cols: int = 0
    viz_url: str = ""
    input_label: str = ""
    input_source_url: str = ""

    def __post_init__(self):
        if self.output_columns is None:
            self.output_columns = []
        if self.output_values is None:
            self.output_values = {}


@dataclass
class LoomRecordInfo:
    """Parsed line from loom_records.txt."""
    resource_id: str = ""
    stmt_count: int = 0
    gitlab_repo: str = ""
    title: str = ""
    done: bool = False


# ---------------------------------------------------------------------------
# Parsing loom_records.txt
# ---------------------------------------------------------------------------
def parse_loom_records(records_file: Path) -> list[LoomRecordInfo]:
    """Parse loom_records.txt into a list of LoomRecordInfo."""
    records = []
    with open(records_file) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            done = line.rstrip().endswith("DONE")
            if done:
                line = line.rstrip()[:-4].rstrip()
            parts = line.split()
            if len(parts) < 3:
                continue
            resource_id = parts[0]
            try:
                stmt_count = int(parts[1])
            except ValueError:
                continue
            gitlab_repo = parts[2]
            title = " ".join(parts[3:]) if len(parts) > 3 else ""
            records.append(LoomRecordInfo(
                resource_id=resource_id,
                stmt_count=stmt_count,
                gitlab_repo=gitlab_repo,
                title=title,
                done=done,
            ))
    return records


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------
def fetch_json(url: str):
    """Fetch JSON from a URL."""
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read().decode())


def gitlab_api_url(repo_path: str, endpoint: str) -> str:
    """Build a GitLab API URL for a repo under the LOOM_GROUP."""
    encoded = urllib.parse.quote(f"{LOOM_GROUP}/{repo_path}", safe="")
    return f"{GITLAB_API}/projects/{encoded}/{endpoint}"


# ---------------------------------------------------------------------------
# dtreg JSON-LD helpers
# ---------------------------------------------------------------------------
def get_prop(obj, suffix):
    """Get a property from a dtreg JSON-LD object by matching key ending with #{suffix} or equal to suffix."""
    if not isinstance(obj, dict):
        return None
    for key, val in obj.items():
        if key.endswith(f"#{suffix}") or key == suffix:
            return val
    return None


def parse_single_step(step, filename):
    """Extract software/input metadata from one analysis step."""
    info = AnalysisInfo(json_file=filename)
    executes = get_prop(step, "executes")
    if isinstance(executes, dict):
        info.method_label = get_prop(executes, "label") or ""
        info.method_call = get_prop(executes, "is_implemented_by") or ""
        part_of = get_prop(executes, "part_of")
        if isinstance(part_of, dict):
            info.package_name = get_prop(part_of, "label") or ""
            info.package_version = get_prop(part_of, "version_info") or ""
            runtime = get_prop(part_of, "part_of")
            if isinstance(runtime, dict):
                info.runtime_name = get_prop(runtime, "label") or ""
                info.runtime_version = get_prop(runtime, "version_info") or ""
    has_input = get_prop(step, "has_input")
    if isinstance(has_input, list):
        has_input = has_input[0] if has_input else None
    if isinstance(has_input, dict):
        info.input_label = get_prop(has_input, "label") or ""
        info.input_source_url = get_prop(has_input, "source_url") or ""
        chars = get_prop(has_input, "has_characteristic")
        if isinstance(chars, dict):
            info.input_rows = get_prop(chars, "number_of_rows") or 0
            info.input_cols = get_prop(chars, "number_of_columns") or 0
    method_str = f"{info.package_name}::{info.method_label}" if info.package_name else info.method_label
    info.label = method_str or filename
    return info if (info.method_label or info.input_label) else None


def extract_analysis_steps(dtreg_data, filename):
    """Extract all analysis steps from a dtreg JSON-LD file. Returns list of AnalysisInfo."""
    results = []
    has_part = get_prop(dtreg_data, "has_part")
    if isinstance(has_part, dict):
        steps = [has_part]
    elif isinstance(has_part, list):
        steps = has_part
    else:
        steps = [dtreg_data]
    for step in steps:
        if not isinstance(step, dict):
            continue
        info = parse_single_step(step, filename)
        if info:
            results.append(info)
    return results


def extract_step_output(step):
    """Extract output/result data from a dtreg analysis step."""
    info = StepOutputInfo()
    info.step_label = get_prop(step, "label") or ""
    # Input data
    has_input = get_prop(step, "has_input")
    if isinstance(has_input, list):
        has_input = has_input[0] if has_input else None
    if isinstance(has_input, dict):
        info.input_label = get_prop(has_input, "label") or ""
        info.input_source_url = get_prop(has_input, "source_url") or ""
    targets = get_prop(step, "targets")
    if isinstance(targets, dict):
        info.target_variable = get_prop(targets, "label") or ""
    elif isinstance(targets, list) and targets:
        info.target_variable = ", ".join(
            get_prop(t, "label") or "" for t in targets if isinstance(t, dict)
        )
    has_output = get_prop(step, "has_output")
    if isinstance(has_output, dict):
        info.output_label = get_prop(has_output, "label") or ""
        # Column names
        parts = get_prop(has_output, "has_part")
        if isinstance(parts, list):
            info.output_columns = [get_prop(p, "label") for p in parts if isinstance(p, dict)]
        # Dimensions
        chars = get_prop(has_output, "has_characteristic")
        if isinstance(chars, dict):
            info.output_rows = get_prop(chars, "number_of_rows") or 0
            info.output_cols = get_prop(chars, "number_of_columns") or 0
        # Visualization URL
        expr = get_prop(has_output, "has_expression")
        if isinstance(expr, dict):
            info.viz_url = get_prop(expr, "source_url") or ""
        # Actual result values from first row of source_table
        src_table = get_prop(has_output, "source_table")
        if isinstance(src_table, dict):
            cols = src_table.get("columns", get_prop(src_table, "columns") or [])
            rows = src_table.get("rows", get_prop(src_table, "rows") or [])
            col_titles = []
            for c in (cols if isinstance(cols, list) else []):
                title = c.get("col_titles", get_prop(c, "col_titles") or get_prop(c, "titles") or "")
                col_titles.append(title)
            if isinstance(rows, list) and rows:
                cells = rows[0].get("cells", get_prop(rows[0], "cells") or [])
                if isinstance(cells, list):
                    for ci, cell in enumerate(cells):
                        v = get_prop(cell, "value") or get_prop(cell, "primary_value") or ""
                        col_name = (
                            col_titles[ci] if ci < len(col_titles)
                            else info.output_columns[ci] if ci < len(info.output_columns)
                            else f"col{ci}"
                        )
                        info.output_values[col_name] = str(v)
    return info


# ---------------------------------------------------------------------------
# RDF helpers
# ---------------------------------------------------------------------------
def slugify(s: str) -> str:
    """Make a URL-safe slug."""
    return re.sub(r'[^a-zA-Z0-9_-]', '-', s.lower()).strip('-')[:60]


def bind_all(ds):
    """Bind standard prefixes to a Dataset."""
    for p, n in [
        ("this", TEMP_NP), ("sub", TEMP_NP), ("np", NP), ("dct", DCT),
        ("nt", NT), ("npx", NPX), ("xsd", XSD), ("rdfs", RDFS),
        ("prov", PROV), ("foaf", FOAF), ("schema", SCHEMA),
        ("sciencelive", SCIENCELIVE),
    ]:
        ds.bind(p, n)


def make_head(ds):
    """Create the Head graph and return the nanopub URI."""
    this_np = URIRef(TEMP_NP)
    h = ds.graph(URIRef(TEMP_NP + "/Head"))
    h.add((this_np, RDF.type, NP.Nanopublication))
    h.add((this_np, NP.hasAssertion, URIRef(TEMP_NP + "/assertion")))
    h.add((this_np, NP.hasProvenance, URIRef(TEMP_NP + "/provenance")))
    h.add((this_np, NP.hasPublicationInfo, URIRef(TEMP_NP + "/pubinfo")))
    return this_np


def make_provenance(ds, author_uri):
    """Create the Provenance graph."""
    p = ds.graph(URIRef(TEMP_NP + "/provenance"))
    p.add((URIRef(TEMP_NP + "/assertion"), PROV.wasAttributedTo, author_uri))


def make_pubinfo(ds, this_np, label, template_uri, author_uri, author_name,
                 introduced_uri=None):
    """Create the PublicationInfo graph."""
    pi = ds.graph(URIRef(TEMP_NP + "/pubinfo"))
    pi.add((author_uri, FOAF.name, Literal(author_name)))
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    pi.add((this_np, DCT.created, Literal(now, datatype=XSD.dateTime)))
    pi.add((this_np, DCT.creator, author_uri))
    pi.add((this_np, DCT.license, URIRef("https://creativecommons.org/licenses/by/4.0/")))
    pi.add((this_np, NPX.wasCreatedAt, URIRef(PUBLISHER)))
    pi.add((this_np, RDFS.label, Literal(label)))
    pi.add((this_np, NT.wasCreatedFromTemplate, template_uri))
    pi.add((this_np, NT.wasCreatedFromProvenanceTemplate, PROV_TEMPLATE))
    pi.add((this_np, NT.wasCreatedFromPubinfoTemplate, PUBINFO_TEMPLATE_1))
    pi.add((this_np, NT.wasCreatedFromPubinfoTemplate, PUBINFO_TEMPLATE_2))
    if introduced_uri:
        pi.add((this_np, NPX.introduces, introduced_uri))


def validate_trig(trig_file: Path):
    """Basic sanity checks on generated TriG."""
    content = trig_file.read_text()
    assert 'orcid.org/https' not in content, f"Double ORCID in {trig_file.name}!"
    assert '10.82209/' not in content, f"Unresolvable TIB DOI in {trig_file.name}!"


# ---------------------------------------------------------------------------
# Claim type auto-mapping
# ---------------------------------------------------------------------------
def map_claim_type(kl_type_name: str) -> URIRef:
    """Map a KL analysis type name to a FORRT claim type URI."""
    kl = kl_type_name.lower()
    if any(t in kl for t in ["group comparison", "regression", "correlation", "multilevel"]):
        return SCIENCELIVE["statistical_significance-FORRT-Claim"]
    if "algorithm evaluation" in kl:
        return SCIENCELIVE["model_performance-FORRT-Claim"]
    if any(t in kl for t in ["descriptive", "factor"]):
        return SCIENCELIVE["descriptive_pattern-FORRT-Claim"]
    if "preprocessing" in kl:
        return SCIENCELIVE["data_quality-FORRT-Claim"]
    return SCIENCELIVE["statistical_significance-FORRT-Claim"]


# ---------------------------------------------------------------------------
# Core processing for one record
# ---------------------------------------------------------------------------
def process_record(record: LoomRecordInfo, output_dir: Path,
                   author_uri: URIRef, author_name: str,
                   dry_run: bool = False) -> dict:
    """
    Fetch KL + dtreg data for one record and generate unsigned TriG files.
    Returns a summary dict with counts of generated files.
    """
    resource_id = record.resource_id
    gitlab_repo = record.gitlab_repo
    kl_url = f"https://knowledgeloom.tib.eu/resource/{resource_id}"

    print(f"\n{'='*70}")
    print(f"Processing: {resource_id} — {record.title}")
    print(f"  KL URL: {kl_url}")
    print(f"  GitLab: {gitlab_repo}")

    if dry_run:
        print(f"  [DRY RUN] Would generate ~{record.stmt_count} claims + 1 study + ~{record.stmt_count} outcomes")
        return {"claims": 0, "study": 0, "outcomes": 0, "dry_run": True}

    # ------------------------------------------------------------------
    # 1. Fetch KL data
    # ------------------------------------------------------------------
    print(f"  Fetching KL article...")
    kl_data = fetch_json(f"{KL_API}/articles/get_article_by_id/?id={resource_id}")
    article = kl_data.get("article", kl_data)
    statements = kl_data.get("statements", [])
    datasets = kl_data.get("basises", [])

    # Source DOI from basises
    source_doi = None
    for d in datasets:
        if d.get("id", "").startswith("http"):
            source_doi = d["id"]
            break

    print(f"  Title: {article.get('name', '?')}")
    print(f"  Source DOI: {source_doi or '(none)'}")
    print(f"  Statements: {len(statements)}")

    # ------------------------------------------------------------------
    # 2. Fetch dtreg JSON-LD from GitLab
    # ------------------------------------------------------------------
    analyses = []
    script_url = ""
    input_data_urls = []  # list of (filename, url)
    stmt_output_map = {}  # statement_label -> StepOutputInfo

    if gitlab_repo:
        gitlab_url = f"https://gitlab.com/{LOOM_GROUP}/{gitlab_repo}"

        # Get file list
        print(f"  Fetching GitLab file list...")
        tree = fetch_json(gitlab_api_url(gitlab_repo, "repository/tree?per_page=100&ref=main"))
        json_files = [f["name"] for f in tree if f["name"].endswith(".json")]
        print(f"  Found {len(json_files)} JSON files")

        # Parse each JSON-LD file
        for jf in json_files:
            encoded_jf = urllib.parse.quote(jf, safe="")
            raw_url = gitlab_api_url(gitlab_repo, f"repository/files/{encoded_jf}/raw?ref=main")
            try:
                dtreg = fetch_json(raw_url)
            except Exception as e:
                print(f"    Warning: could not parse {jf}: {e}")
                continue
            # Script URL from top-level is_implemented_by
            impl = get_prop(dtreg, "is_implemented_by")
            if isinstance(impl, str) and impl.startswith("http"):
                script_url = impl
            infos = extract_analysis_steps(dtreg, jf)
            for info in infos:
                analyses.append(info)
                print(f"    {jf}: {info.label}")

        if script_url:
            print(f"  Script: {script_url}")

        # Extract input dataset URLs from metadata.json file_mapping
        try:
            encoded_meta = urllib.parse.quote("utils/metadata.json", safe="")
            meta_url = gitlab_api_url(gitlab_repo, f"repository/files/{encoded_meta}/raw?ref=main")
            _metadata = fetch_json(meta_url)
            _fm = _metadata.get("file_mapping", {})
            data_exts = {"csv", "xlsx", "tsv", "rds", "rda", "dat", "sav", "txt"}
            for fname, finfo in _fm.items():
                ext = fname.rsplit(".", 1)[-1].lower() if "." in fname else ""
                if ext in data_exts and finfo.get("resource_url"):
                    input_data_urls.append((fname, finfo["resource_url"]))
            if input_data_urls:
                print(f"  Input datasets: {len(input_data_urls)}")
        except Exception as e:
            print(f"  Warning: could not extract data URLs: {e}")

        # Build per-statement output mapping via metadata.json
        try:
            encoded_meta = urllib.parse.quote("utils/metadata.json", safe="")
            meta_url = gitlab_api_url(gitlab_repo, f"repository/files/{encoded_meta}/raw?ref=main")
            metadata = fetch_json(meta_url)
            meta_stmts = metadata.get("statements", {})
            file_mapping = metadata.get("file_mapping", {})
            label_to_json = {}
            for sid, sinfo in meta_stmts.items():
                label = sinfo.get("label", "")
                json_orig = sinfo.get("json_file_name", "")
                mapped = file_mapping.get(json_orig, {}).get("mapped_name", "")
                if label and mapped:
                    label_to_json[label] = mapped
                if label.strip() and mapped:
                    label_to_json[label.strip()] = mapped
            for label, json_fname in label_to_json.items():
                encoded_jf = urllib.parse.quote(json_fname, safe="")
                raw_url = gitlab_api_url(gitlab_repo, f"repository/files/{encoded_jf}/raw?ref=main")
                try:
                    dtreg = fetch_json(raw_url)
                except Exception:
                    continue
                has_part = get_prop(dtreg, "has_part")
                steps = (
                    has_part if isinstance(has_part, list)
                    else [has_part] if isinstance(has_part, dict)
                    else []
                )
                for step in reversed(steps):
                    if isinstance(step, dict):
                        output_info = extract_step_output(step)
                        if output_info.output_values or output_info.output_label or output_info.viz_url:
                            stmt_output_map[label] = output_info
                            break
            print(f"  Statement-output mapping: {len(stmt_output_map)}/{len(label_to_json)} statements have output data")
        except Exception as e:
            print(f"  Warning: could not build statement-output mapping: {e}")
    else:
        gitlab_url = ""
        print("  No GitLab repo — skipping dtreg parsing.")

    # ------------------------------------------------------------------
    # 3. Generate TriG files
    # ------------------------------------------------------------------
    rec_dir = output_dir / resource_id
    rec_dir.mkdir(parents=True, exist_ok=True)

    # --- Claims (one per KL statement) ---
    claim_files = []
    for i, stmt in enumerate(statements):
        ds = Dataset()
        bind_all(ds)
        this_np = make_head(ds)
        claim_id = slugify(f"kl-claim-{stmt['statement_id']}")
        claim_uri = URIRef(str(TEMP_NP) + "/" + claim_id)
        aida_sentence = stmt["label"]
        aida_uri = URIRef(f"http://purl.org/aida/{urllib.parse.quote(aida_sentence, safe='')}")

        a = ds.graph(URIRef(TEMP_NP + "/assertion"))
        a.add((claim_uri, RDF.type, SCIENCELIVE["FORRT-Claim"]))
        # Auto-map claim type
        kl_type = stmt.get("type", {}).get("name", "")
        forrt_type = map_claim_type(kl_type)
        a.add((claim_uri, RDF.type, forrt_type))
        a.add((claim_uri, RDFS.label, Literal(aida_sentence)))
        a.add((claim_uri, SCIENCELIVE["asAidaStatement"], aida_uri))
        if source_doi:
            a.add((claim_uri, DCT.source, URIRef(source_doi)))
        # Input data source from dtreg analyses
        for ai in analyses:
            if ai.input_label:
                desc = ai.input_label
                if ai.input_rows and ai.input_cols:
                    desc += f" ({ai.input_rows} rows x {ai.input_cols} columns)"
                a.add((claim_uri, SCIENCELIVE["hasDataSource"], Literal(desc)))
                break

        make_provenance(ds, author_uri)
        label = f"FORRT Claim: {aida_sentence[:80]}{'...' if len(aida_sentence) > 80 else ''}"
        make_pubinfo(ds, this_np, label, FORRT_CLAIM_TEMPLATE, author_uri, author_name, claim_uri)

        f = rec_dir / f"{resource_id}-claim-{i+1}.trig"
        ds.serialize(destination=str(f), format='trig')
        validate_trig(f)
        claim_files.append(f)
        print(f"    {f.name} — {label}")

    print(f"  Generated {len(claim_files)} claims.")

    # --- Study ---
    ds = Dataset()
    bind_all(ds)
    this_np = make_head(ds)
    study_id = slugify(f"kl-study-{resource_id}")
    study_uri = URIRef(str(TEMP_NP) + "/" + study_id)

    a = ds.graph(URIRef(TEMP_NP + "/assertion"))
    a.add((study_uri, RDF.type, SCIENCELIVE["FORRT-Replication-Study"]))
    a.add((study_uri, RDF.type, SCIENCELIVE["Reproduction-Study"]))
    label = f"Replication study: {article.get('name', resource_id)}"
    a.add((study_uri, RDFS.label, Literal(label)))

    # Scope
    abstract = article.get("abstract", "")
    scope = f"Reproduction of analyses from Knowledge Loom record: {article.get('name', resource_id)}"
    if abstract:
        scope += f". {abstract}"
    a.add((study_uri, SCIENCELIVE["hasScopeDescription"], Literal(scope)))

    # Methodology
    types = set(
        s.get("type", {}).get("name", "") for s in statements if s.get("type", {}).get("name")
    )
    method = f"Analysis types: {', '.join(sorted(types))}." if types else ""
    method += " Machine-readable descriptions generated using dtreg and published in the TIB Knowledge Loom."
    a.add((study_uri, SCIENCELIVE["hasMethodologyDescription"], Literal(method.strip())))

    # Software and input data from dtreg
    if analyses:
        methods_set, packages_set, runtimes_set = set(), set(), set()
        input_descs_set, input_urls_set = set(), set()
        for ai in analyses:
            if ai.method_call:
                methods_set.add(ai.method_call.split("\n")[0].strip())
            elif ai.method_label and ai.package_name:
                methods_set.add(f"{ai.package_name}::{ai.method_label}()")
            if ai.package_name:
                packages_set.add(f"{ai.package_name} {ai.package_version}".strip())
            if ai.runtime_name:
                runtimes_set.add(f"{ai.runtime_name} {ai.runtime_version}".strip())
            if ai.input_label:
                desc = ai.input_label
                if ai.input_rows and ai.input_cols:
                    desc += f" ({ai.input_rows} rows x {ai.input_cols} columns)"
                input_descs_set.add(desc)
            if ai.input_source_url:
                input_urls_set.add(ai.input_source_url)
        if methods_set:
            a.add((study_uri, SCIENCELIVE["executesMethod"], Literal("; ".join(sorted(methods_set)))))
        if packages_set:
            a.add((study_uri, SCIENCELIVE["usesSoftwarePackage"], Literal("; ".join(sorted(packages_set)))))
        if runtimes_set:
            a.add((study_uri, SCIENCELIVE["hasRuntimeEnvironment"], Literal("; ".join(sorted(runtimes_set)))))
        if input_descs_set:
            a.add((study_uri, SCIENCELIVE["hasInputDataDescription"], Literal("; ".join(sorted(input_descs_set)))))
        for url in sorted(input_urls_set):
            a.add((study_uri, SCIENCELIVE["hasInputDataSource"], URIRef(url)))

    # Input dataset URLs from metadata.json
    for fname, url in input_data_urls:
        a.add((study_uri, SCIENCELIVE["hasInputDataset"], URIRef(url)))
    if script_url:
        a.add((study_uri, SCIENCELIVE["hasAnalysisScript"], URIRef(script_url)))
    # KL link
    a.add((study_uri, SCIENCELIVE["hasLoomRecord"], URIRef(kl_url)))

    make_provenance(ds, author_uri)
    make_pubinfo(ds, this_np, label, FORRT_KL_STUDY_TEMPLATE, author_uri, author_name, study_uri)

    study_file = rec_dir / f"{resource_id}-study.trig"
    ds.serialize(destination=str(study_file), format='trig')
    validate_trig(study_file)
    print(f"    {study_file.name} — {label}")
    print(f"  Generated study.")

    # --- Outcomes (one per KL statement) ---
    outcome_files = []
    for i, stmt in enumerate(statements):
        ds = Dataset()
        bind_all(ds)
        this_np = make_head(ds)
        outcome_id = slugify(f"kl-outcome-{resource_id}-{i+1}")
        outcome_uri = URIRef(str(TEMP_NP) + "/" + outcome_id)
        stmt_label = stmt["label"]
        type_info = stmt.get("type", {})
        type_name = type_info.get("name", "analysis")
        label = f"Outcome ({type_name}): {stmt_label[:60]}{'...' if len(stmt_label) > 60 else ''}"
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        a = ds.graph(URIRef(TEMP_NP + "/assertion"))
        a.add((outcome_uri, RDF.type, SCIENCELIVE["FORRT-Replication-Outcome"]))
        a.add((outcome_uri, RDFS.label, Literal(label)))
        a.add((outcome_uri, SCIENCELIVE["hasOutcomeRepository"], URIRef(kl_url)))
        a.add((outcome_uri, SCHEMA.endDate, Literal(today, datatype=XSD.date)))
        a.add((outcome_uri, SCIENCELIVE["hasValidationStatus"], SCIENCELIVE["Validated"]))
        a.add((outcome_uri, SCIENCELIVE["hasConfidenceLevel"], SCIENCELIVE["HighConfidence"]))
        a.add((outcome_uri, SCIENCELIVE["hasConclusionDescription"],
               Literal(f"Knowledge Loom verified: {stmt_label}")))

        # Enrich with output data from dtreg (per-statement mapping)
        output_info = stmt_output_map.get(stmt_label) or stmt_output_map.get(stmt_label.strip())
        if output_info:
            if output_info.target_variable:
                a.add((outcome_uri, SCIENCELIVE["hasTargetVariable"], Literal(output_info.target_variable)))
            if output_info.output_label:
                a.add((outcome_uri, SCIENCELIVE["hasOutputDescription"], Literal(output_info.output_label)))
            if output_info.output_values:
                vals_str = "; ".join(f"{k} = {v}" for k, v in output_info.output_values.items())
                a.add((outcome_uri, SCIENCELIVE["hasResultValues"], Literal(vals_str)))
            if output_info.output_columns:
                a.add((outcome_uri, SCIENCELIVE["hasResultMetrics"], Literal(", ".join(output_info.output_columns))))
            if output_info.viz_url:
                a.add((outcome_uri, SCIENCELIVE["hasResultVisualization"], URIRef(output_info.viz_url)))
            if output_info.step_label:
                a.add((outcome_uri, SCIENCELIVE["hasAnalysisDescription"], Literal(output_info.step_label)))
            evidence = f"Reproduced {type_name}: {output_info.step_label or stmt_label}."
            if output_info.output_values:
                top_vals = "; ".join(f"{k}={v}" for k, v in list(output_info.output_values.items())[:5])
                evidence += f" Result: {top_vals}."
        else:
            evidence = f"Machine-readable analysis proof ({type_name}) published in the TIB Knowledge Loom."
        a.add((outcome_uri, SCIENCELIVE["hasEvidenceDescription"], Literal(evidence)))

        # Input data source for this specific outcome
        if output_info:
            if output_info.input_label:
                a.add((outcome_uri, SCIENCELIVE["hasInputDataDescription"], Literal(output_info.input_label)))
            if output_info.input_source_url:
                a.add((outcome_uri, SCIENCELIVE["hasInputDataSource"], URIRef(output_info.input_source_url)))

        a.add((outcome_uri, SCIENCELIVE["hasMachineReadableProof"], URIRef(kl_url)))
        type_hash = type_info.get("type_id", "")
        if type_hash in DTREG_TYPE_MAP:
            a.add((outcome_uri, SCIENCELIVE["hasAnalysisType"], DTREG_TYPE_MAP[type_hash]))

        make_provenance(ds, author_uri)
        make_pubinfo(ds, this_np, label, FORRT_KL_OUTCOME_TEMPLATE, author_uri, author_name, outcome_uri)

        f = rec_dir / f"{resource_id}-outcome-{i+1}.trig"
        ds.serialize(destination=str(f), format='trig')
        validate_trig(f)
        outcome_files.append(f)
        print(f"    {f.name} — {label}")

    print(f"  Generated {len(outcome_files)} outcomes.")

    return {"claims": len(claim_files), "study": 1, "outcomes": len(outcome_files)}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Generate FORRT-KL nanopub TriG files from Knowledge Loom records"
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "resource_id", nargs="?", default=None,
        help="Loom resource ID (e.g. vjea9aobg7) to process a single record",
    )
    group.add_argument(
        "--all", action="store_true",
        help="Process all non-DONE records from loom_records.txt",
    )
    parser.add_argument(
        "--output-dir", "-o", default=None,
        help="Output directory for TriG files (default: outputs/ next to loom_records.txt)",
    )
    parser.add_argument(
        "--profile", default=DEFAULT_PROFILE_PATH,
        help=f"Path to nanopub profile YAML (default: {DEFAULT_PROFILE_PATH})",
    )
    parser.add_argument(
        "--orcid", default=None,
        help="Override ORCID from profile (e.g. https://orcid.org/0000-0002-1784-2920)",
    )
    parser.add_argument(
        "--name", default=None,
        help="Override author name from profile",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Show what would be processed without generating files",
    )

    args = parser.parse_args()

    # Locate loom_records.txt relative to this script
    script_dir = Path(__file__).resolve().parent
    records_file = script_dir / "loom_records.txt"
    if not records_file.exists():
        print(f"Error: {records_file} not found", file=sys.stderr)
        sys.exit(1)

    # Output directory
    if args.output_dir:
        output_dir = Path(args.output_dir)
    else:
        output_dir = script_dir / "outputs"

    # Load profile
    print(f"Loading profile from {args.profile}...")
    profile = load_profile(args.profile)
    author_uri = URIRef(args.orcid) if args.orcid else URIRef(profile.orcid_id)
    author_name = args.name if args.name else profile.name
    assert 'orcid.org/orcid.org' not in str(author_uri), "Double ORCID detected in profile!"
    print(f"Author: {author_name} ({author_uri})")

    # Parse records
    all_records = parse_loom_records(records_file)
    print(f"Loaded {len(all_records)} records from {records_file.name}")

    # Select records to process
    if args.all:
        to_process = [r for r in all_records if not r.done]
        print(f"Processing {len(to_process)} non-DONE records (skipping {len(all_records) - len(to_process)} DONE)")
    else:
        to_process = [r for r in all_records if r.resource_id == args.resource_id]
        if not to_process:
            print(f"Error: resource ID '{args.resource_id}' not found in {records_file.name}", file=sys.stderr)
            sys.exit(1)

    if not to_process:
        print("No records to process.")
        sys.exit(0)

    output_dir.mkdir(parents=True, exist_ok=True)

    # Process each record
    totals = {"claims": 0, "study": 0, "outcomes": 0}
    errors = []
    for record in to_process:
        try:
            result = process_record(record, output_dir, author_uri, author_name, args.dry_run)
            totals["claims"] += result["claims"]
            totals["study"] += result["study"]
            totals["outcomes"] += result["outcomes"]
        except Exception as e:
            print(f"\n  ERROR processing {record.resource_id}: {e}", file=sys.stderr)
            errors.append((record.resource_id, str(e)))

    # Summary
    print(f"\n{'='*70}")
    print(f"SUMMARY")
    print(f"{'='*70}")
    print(f"Records processed: {len(to_process) - len(errors)}/{len(to_process)}")
    if not args.dry_run:
        total_files = totals["claims"] + totals["study"] + totals["outcomes"]
        print(f"Files generated:   {total_files} ({totals['claims']} claims, {totals['study']} studies, {totals['outcomes']} outcomes)")
        print(f"Output directory:  {output_dir}")
    if errors:
        print(f"\nErrors ({len(errors)}):")
        for rid, err in errors:
            print(f"  {rid}: {err}")


if __name__ == "__main__":
    main()
