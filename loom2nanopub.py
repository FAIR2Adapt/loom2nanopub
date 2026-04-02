#!/usr/bin/env python3
"""
Generate FORRT Knowledge Loom nanopublication TriG files from a TIB Knowledge Loom record.

Usage:
    python loom-to-forrt-kl.py <gitlab-repo-url-or-path>

Example:
    python loom-to-forrt-kl.py mills-2020-1-1
    python loom-to-forrt-kl.py https://gitlab.com/TIBHannover/lki/knowledge-loom/loom-records/mills-2020-1-1

Outputs unsigned TriG files that can be signed with the `np` CLI:
    np sign -k ~/.nanopub/id_rsa <file>.trig
    np publish <file>.trig
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


GITLAB_API = "https://gitlab.com/api/v4"
LOOM_GROUP = "TIBHannover/lki/knowledge-loom/loom-records"
SCIENCELIVE = "https://w3id.org/sciencelive/o/terms/"

# dtreg type DOI -> analysis type URI mapping
DTREG_TYPE_MAP = {
    "feeb33ad3e4440682a4d": f"{SCIENCELIVE}dtreg-DataAnalysis",
    "37182ecfb4474942e255": f"{SCIENCELIVE}dtreg-DataPreprocessing",
    "5b66cb584b974b186f37": f"{SCIENCELIVE}dtreg-DescriptiveStatistics",
    "b9335ce2c99ed87735a6": f"{SCIENCELIVE}dtreg-GroupComparison",
    "286991b26f02d58ee490": f"{SCIENCELIVE}dtreg-RegressionAnalysis",
    "3f64a93eef69d721518f": f"{SCIENCELIVE}dtreg-CorrelationAnalysis",
    "c6b413ba96ba477b5dca": f"{SCIENCELIVE}dtreg-MultilevelAnalysis",
    "6e3e29ce3ba5a0b9abfe": f"{SCIENCELIVE}dtreg-ClassPrediction",
    "c6e19df3b52ab8d855a9": f"{SCIENCELIVE}dtreg-ClassDiscovery",
    "5e782e67e70d0b2a022a": f"{SCIENCELIVE}dtreg-AlgorithmEvaluation",
    "437807f8d1a81b5138a3": f"{SCIENCELIVE}dtreg-FactorAnalysis",
}

# ePIC DOI hashes for analysis-level types (not sub-types like table, column, etc.)
ANALYSIS_TYPE_HASHES = set(DTREG_TYPE_MAP.keys())


@dataclass
class AnalysisInfo:
    """Extracted info from one dtreg JSON-LD analysis step."""
    label: str = ""
    method_label: str = ""           # e.g. "glmer"
    method_call: str = ""            # e.g. "lme4::glmer(...)"
    package_name: str = ""           # e.g. "lme4"
    package_version: str = ""        # e.g. "1.1-35.5"
    runtime_name: str = ""           # e.g. "R"
    runtime_version: str = ""        # e.g. "4.4.1"
    input_label: str = ""            # e.g. "Database containing..."
    input_source_url: str = ""       # URL to download data
    input_rows: int = 0
    input_cols: int = 0
    output_rows: int = 0
    output_cols: int = 0
    analysis_type_hash: str = ""     # ePIC hash of the analysis type
    json_file: str = ""              # source JSON-LD filename


@dataclass
class LoomRecord:
    """All metadata extracted from a Knowledge Loom record."""
    name: str = ""                   # e.g. "mills-2020-1-1"
    paper_doi: str = ""
    data_doi: str = ""
    script_url: str = ""
    gitlab_url: str = ""
    analyses: list = field(default_factory=list)


def gitlab_api_url(repo_path: str, endpoint: str) -> str:
    """Build a GitLab API URL for a repo."""
    encoded = urllib.parse.quote(f"{LOOM_GROUP}/{repo_path}", safe="")
    return f"{GITLAB_API}/projects/{encoded}/{endpoint}"


def fetch_json(url: str):
    """Fetch JSON from a URL."""
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read().decode())


def fetch_text(url: str) -> str:
    """Fetch text from a URL."""
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req) as resp:
        return resp.read().decode()


def extract_dois_from_script(repo_path: str) -> tuple[str, str]:
    """Extract paper and data DOIs from the R script comments."""
    # Find R scripts
    tree = fetch_json(gitlab_api_url(repo_path, "repository/tree?per_page=100&ref=main"))
    r_scripts = [f["name"] for f in tree if f["name"].endswith(".R")]

    paper_doi = ""
    data_doi = ""

    for script in r_scripts:
        encoded_path = urllib.parse.quote(script, safe="")
        url = gitlab_api_url(repo_path, f"repository/files/{encoded_path}/raw?ref=main")
        try:
            content = fetch_text(url)
        except Exception:
            continue

        for line in content.split("\n"):
            line_lower = line.lower().strip()
            if line_lower.startswith("#"):
                if "doi paper" in line_lower or "doi article" in line_lower:
                    match = re.search(r'https://doi\.org/[^\s"]+', line)
                    if match:
                        paper_doi = match.group(0)
                elif "doi data" in line_lower:
                    match = re.search(r'https://doi\.org/[^\s"]+', line)
                    if match:
                        data_doi = match.group(0)

    return paper_doi, data_doi


def extract_analysis_from_jsonld(data: dict, filename: str) -> list[AnalysisInfo]:
    """Extract analysis info from a dtreg JSON-LD object."""
    analyses = []

    def get_prop(obj, suffix):
        """Get a property value by matching the key suffix."""
        if not isinstance(obj, dict):
            return None
        for key, val in obj.items():
            if key.endswith(f"#{suffix}") or key == suffix:
                return val
        return None

    def extract_step(step: dict) -> AnalysisInfo | None:
        """Extract info from a single analysis step."""
        info = AnalysisInfo(json_file=filename)

        # Get the analysis type from @type
        step_type = step.get("@type", "")
        if step_type.startswith("doi:"):
            hash_val = step_type.split("doi:")[-1]
            if hash_val in ANALYSIS_TYPE_HASHES:
                info.analysis_type_hash = hash_val

        # Extract method info
        executes = get_prop(step, "executes")
        if isinstance(executes, dict):
            info.method_label = get_prop(executes, "label") or ""
            info.method_call = get_prop(executes, "is_implemented_by") or ""

            # Package info
            part_of = get_prop(executes, "part_of")
            if isinstance(part_of, dict):
                info.package_name = get_prop(part_of, "label") or ""
                info.package_version = get_prop(part_of, "version_info") or ""

                # Runtime info
                runtime = get_prop(part_of, "part_of")
                if isinstance(runtime, dict):
                    info.runtime_name = get_prop(runtime, "label") or ""
                    info.runtime_version = get_prop(runtime, "version_info") or ""

        # Extract input info
        has_input = get_prop(step, "has_input")
        if isinstance(has_input, dict):
            info.input_label = get_prop(has_input, "label") or ""
            info.input_source_url = get_prop(has_input, "source_url") or ""

            chars = get_prop(has_input, "has_characteristic")
            if isinstance(chars, dict):
                info.input_rows = get_prop(chars, "number_of_rows") or 0
                info.input_cols = get_prop(chars, "number_of_columns") or 0

        # Extract output info
        has_output = get_prop(step, "has_output")
        if isinstance(has_output, dict):
            chars = get_prop(has_output, "has_characteristic")
            if isinstance(chars, dict):
                info.output_rows = get_prop(chars, "number_of_rows") or 0
                info.output_cols = get_prop(chars, "number_of_columns") or 0

        # Build label
        method_str = f"{info.package_name}::{info.method_label}" if info.package_name else info.method_label
        info.label = method_str or filename

        return info if (info.method_label or info.analysis_type_hash) else None

    # The top-level is a data_analysis container
    has_part = get_prop(data, "has_part")
    if isinstance(has_part, dict):
        # Single analysis step
        result = extract_step(has_part)
        if result:
            analyses.append(result)
    elif isinstance(has_part, list):
        # Multiple analysis steps
        for step in has_part:
            if isinstance(step, dict):
                result = extract_step(step)
                if result:
                    analyses.append(result)
    else:
        # Try the top level itself as an analysis step
        result = extract_step(data)
        if result:
            analyses.append(result)

    return analyses


def fetch_loom_record(repo_path: str) -> LoomRecord:
    """Fetch and parse a complete loom record from GitLab."""
    record = LoomRecord(name=repo_path)
    record.gitlab_url = f"https://gitlab.com/{LOOM_GROUP}/{repo_path}"

    # Get DOIs from R script
    print(f"  Extracting DOIs from R scripts...")
    record.paper_doi, record.data_doi = extract_dois_from_script(repo_path)
    if record.paper_doi:
        print(f"    Paper DOI: {record.paper_doi}")
    if record.data_doi:
        print(f"    Data DOI: {record.data_doi}")

    # Get file list
    tree = fetch_json(gitlab_api_url(repo_path, "repository/tree?per_page=100&ref=main"))
    json_files = [f["name"] for f in tree if f["name"].endswith(".json")]

    # Find R script URL for the script field
    r_scripts = [f["name"] for f in tree if f["name"].endswith(".R")]
    if r_scripts:
        record.script_url = f"{record.gitlab_url}/-/blob/main/{r_scripts[0]}"

    # Parse each JSON-LD file
    print(f"  Parsing {len(json_files)} JSON-LD files...")
    for jf in json_files:
        encoded = urllib.parse.quote(jf, safe="")
        url = gitlab_api_url(repo_path, f"repository/files/{encoded}/raw?ref=main")
        try:
            data = fetch_json(url)
        except Exception as e:
            print(f"    Warning: could not parse {jf}: {e}")
            continue

        analyses = extract_analysis_from_jsonld(data, jf)
        if analyses:
            for a in analyses:
                print(f"    {jf}: {a.label} ({a.package_name} {a.package_version})")
            record.analyses.extend(analyses)

    print(f"  Found {len(record.analyses)} analysis steps")
    return record


def slugify(s: str) -> str:
    """Make a URL-safe slug."""
    return re.sub(r'[^a-zA-Z0-9_-]', '-', s.lower()).strip('-')[:60]


def generate_study_trig(record: LoomRecord, orcid: str, name: str) -> str:
    """Generate an unsigned FORRT-KL Replication Study TriG."""
    study_id = slugify(f"kl-study-{record.name}")
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")

    # Aggregate software info across all analyses
    methods = set()
    packages = set()
    runtimes = set()
    input_descs = set()
    input_urls = set()

    for a in record.analyses:
        if a.method_call:
            methods.add(a.method_call.split("\n")[0].strip())  # First line only
        elif a.method_label and a.package_name:
            methods.add(f"{a.package_name}::{a.method_label}()")
        if a.package_name:
            pkg = f"{a.package_name} {a.package_version}".strip()
            packages.add(pkg)
        if a.runtime_name:
            rt = f"{a.runtime_name} {a.runtime_version}".strip()
            runtimes.add(rt)
        if a.input_label:
            desc = a.input_label
            if a.input_rows and a.input_cols:
                desc += f" ({a.input_rows} rows x {a.input_cols} columns)"
            input_descs.add(desc)
        if a.input_source_url:
            input_urls.add(a.input_source_url)

    method_str = "; ".join(sorted(methods))
    package_str = "; ".join(sorted(packages))
    runtime_str = "; ".join(sorted(runtimes))
    input_desc_str = "; ".join(sorted(input_descs))

    # Build label
    label = f"Replication study of {record.name}"
    if record.paper_doi:
        label += f" ({record.paper_doi})"

    trig = f"""@prefix this: <http://purl.org/nanopub/temp/{study_id}> .
@prefix sub: <http://purl.org/nanopub/temp/{study_id}/> .
@prefix np: <http://www.nanopub.org/nschema#> .
@prefix dct: <http://purl.org/dc/terms/> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix nt: <https://w3id.org/np/o/ntemplate/> .
@prefix npx: <http://purl.org/nanopub/x/> .
@prefix prov: <http://www.w3.org/ns/prov#> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .
@prefix orcid: <https://orcid.org/> .
@prefix sciencelive: <{SCIENCELIVE}> .

sub:Head {{
  this: a np:Nanopublication ;
    np:hasAssertion sub:assertion ;
    np:hasProvenance sub:provenance ;
    np:hasPublicationInfo sub:pubinfo .
}}

sub:assertion {{
  sub:{study_id} a sciencelive:FORRT-Replication-Study ;
    rdfs:label "{_escape(label)}" ;
    rdf:type sciencelive:Reproduction-Study ;
    sciencelive:hasScopeDescription "Reproduction of analyses from Knowledge Loom record {record.name}" ;
    sciencelive:hasMethodologyDescription "Analyses reproduced using dtreg-generated machine-readable descriptions from the Knowledge Loom record." """

    if method_str:
        trig += f""";
    sciencelive:executesMethod "{_escape(method_str)}" """
    if package_str:
        trig += f""";
    sciencelive:usesSoftwarePackage "{_escape(package_str)}" """
    if runtime_str:
        trig += f""";
    sciencelive:hasRuntimeEnvironment "{_escape(runtime_str)}" """
    if input_desc_str:
        trig += f""";
    sciencelive:hasInputDataDescription "{_escape(input_desc_str)}" """
    if input_urls:
        for url in sorted(input_urls):
            trig += f""";
    sciencelive:hasInputDataSource <{url}> """
    if record.script_url:
        trig += f""";
    sciencelive:hasAnalysisScript <{record.script_url}> """
    if record.gitlab_url:
        trig += f""";
    sciencelive:hasLoomRecord <{record.gitlab_url}> """

    trig += f""".
}}

sub:provenance {{
  sub:assertion prov:wasAttributedTo orcid:{orcid} .
}}

sub:pubinfo {{
  orcid:{orcid} foaf:name "{_escape(name)}" .

  this: dct:created "{now}"^^xsd:dateTime ;
    dct:creator orcid:{orcid} ;
    dct:license <https://creativecommons.org/licenses/by/4.0/> ;
    rdfs:label "{_escape(label)}" ;
    nt:wasCreatedFromTemplate <https://w3id.org/np/RALIq4JelUP-q9BuWONcKMJ87B5n59ppcwhQjl-1dheO4> .
}}
"""
    return trig


def generate_outcome_trig(record: LoomRecord, analysis: AnalysisInfo,
                          index: int, orcid: str, name: str) -> str:
    """Generate an unsigned FORRT-KL Replication Outcome TriG for one analysis step."""
    outcome_id = slugify(f"kl-outcome-{record.name}-{index}")
    study_id = slugify(f"kl-study-{record.name}")
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    method_str = f"{analysis.package_name}::{analysis.method_label}" if analysis.package_name else analysis.method_label
    label = f"Outcome: {method_str} from {record.name}"

    # Build key result from output dimensions
    key_result = ""
    if analysis.output_rows and analysis.output_cols:
        key_result = f"Output: {analysis.output_rows} rows x {analysis.output_cols} columns"

    # Map analysis type
    analysis_type_uri = DTREG_TYPE_MAP.get(analysis.analysis_type_hash, "")

    # JSON-LD proof URL
    proof_url = f"{record.gitlab_url}/-/raw/main/{analysis.json_file}" if analysis.json_file else ""

    trig = f"""@prefix this: <http://purl.org/nanopub/temp/{outcome_id}> .
@prefix sub: <http://purl.org/nanopub/temp/{outcome_id}/> .
@prefix np: <http://www.nanopub.org/nschema#> .
@prefix dct: <http://purl.org/dc/terms/> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix nt: <https://w3id.org/np/o/ntemplate/> .
@prefix npx: <http://purl.org/nanopub/x/> .
@prefix prov: <http://www.w3.org/ns/prov#> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .
@prefix orcid: <https://orcid.org/> .
@prefix schema: <http://schema.org/> .
@prefix sciencelive: <{SCIENCELIVE}> .

sub:Head {{
  this: a np:Nanopublication ;
    np:hasAssertion sub:assertion ;
    np:hasProvenance sub:provenance ;
    np:hasPublicationInfo sub:pubinfo .
}}

sub:assertion {{
  sub:{outcome_id} a sciencelive:FORRT-Replication-Outcome ;
    rdfs:label "{_escape(label)}" ;
    sciencelive:hasOutcomeRepository <{record.gitlab_url}> ;
    schema:endDate "{today}"^^xsd:date ;
    sciencelive:hasValidationStatus sciencelive:Validated ;
    sciencelive:hasConfidenceLevel sciencelive:HighConfidence ;
    sciencelive:hasConclusionDescription "Analysis reproduced from Knowledge Loom record {record.name} using {method_str}." ;
    sciencelive:hasEvidenceDescription "Machine-readable analysis proof available as dtreg JSON-LD in the Knowledge Loom record." """

    if proof_url:
        trig += f""";
    sciencelive:hasMachineReadableProof <{proof_url}> """
    if analysis_type_uri:
        trig += f""";
    sciencelive:hasAnalysisType <{analysis_type_uri}> """
    if key_result:
        trig += f""";
    sciencelive:hasKeyResult "{_escape(key_result)}" """

    trig += f""".
}}

sub:provenance {{
  sub:assertion prov:wasAttributedTo orcid:{orcid} .
}}

sub:pubinfo {{
  orcid:{orcid} foaf:name "{_escape(name)}" .

  this: dct:created "{now}"^^xsd:dateTime ;
    dct:creator orcid:{orcid} ;
    dct:license <https://creativecommons.org/licenses/by/4.0/> ;
    rdfs:label "{_escape(label)}" ;
    nt:wasCreatedFromTemplate <https://w3id.org/np/RAw3XdUhxQJfKBaU-cQhV6c7au4rLd5CSUdbMKTS_FB8g> .
}}
"""
    return trig


def _escape(s: str) -> str:
    """Escape a string for use in TriG literals."""
    return s.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def main():
    parser = argparse.ArgumentParser(
        description="Generate FORRT-KL nanopub TriG files from a Knowledge Loom record"
    )
    parser.add_argument(
        "record",
        help="Loom record name (e.g. mills-2020-1-1) or full GitLab URL",
    )
    parser.add_argument(
        "--orcid", default="0000-0002-1784-2920",
        help="ORCID of the creator (default: Anne Fouilloux)",
    )
    parser.add_argument(
        "--name", default="Anne Fouilloux",
        help="Name of the creator",
    )
    parser.add_argument(
        "--output-dir", "-o", default=".",
        help="Output directory for TriG files",
    )

    args = parser.parse_args()

    # Parse the record name from URL if needed
    record_name = args.record
    if "gitlab.com" in record_name:
        record_name = record_name.rstrip("/").split("/")[-1]

    print(f"Fetching loom record: {record_name}")
    record = fetch_loom_record(record_name)

    if not record.analyses:
        print("No analyses found in this record.")
        sys.exit(1)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Generate study TriG
    study_file = output_dir / f"{record_name}-study.trig"
    study_trig = generate_study_trig(record, args.orcid, args.name)
    study_file.write_text(study_trig)
    print(f"\nGenerated: {study_file}")

    # Generate outcome TriG for each analysis
    for i, analysis in enumerate(record.analyses):
        outcome_file = output_dir / f"{record_name}-outcome-{i+1}.trig"
        outcome_trig = generate_outcome_trig(record, analysis, i+1, args.orcid, args.name)
        outcome_file.write_text(outcome_trig)
        print(f"Generated: {outcome_file}")

    print(f"\nDone! Generated {1 + len(record.analyses)} TriG files.")
    print(f"\nTo sign and publish:")
    print(f"  for f in {record_name}-*.trig; do np sign -k ~/.nanopub/id_rsa $f; done")
    print(f"  for f in {record_name}-*.trig; do np publish $f; done")


if __name__ == "__main__":
    main()
