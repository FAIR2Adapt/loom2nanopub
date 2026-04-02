# loom2nanopub

Convert [TIB Knowledge Loom](https://knowledgeloom.tib.eu) records into [FORRT](https://forrt.org) nanopublications — semantic, signed, replicable assertions published to the [nanopub network](https://nanopub.net).

Each Knowledge Loom record becomes three types of nanopublications:
- **FORRT Claims** — one per statement (what was found)
- **FORRT-KL Replication Study** — the study design (software, methods, data, script)
- **FORRT-KL Replication Outcomes** — the results (actual values, metrics, visualizations)

## Record mapping

`loom_records.txt` maps all 32 Knowledge Loom records to their GitLab repos:

```
a03gjbxh5v  4  lezhnina-2025-1-1   Analysis of difference for Iris species
vjea9aobg7  2  paredes-2022-1      Determinants of B. oleae abundance  DONE
...
```

## Usage

### Jupyter notebooks

**Single record** — `notebooks/loom2nanopub.ipynb`

Interactive workflow with preview before each publish step:
1. Set `LOOM_RESOURCE_ID` in the Configuration cell
2. Run all cells top to bottom
3. Preview each TriG file before publishing
4. Mark the record as DONE in `loom_records.txt`

**Batch** — `notebooks/loom2nanopub_batch.ipynb`

Process all non-DONE records:
1. Run Setup cells (section 1)
2. Run Generate — creates TriG files for all records into `outputs/<ID>/`
3. Review the generated files
4. Set `PUBLISH_BATCH = True` and run Publish

### CLI script

```bash
# Single record
python loom2nanopub.py a03gjbxh5v

# All non-DONE records
python loom2nanopub.py --all

# Dry run (show what would be generated)
python loom2nanopub.py --all --dry-run

# Custom output directory
python loom2nanopub.py --all -o ./batch-output/
```

## What gets extracted

The tools parse [dtreg](https://arxiv.org/html/2512.10836) JSON-LD files from each record's [GitLab repo](https://gitlab.com/TIBHannover/lki/knowledge-loom/loom-records) and the Knowledge Loom API to produce nanopubs with:

| Field | Source | Nanopub type |
|-------|--------|-------------|
| Statement text | KL API | Claim |
| Source publication DOI | KL API (`basises`) | Claim |
| Data source description | dtreg `has_input` | Claim |
| Software packages + versions | dtreg `executes → part_of` | Study |
| Runtime environment | dtreg `part_of → part_of` | Study |
| Method calls | dtreg `executes → is_implemented_by` | Study |
| Input data descriptions | dtreg `has_input → label` | Study, Outcome |
| Input dataset URLs | `metadata.json` file_mapping | Study |
| Analysis script URL | dtreg top-level `is_implemented_by` | Study |
| Result values (actual numbers) | dtreg `has_output → source_table` | Outcome |
| Target variable | dtreg `targets → label` | Outcome |
| Result visualization URL | dtreg `has_output → has_expression` | Outcome |
| KL record link | KL URL | Study, Outcome |

## Publish order

Publishing must be sequential per record because each nanopub references the previous:

1. **Claims** — published first, resource URIs collected
2. **Study** — references claims via `targetsClaim`, study URI collected
3. **Outcomes** — reference study via `isOutcomeOf`

Both the batch notebook and CLI handle this automatically.

## Requirements

- Python 3.10+
- `rdflib`
- `nanopub` (Python library for signing and publishing)
- A nanopub profile (`~/.nanopub/` or custom path)

```bash
mamba activate sciencelive  # or your env with rdflib + nanopub
```

## FORRT-KL Templates

- **Claim**: [RAu5uTahAxc0OLBB3vaGwK3OQDDZV7QuWtDlBk0Ea3bco](https://w3id.org/np/RAu5uTahAxc0OLBB3vaGwK3OQDDZV7QuWtDlBk0Ea3bco)
- **Study**: [RALIq4JelUP-q9BuWONcKMJ87B5n59ppcwhQjl-1dheO4](https://w3id.org/np/RALIq4JelUP-q9BuWONcKMJ87B5n59ppcwhQjl-1dheO4)
- **Outcome**: [RAw3XdUhxQJfKBaU-cQhV6c7au4rLd5CSUdbMKTS_FB8g](https://w3id.org/np/RAw3XdUhxQJfKBaU-cQhV6c7au4rLd5CSUdbMKTS_FB8g)

## Links

- [TIB Knowledge Loom](https://knowledgeloom.tib.eu)
- [Knowledge Loom GitLab](https://gitlab.com/TIBHannover/lki/knowledge-loom)
- [dtreg paper](https://arxiv.org/html/2512.10836)
- [Science Live Platform](https://platform.sciencelive4all.org)
- [Nanopub network](https://nanopub.net)
