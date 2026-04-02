# loom2nanopub

Generate [FORRT Knowledge Loom](https://w3id.org/np/RALIq4JelUP-q9BuWONcKMJ87B5n59ppcwhQjl-1dheO4) nanopublications from [TIB Knowledge Loom](https://knowledgeloom.tib.eu) records.

Takes a Knowledge Loom record (GitLab repo containing dtreg JSON-LD files) and generates unsigned nanopublication TriG files that can be signed and published to the nanopub network.

## Usage

```bash
# By record name
python loom2nanopub.py mills-2020-1-1

# By GitLab URL
python loom2nanopub.py https://gitlab.com/TIBHannover/lki/knowledge-loom/loom-records/mills-2020-1-1

# With options
python loom2nanopub.py mills-2020-1-1 --orcid 0000-0002-1784-2920 --name "Anne Fouilloux" -o output/
```

## What it generates

For each loom record:

1. **One FORRT-KL Replication Study** nanopub — aggregates all computational metadata (software methods, packages, runtime, input data, script URL, loom record link)
2. **One FORRT-KL Replication Outcome per analysis step** — each dtreg JSON-LD analysis becomes an outcome with machine-readable proof URL, analysis type, and key result

## Signing and publishing

The script generates unsigned TriG files. To sign and publish:

```bash
# Sign all generated files
for f in mills-2020-1-1-*.trig; do np sign -k ~/.nanopub/id_rsa "$f"; done

# Publish to the nanopub network
for f in mills-2020-1-1-*.trig; do np publish "$f"; done
```

Requires the [`np` CLI tool](https://github.com/Nanopublication/nanopub-java).

## Requirements

- Python 3.10+
- No external dependencies (uses only stdlib)

## FORRT-KL Templates

This tool generates nanopubs using the published FORRT Knowledge Loom templates:

- **Study**: [RALIq4JelUP-q9BuWONcKMJ87B5n59ppcwhQjl-1dheO4](https://w3id.org/np/RALIq4JelUP-q9BuWONcKMJ87B5n59ppcwhQjl-1dheO4)
- **Outcome**: [RAw3XdUhxQJfKBaU-cQhV6c7au4rLd5CSUdbMKTS_FB8g](https://w3id.org/np/RAw3XdUhxQJfKBaU-cQhV6c7au4rLd5CSUdbMKTS_FB8g)

## Links

- [TIB Knowledge Loom](https://knowledgeloom.tib.eu)
- [Knowledge Loom GitLab](https://gitlab.com/TIBHannover/lki/knowledge-loom)
- [dtreg paper](https://arxiv.org/html/2512.10836)
- [Science Live Platform](https://platform.sciencelive4all.org)
