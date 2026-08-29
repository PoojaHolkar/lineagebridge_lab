# LineageBridge — TechXchange 2026 · Session 2973

Modern streaming pipelines have a lineage problem — Kafka topics, Flink SQL jobs, Schema Registry, and Tableflow Iceberg tables all evolve independently with no unified view of what feeds what. When something breaks, there is no map.

This lab fixes that. You will provision a live Confluent Cloud streaming pipeline, extract its full lineage graph with LineageBridge, and bridge the Tableflow Iceberg tables into IBM watsonx.data so they can be queried with Presto.

---

## What You Build

```text
Datagen → Kafka topics → Flink SQL → Tableflow (Iceberg)
                                           │
                              Spark bridge (tableflow_to_wxd.py)
                                           │
                              iceberg_catalog.lineage.* (watsonx.data)
                                           │
                                    Presto / watsonx BI
```

LineageBridge renders all of this — connectors, topics, Flink jobs, schemas, Tableflow tables — as an interactive graph at `http://localhost:8501`.

---

## Quick Start

```bash
git clone https://github.ibm.com/itz-content/txc-2026-lab-2973.git
cd txc-2026-lab-2973/lab-content
uv sync
uv run python3 setup.py          # enter API keys + your initials
cd terraform && terraform init && terraform apply
terraform output -json > /tmp/tf_out.json && cd ..
uv run python3 gen-env.py        # writes .env + patches tableflow_to_wxd.py
uv run streamlit run app.py      # opens http://localhost:8501
```

Full step-by-step instructions are in the **Lab Guide PDF** distributed with this session.

---

## Lab Content

All lab files are in [`lab-content/`](lab-content/).

| File | Purpose |
|------|---------|
| `setup.py` | Enter credentials + initials → creates `terraform.tfvars` |
| `gen-env.py` | Reads terraform outputs → writes `.env`, patches Spark script |
| `app.py` | Launches the LineageBridge Streamlit UI |
| `tableflow_to_wxd.py` | Spark job: bridges Tableflow → watsonx.data |
| `terraform/` | Provisions 22 Confluent Cloud resources |

---

## Prerequisites

- Confluent Cloud account · IBM watsonx.data instance
- `terraform >= 1.5` · `uv` · Confluent CLI

---

*IBM TechXchange 2026 · Session 2973*
