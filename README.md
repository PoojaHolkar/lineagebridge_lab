# Confluent lineage demo

This exercise creates a small Confluent Cloud environment, then uses
LineageBridge to extract and display its lineage. You export the resulting graph
and upload it to IBM watsonx.data intelligence by hand.

LineageBridge is a dependency here, not part of this repository. `uv sync`
installs it from <[https://github.com/takabayashi/lineage-bridge](https://github.com/takabayashi/lineage-bridge)>. What this
repository holds is the Terraform for the demo environment and the runbook.

The Terraform configuration creates billable Confluent resources. Complete the
teardown when you finish.

## 1. Set up Confluent Cloud

You need:

- a Confluent Cloud account that can create billable resources
- an organization-level Confluent Cloud API key and secret
- Git
- [uv](https://docs.astral.sh/uv/)
- Terraform 1.5 or later

### Get the repository

```bash
git clone <repository-url> atlanta-lineage
cd atlanta-lineage
```

### Add your Confluent Cloud credentials

Create an organization-level Cloud API key in Confluent Cloud. Use the
'My account' scope. A cluster-level key will not provision the demo.

Create `terraform/terraform.tfvars` with your credentials:

```hcl
confluent_cloud_api_key    = "<organization-level-key>"
confluent_cloud_api_secret = "<organization-level-secret>"
cloud_region               = "us-east-1"
enable_tableflow           = true
```

This file is git-ignored and never committed.

### Create the demo environment

```bash
cd terraform
terraform init
terraform apply
```

Review the plan and enter `yes` when Terraform asks for confirmation.
Provisioning usually takes 8 to 12 minutes.

Terraform creates:

- a Confluent environment and Standard Kafka cluster
- `orders` and `customers` topics with Avro schemas
- 2 Datagen source connectors
- a Flink compute pool
- Flink jobs that create `enriched_orders` and `order_stats` topics
- service-specific API keys for Kafka, Schema Registry, Flink and Tableflow
- a Tableflow sink that materialises three topics as Iceberg tables (see
  'The Tableflow sink' below)

The shared `confluent-core` module lives in `terraform/modules/confluent-core`.
It is vendored from the upstream LineageBridge repository, so `terraform init`
needs nothing outside this repository.

### Generate the .env file

After Terraform finishes, run from the repository root:

```bash
cd terraform && terraform output -json > /tmp/tf_out.json && cd ..
uv run python3 gen-env.py
```

This writes a complete `.env` with all credentials automatically. You do not
need to copy any values manually. The `.env` includes:

- the org-level Cloud API key (from `terraform.tfvars`)
- the cluster-scoped Kafka API key (`LINEAGE_BRIDGE_CLUSTER_CREDENTIALS`)
- Schema Registry endpoint and API key
- Flink API key
- Tableflow API key

To inspect individual values:

```bash
cd terraform
terraform output -raw schema_registry_api_key_secret
terraform output -raw kafka_api_key_secret
```

### The Tableflow sink

Terraform materialises `orders_v2`, `customers_v2` and `order_stats` as Iceberg
tables. Each one becomes a Tableflow table node in the graph, fed by a
`MATERIALIZES` edge from its topic, which extends the lineage past Kafka.

This is on by default and needs nothing extra from you. The tables are written
to Confluent-managed storage, so there is no AWS, Google Cloud or Azure account
involved and no bucket to create. Tableflow needs its own API key rather than
the Cloud key, and this configuration creates that key for you.

There is no catalog integration, because Glue, Unity Catalog and Snowflake Open
Catalog all require a second cloud account. The graph therefore ends at the
Tableflow table.

To leave the sink out, add this to `terraform/terraform.tfvars`:

```hcl
enable_tableflow = false
```

Materialisation is not instant. `terraform apply` returns once Confluent accepts
each topic, and the tables become queryable a few minutes later. Check with:

```bash
terraform output tableflow_tables
```

Extraction reads Tableflow through the Cloud API key, so `.env` needs no new
entries. If you would rather the extractor used the narrower key, add it:

```dotenv
LINEAGE_BRIDGE_TABLEFLOW_API_KEY=<tableflow_api_key_id>
LINEAGE_BRIDGE_TABLEFLOW_API_SECRET=<tableflow_api_key_secret>
```

`enriched_orders` is left out. Its `LEFT JOIN` makes it a changelog topic, and
the Glue demo this configuration follows does not materialise it either.

### Tear down the Confluent environment

Complete this step only after you finish the LineageBridge exercise in section 2.
Destroy the demo resources as soon as the exercise ends:

```bash
cd terraform
terraform destroy
```

> **Note:** Flink-created topics (`lineage_bridge.enriched_orders`,
> `lineage_bridge.order_stats`) may survive `terraform destroy` because Flink
> creates them as a side effect. If they appear in the Confluent UI after
> destroy, delete them manually before reprovisioning.

## 2. Set up LineageBridge

Run this from the repository root:

```bash
uv sync
```

That resolves LineageBridge from its Git repository into `.venv` and writes
`uv.lock`, so everyone gets the same upstream commit. To move to a newer one
later:

```bash
uv lock --upgrade-package lineage-bridge
```

Start the LineageBridge interface:

```bash
uv run streamlit run app.py
```

Open <http://localhost:8501> if your browser does not open automatically.

`app.py` is a three-line shim. LineageBridge ships its interface inside the
installed package, so there is no app file here for Streamlit to point at, and
the `lineage-bridge-ui` console script that upstream installs is broken: it
imports a module that builds the whole interface at import time, which fails
outside a Streamlit script run. The shim runs that module the way Streamlit
expects.

In LineageBridge:

1. Select the Terraform-created Confluent environment.
2. Run the extraction.
3. Explore the generated lineage graph.

A typical extraction shows 13 nodes and 13 edges. These include topics,
connectors, Flink jobs, schemas and an external dataset. The Tableflow sink
adds 3 more of each, for 16 and 16.

Clicking a node focuses the graph on its neighbours. Select 'Clear focus' in
the sidebar to restore the full graph.

Stop LineageBridge with `Ctrl+C` in its terminal.

## 3. Hand the graph to watsonx

Upstream LineageBridge publishes to Databricks Unity Catalog, AWS Glue, AWS
DataZone and Google Data Lineage. It has no watsonx target, so the handoff is
manual:

1. In the interface, use the Export JSON button to download the graph.
2. Upload that file to watsonx.data intelligence.

The exported file is LineageBridge's own graph format, `{"nodes": [...],
"edges": [...]}`, not OpenLineage. Convert it before uploading if watsonx wants
OpenLineage run events.

### Checking watsonx ingestion first

`watsonx_lineage_repro.py` checks whether a watsonx account can accept
OpenLineage over HTTP at all, before you build a graph to send it. It exchanges
the IBM Cloud API key for a bearer token, confirms the tenant has lineage
storage configured, and posts one event to both the single and batch endpoints.

Set `LINEAGE_BRIDGE_WATSONX_HOST` and `LINEAGE_BRIDGE_WATSONX_API_KEY` in
`.env`, then:

```bash
uv run --with requests --env-file .env watsonx_lineage_repro.py
```

Add `--events sample_events.json` to send a real captured batch instead of the
built-in sample event. The script needs only Python 3.9 and the standard
library, so it also runs outside this repository.

See `pyproject.toml` to swap the lineage-bridge source if needed.

## 4. Running it in Docker instead

Sections 1 to 3 assume Terraform and uv on your own machine. `docker/` does the
same work in two containers, so the only prerequisite is Docker.

- `terraform` provisions the Confluent Cloud environment.
- `ui` runs the LineageBridge interface.

Both read the repository's `.env`. Generate it with `python3 gen-env.py` after
`terraform apply` as described in section 1. Both use host networking, so the
interface appears on `http://localhost:8501` with no port mapping.

Each service sits behind a Compose profile. A bare `docker compose up` therefore
starts nothing, and cannot bill you by accident.

```bash
cd docker

# Provision. Takes 8 to 12 minutes.
docker compose run --rm terraform

# Generate .env from Terraform outputs (run from repository root).
docker compose run --rm terraform output -json > /tmp/tf_out.json
uv run python3 gen-env.py

# Start the interface.
docker compose up ui
```

The `terraform` service mounts `terraform/` at `/workspace/terraform`. One
useful side effect: `terraform.tfstate` is written back to the host, so you can
tear the environment down from inside or outside Docker later.

The container runs as UID 1000 to keep that state file owned by you. If your
account uses a different ID:

```bash
DOCKER_UID=$(id -u) DOCKER_GID=$(id -g) docker compose run --rm terraform
```

Anything after the service name goes straight to `terraform`:

```bash
docker compose run --rm terraform plan
docker compose run --rm terraform destroy -auto-approve
```

The `ui` service installs LineageBridge from Git at build time, so the image
depends on nothing in this repository beyond `pyproject.toml` and `uv.lock`.
Rebuild it with `docker compose build ui` after changing either.

The Tableflow sink needs no extra credentials: Terraform creates its API key.

Two caveats. Host networking behaves differently on Docker Desktop for macOS and
Windows, where it has to be enabled in settings; without it, add
`ports: ["8501:8501"]` to the `ui` service and drop `network_mode: host` from
both. And `docker compose run` gives Terraform no TTY by default, which is why
the provisioning command already implies `-auto-approve`: it will not stop to
ask you to confirm the plan. Run `docker compose run --rm terraform plan` first
if you want to read it.
