"""Streamlit entry point for the LineageBridge interface.

    uv run streamlit run app.py

LineageBridge ships its interface inside the installed package, so there is no
app file in this repository to point Streamlit at. This shim executes that
module in Streamlit's script context.

Two things it fixes:

1. The `lineage-bridge-ui` console script that upstream installs does not work.
   It does `from lineage_bridge.ui.app import run`, and that module builds the
   whole interface at import time, which fails outside a Streamlit script run.

2. Nothing in LineageBridge calls `load_dotenv`. Its Settings object reads
   `.env` on its own, but the welcome dialog checks `os.getenv` directly and
   would report missing credentials even with a filled-in `.env`. Loading the
   file into the process environment here keeps both paths agreeing.
"""

import runpy

from dotenv import load_dotenv

# Silently does nothing when .env is absent, which is the demo-graph path.
load_dotenv(".env")

runpy.run_module("lineage_bridge.ui.app", run_name="__main__")
