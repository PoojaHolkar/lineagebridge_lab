#!/usr/bin/env python3
"""
Lab setup — run this ONCE before `terraform init && terraform apply`.

Prompts for:
  - Your Confluent Cloud org-level API key + secret
  - Your initials / short handle (used to personalise resource names)

Writes terraform/terraform.tfvars so Terraform can provision a uniquely
named environment:  lb-<initials>-<8-char-hex>

Usage:
    uv run python3 setup.py
"""
import re
import sys
from pathlib import Path

TFVARS   = Path("terraform/terraform.tfvars")
TEMPLATE = """\
confluent_cloud_api_key    = "{key}"
confluent_cloud_api_secret = "{secret}"
participant_initials       = "{initials}"
cloud_region               = "us-east-1"
enable_tableflow           = true
"""


def prompt_initials() -> str:
    print()
    print("  Your initials will appear in the Confluent Cloud environment name")
    print("  so you can find your resources in a shared org.")
    print("  Example: 'poojah'  →  lb-poojah-81f290fa")
    print()
    while True:
        raw = input("  Your initials/handle (2-8 lowercase letters or digits): ").strip().lower()
        if re.match(r'^[a-z0-9]{2,8}$', raw):
            return raw
        print("  ✗ Must be 2-8 lowercase letters or digits. Try again.")


def main():
    if TFVARS.exists():
        print(f"✓ {TFVARS} already exists.")
        overwrite = input("  Overwrite? (y/N): ").strip().lower()
        if overwrite != "y":
            print("  Keeping existing file. Nothing changed.")
            sys.exit(0)

    print("=" * 55)
    print("  LineageBridge Lab — environment setup")
    print("=" * 55)
    print()
    print("  You need a Confluent Cloud org-level API key.")
    print("  Create one at: https://confluent.cloud → API Keys")
    print()

    key     = input("  Confluent Cloud API key:    ").strip()
    secret  = input("  Confluent Cloud API secret: ").strip()
    initials = prompt_initials()

    TFVARS.parent.mkdir(parents=True, exist_ok=True)
    TFVARS.write_text(TEMPLATE.format(key=key, secret=secret, initials=initials))

    print()
    print(f"✓ Written to {TFVARS}")
    print()
    print("  Your Confluent environment will be named:")
    print(f"    lb-{initials}-<random-hex>")
    print()
    print("  Next steps:")
    print("    cd terraform")
    print("    terraform init")
    print("    terraform apply")
    print("    terraform output -json > /tmp/tf_out.json")
    print("    cd ..")
    print("    uv run python3 gen-env.py")
    print("    uv run streamlit run app.py")


if __name__ == "__main__":
    main()
