"""
Terraform execution wrapper utilities.

Provides functions for:
- Running terraform init and apply
- Running terraform destroy
- Handling terraform errors and output
"""

import json
import subprocess
import sys
from pathlib import Path

from .generate_deployment_summary import generate_credentials_markdown


def _remove_stale_state_resources(env_path: Path, stale_resources: list[str]) -> None:
    """
    Remove stale resources from Terraform state that no longer exist in config.

    This prevents 401/404 errors during state refresh when resources have been
    removed from main.tf but still exist in terraform.tfstate.

    Args:
        env_path: Path to terraform directory
        stale_resources: List of resource addresses to remove (e.g. ["confluent_flink_statement.create_payments_passed"])
    """
    # Get current state list to check which stale resources actually exist
    try:
        result = subprocess.run(
            ["terraform", "state", "list"],
            cwd=env_path,
            capture_output=True,
            text=True,
            check=True,
        )
        existing = result.stdout.splitlines()
    except subprocess.CalledProcessError:
        # State may be empty or uninitialised — nothing to remove
        return

    for resource in stale_resources:
        if resource in existing:
            print(f"  Removing stale state entry: {resource}")
            subprocess.run(
                ["terraform", "state", "rm", resource],
                cwd=env_path,
                check=False,  # Non-fatal — apply will surface any real issues
            )


def run_terraform(env_path: Path, auto_approve: bool = True, clean_state: bool = False) -> bool:
    """
    Run terraform init and apply in the specified environment.

    Args:
        env_path: Path to terraform directory
        auto_approve: Whether to auto-approve terraform apply (default: True)
        clean_state: Whether to remove existing state files before running (default: False)

    Returns:
        True if successful, False otherwise

    Raises:
        SystemExit: If terraform binary is not found
    """
    if clean_state:
        print(f"\nCleaning existing Terraform state in {env_path}...")
        state_files = [
            env_path / "terraform.tfstate",
            env_path / "terraform.tfstate.backup",
            env_path / ".terraform.lock.hcl",
        ]
        for f in state_files:
            if f.exists():
                f.unlink()
                print(f"  Removed {f.name}")

    print(f"\nInitializing Terraform in {env_path}...")

    # Resources removed from config that may still linger in state.
    # Add entries here whenever a resource is deleted from a Terraform module.
    STALE_RESOURCES: dict[str, list[str]] = {
        # payment_fraud_pipeline state was wiped clean — nothing stale currently
    }

    try:
        subprocess.run(["terraform", "init"], cwd=env_path, check=True)

        # Remove any stale state entries before applying to avoid 401/404 refresh errors
        stale = STALE_RESOURCES.get(env_path.name, [])
        if stale:
            print(f"Checking for stale state entries in {env_path.name}...")
            _remove_stale_state_resources(env_path, stale)

        apply_cmd = ["terraform", "apply"]
        if auto_approve:
            apply_cmd.append("-auto-approve")

        print(f"Running terraform apply in {env_path}...")
        subprocess.run(apply_cmd, cwd=env_path, check=True)

        print(f"✓ Deployment successful: {env_path.name}")

        # Generate credentials markdown for Core deployments
        if env_path.name == "core":
            _generate_deployment_summary(env_path)

        return True

    except subprocess.CalledProcessError:
        print(f"✗ Terraform failed in {env_path.name}")
        return False
    except FileNotFoundError:
        print("Error: Terraform not found. Please install Terraform first.")
        sys.exit(1)


def run_terraform_destroy(env_path: Path, auto_approve: bool = True) -> bool:
    """
    Run terraform destroy in the specified environment.

    Args:
        env_path: Path to terraform directory
        auto_approve: Whether to auto-approve terraform destroy (default: True)

    Returns:
        True if successful, False otherwise

    Raises:
        SystemExit: If terraform binary is not found
    """
    print(f"\nInitializing Terraform in {env_path}...")

    try:
        subprocess.run(["terraform", "init"], cwd=env_path, check=True)

        destroy_cmd = ["terraform", "destroy"]
        if auto_approve:
            destroy_cmd.append("-auto-approve")

        print(f"Running terraform destroy in {env_path}...")
        subprocess.run(destroy_cmd, cwd=env_path, check=True)

        print(f"✓ Destroy successful: {env_path.name}")

        # Clean up deployment summary for Core deployments
        if env_path.name == "core":
            _cleanup_deployment_summary(env_path)

        return True

    except subprocess.CalledProcessError:
        print(f"✗ Terraform destroy failed in {env_path.name}")
        return False
    except FileNotFoundError:
        print("Error: Terraform not found. Please install Terraform first.")
        sys.exit(1)


def _generate_deployment_summary(env_path: Path) -> None:
    """
    Generate DEPLOYED_RESOURCES.md file after successful Core deployment.

    Args:
        env_path: Path to the terraform core directory (e.g., terraform/core)
    """
    try:
        # Get terraform outputs as JSON
        print("\nGenerating deployment summary...")
        result = subprocess.run(
            ["terraform", "output", "-json"],
            cwd=env_path,
            capture_output=True,
            text=True,
            check=True,
        )

        # Parse terraform outputs
        tf_outputs = json.loads(result.stdout)

        # Extract cloud_provider from terraform output, default to "aws"
        cloud_provider = "aws"
        if "cloud_provider" in tf_outputs:
            cloud_provider = (
                tf_outputs["cloud_provider"].get("value", "aws")
                if isinstance(tf_outputs["cloud_provider"], dict)
                else tf_outputs["cloud_provider"]
            )

        # Generate markdown file
        output_file = env_path / "DEPLOYED_RESOURCES.md"
        generate_credentials_markdown(cloud_provider, tf_outputs, output_file)

    except Exception as e:
        print(f"Warning: Failed to generate deployment summary: {e}")
        # Don't fail the deployment if summary generation fails


def _cleanup_deployment_summary(env_path: Path) -> None:
    """
    Delete DEPLOYED_RESOURCES.md file after successful Core destroy.

    Args:
        env_path: Path to the terraform core directory (e.g., terraform/core)
    """
    try:
        output_file = env_path / "DEPLOYED_RESOURCES.md"
        if output_file.exists():
            output_file.unlink()
            print(f"Removed {output_file}")
    except Exception as e:
        print(f"Warning: Failed to remove deployment summary: {e}")
