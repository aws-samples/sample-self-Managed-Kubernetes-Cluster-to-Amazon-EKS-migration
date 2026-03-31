#!/usr/bin/env python3
"""
Rollback / Cleanup resources migrated to an Amazon EKS cluster.

Reads the migration report produced by migrate_to_eks.py and deletes
every resource that was successfully migrated, in reverse phase order
so that dependents are removed before the things they depend on
(e.g. workloads before namespaces).

Usage:
  # Preview what would be deleted (safe)
  python rollback_eks_migration.py --dry-run

  # Actually rollback
  python rollback_eks_migration.py

  # Use a custom report path
  python rollback_eks_migration.py --report migration-output/live/migration_report.json

  # Skip the interactive confirmation prompt
  python rollback_eks_migration.py --yes
"""

import json
import subprocess
import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import List

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger(__name__)


@dataclass
class RollbackResult:
    phase: str
    deleted: list = field(default_factory=list)
    skipped: list = field(default_factory=list)
    errors: list = field(default_factory=list)


# ── Helpers ──────────────────────────────────────────────────────────────────

def kubectl_delete(resource_type: str, name: str, namespace: str = None) -> tuple:
    """Delete a single Kubernetes resource. Returns (ok, message)."""
    cmd = ["kubectl", "delete", resource_type, name, "--ignore-not-found", "--wait=true", "--timeout=60s"]
    if namespace:
        cmd += ["-n", namespace]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120, check=False)
    ok = result.returncode == 0
    msg = result.stdout.strip() if ok else result.stderr.strip()
    return ok, msg


def resource_exists(resource_type: str, name: str, namespace: str = None) -> bool:
    cmd = ["kubectl", "get", resource_type, name]
    if namespace:
        cmd += ["-n", namespace]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30, check=False)
    return result.returncode == 0


# ── Phase-specific rollback handlers ─────────────────────────────────────────
# Each handler parses the "migrated" list from its phase in the migration report
# and issues the appropriate kubectl delete commands.

def rollback_networking(migrated: List[str], dry_run: bool) -> RollbackResult:
    """Phase 7: Services, Ingresses, NetworkPolicies."""
    result = RollbackResult(phase="7-networking")
    kind_map = {
        "Service": "service",
        "Ingress": "ingress",
        "Ingresse": "ingress",
        "NetworkPolicy": "networkpolicy",
        "NetworkPolicie": "networkpolicy",
    }
    for entry in migrated:
        # Format: ns/Kind/name
        parts = entry.split("/")
        if len(parts) != 3:
            result.skipped.append(f"{entry} (unexpected format)")
            continue
        ns, kind, name = parts
        k8s_type = kind_map.get(kind, kind.lower())
        if dry_run:
            log.info(f"  [DRY RUN] Would delete {k8s_type}/{name} -n {ns}")
            result.deleted.append(entry)
            continue
        ok, msg = kubectl_delete(k8s_type, name, ns)
        if ok:
            result.deleted.append(entry)
            log.info(f"  Deleted {k8s_type}/{name} -n {ns}")
        else:
            result.errors.append({"resource": entry, "error": msg})
            log.error(f"  Failed to delete {entry}: {msg}")
    return result


def rollback_workloads(migrated: List[str], dry_run: bool) -> RollbackResult:
    """Phase 6: Deployments, StatefulSets, DaemonSets, Jobs, CronJobs."""
    result = RollbackResult(phase="6-workloads")
    kind_map = {
        "Deployment": "deployment",
        "StatefulSet": "statefulset",
        "DaemonSet": "daemonset",
        "Job": "job",
        "CronJob": "cronjob",
    }
    for entry in migrated:
        parts = entry.split("/")
        if len(parts) != 3:
            result.skipped.append(f"{entry} (unexpected format)")
            continue
        ns, kind, name = parts
        k8s_type = kind_map.get(kind, kind.lower())
        if dry_run:
            log.info(f"  [DRY RUN] Would delete {k8s_type}/{name} -n {ns}")
            result.deleted.append(entry)
            continue
        ok, msg = kubectl_delete(k8s_type, name, ns)
        if ok:
            result.deleted.append(entry)
            log.info(f"  Deleted {k8s_type}/{name} -n {ns}")
        else:
            result.errors.append({"resource": entry, "error": msg})
            log.error(f"  Failed to delete {entry}: {msg}")
    return result


def rollback_crds(migrated: List[str], dry_run: bool) -> RollbackResult:
    """Phase 5: CRD instances."""
    result = RollbackResult(phase="5-crds")
    for entry in migrated:
        # Format: crd_name/ns/name  or  crd_name/name
        parts = entry.split("/")
        if len(parts) == 3:
            crd_type, ns, name = parts
            ns_flag = f"-n {ns}"
        elif len(parts) == 2:
            crd_type, name = parts
            ns_flag = ""
            ns = None
        else:
            result.skipped.append(f"{entry} (unexpected format)")
            continue
        if dry_run:
            log.info(f"  [DRY RUN] Would delete {crd_type} {name} {ns_flag}")
            result.deleted.append(entry)
            continue
        ok, msg = kubectl_delete(crd_type, name, ns)
        if ok:
            result.deleted.append(entry)
            log.info(f"  Deleted {crd_type} {name} {ns_flag}")
        else:
            result.errors.append({"resource": entry, "error": msg})
            log.error(f"  Failed to delete {entry}: {msg}")
    return result


def rollback_configmaps_secrets(migrated: List[str], dry_run: bool) -> RollbackResult:
    """Phase 4: ConfigMaps and Secrets."""
    result = RollbackResult(phase="4-config")
    for entry in migrated:
        # Format: ns/cm/name  or  ns/secret/name
        if "/cm/" in entry:
            ns, _, name = entry.partition("/cm/")
            k8s_type = "configmap"
        elif "/secret/" in entry:
            ns, _, name = entry.partition("/secret/")
            k8s_type = "secret"
        else:
            result.skipped.append(f"{entry} (unexpected format)")
            continue
        if dry_run:
            log.info(f"  [DRY RUN] Would delete {k8s_type}/{name} -n {ns}")
            result.deleted.append(entry)
            continue
        ok, msg = kubectl_delete(k8s_type, name, ns)
        if ok:
            result.deleted.append(entry)
            log.info(f"  Deleted {k8s_type}/{name} -n {ns}")
        else:
            result.errors.append({"resource": entry, "error": msg})
            log.error(f"  Failed to delete {entry}: {msg}")
    return result


def rollback_storage_classes(migrated: List[str], dry_run: bool) -> RollbackResult:
    """Phase 3: StorageClasses."""
    result = RollbackResult(phase="3-storage-classes")
    for name in migrated:
        if dry_run:
            log.info(f"  [DRY RUN] Would delete storageclass/{name}")
            result.deleted.append(name)
            continue
        ok, msg = kubectl_delete("storageclass", name)
        if ok:
            result.deleted.append(name)
            log.info(f"  Deleted storageclass/{name}")
        else:
            result.errors.append({"resource": name, "error": msg})
            log.error(f"  Failed to delete storageclass/{name}: {msg}")
    return result


def rollback_rbac(migrated: List[str], dry_run: bool) -> RollbackResult:
    """Phase 2: RBAC — ServiceAccounts, ClusterRoles, ClusterRoleBindings, Roles, RoleBindings."""
    result = RollbackResult(phase="2-rbac")
    # The migration report stores entries like "ns/name" for namespaced or just "name" for cluster-scoped.
    # We try each RBAC type in order until one succeeds.
    rbac_types_namespaced = ["serviceaccount", "role", "rolebinding"]
    rbac_types_cluster = ["clusterrole", "clusterrolebinding"]

    for entry in migrated:
        if "/" in entry:
            ns, name = entry.rsplit("/", 1)
            if dry_run:
                log.info(f"  [DRY RUN] Would delete RBAC resource {name} -n {ns}")
                result.deleted.append(entry)
                continue
            deleted = False
            for rt in rbac_types_namespaced:
                if resource_exists(rt, name, ns):
                    ok, msg = kubectl_delete(rt, name, ns)
                    if ok:
                        result.deleted.append(entry)
                        log.info(f"  Deleted {rt}/{name} -n {ns}")
                        deleted = True
                        break
            if not deleted:
                result.skipped.append(f"{entry} (not found or already removed)")
        else:
            name = entry
            if dry_run:
                log.info(f"  [DRY RUN] Would delete cluster RBAC resource {name}")
                result.deleted.append(entry)
                continue
            deleted = False
            for rt in rbac_types_cluster:
                if resource_exists(rt, name):
                    ok, msg = kubectl_delete(rt, name)
                    if ok:
                        result.deleted.append(entry)
                        log.info(f"  Deleted {rt}/{name}")
                        deleted = True
                        break
            if not deleted:
                result.skipped.append(f"{entry} (not found or already removed)")
    return result


def rollback_namespaces(migrated: List[str], dry_run: bool) -> RollbackResult:
    """Phase 1: Namespaces — deleted last so everything inside is cleaned up first."""
    result = RollbackResult(phase="1-namespaces")
    for ns in migrated:
        if dry_run:
            log.info(f"  [DRY RUN] Would delete namespace/{ns}")
            result.deleted.append(ns)
            continue
        ok, msg = kubectl_delete("namespace", ns)
        if ok:
            result.deleted.append(ns)
            log.info(f"  Deleted namespace/{ns}")
        else:
            result.errors.append({"resource": ns, "error": msg})
            log.error(f"  Failed to delete namespace/{ns}: {msg}")
    return result


# ── Rollback phases in reverse migration order ──────────────────────────────

ROLLBACK_HANDLERS = {
    "7-networking":      rollback_networking,
    "6-workloads":       rollback_workloads,
    "5-crds":            rollback_crds,
    "4-config":          rollback_configmaps_secrets,
    "3-storage-classes": rollback_storage_classes,
    "2-rbac":            rollback_rbac,
    "1-namespaces":      rollback_namespaces,
}

# Helm (phase 8) is skipped — the migration script only logs helm releases
# for manual re-install, so there's nothing to roll back automatically.

PHASE_ORDER = [
    "8-helm",
    "7-networking",
    "6-workloads",
    "5-crds",
    "4-config",
    "3-storage-classes",
    "2-rbac",
    "1-namespaces",
]


def run_rollback(report_path: Path, dry_run: bool = False):
    if not report_path.exists():
        log.error(f"Migration report not found: {report_path}")
        log.error("Run the migration script first, or pass --report <path>.")
        sys.exit(1)

    with open(report_path, encoding="utf-8") as f:
        report = json.load(f)

    # Build a lookup: phase_name -> migrated list
    phase_data = {}
    for phase in report.get("phases", []):
        phase_name = phase.get("phase", "")
        migrated = phase.get("migrated", [])
        if migrated:
            phase_data[phase_name] = migrated

    if not phase_data:
        log.info("Nothing to roll back — no migrated resources found in the report.")
        return

    log.info(f"{'='*60}")
    log.info("EKS Migration Rollback / Cleanup")
    log.info(f"Report : {report_path.resolve()}")
    log.info(f"Mode   : {'DRY RUN' if dry_run else 'LIVE'}")
    log.info(f"{'='*60}\n")

    total_resources = sum(len(v) for v in phase_data.values())
    log.info(f"Found {total_resources} migrated resources across {len(phase_data)} phases.\n")

    rollback_report = {"timestamp": datetime.now(timezone.utc).isoformat(), "dry_run": dry_run, "phases": []}

    for phase_name in PHASE_ORDER:
        if phase_name not in phase_data:
            continue
        handler = ROLLBACK_HANDLERS.get(phase_name)
        if not handler:
            log.info(f"Skipping {phase_name} (no automatic rollback handler)")
            continue

        migrated = phase_data[phase_name]
        log.info(f"\n{'='*60}")
        log.info(f"Rolling back {phase_name} ({len(migrated)} resources)")
        log.info(f"{'='*60}")

        try:
            result = handler(migrated, dry_run)
            rollback_report["phases"].append({
                "phase": result.phase,
                "deleted_count": len(result.deleted),
                "skipped_count": len(result.skipped),
                "error_count": len(result.errors),
                "deleted": result.deleted,
                "skipped": result.skipped,
                "errors": result.errors,
            })
            log.info(
                f"  {phase_name} -> deleted={len(result.deleted)} "
                f"skipped={len(result.skipped)} errors={len(result.errors)}"
            )
        except Exception as e:
            log.exception(f"  {phase_name} crashed: {e}")
            rollback_report["phases"].append({
                "phase": phase_name, "status": "crashed", "error": str(e)
            })

    # Save rollback report
    output_dir = report_path.parent
    rollback_report_path = output_dir / "rollback_report.json"
    with open(rollback_report_path, "w", encoding="utf-8") as f:
        json.dump(rollback_report, f, indent=2, default=str)
    log.info(f"\nRollback report saved to {rollback_report_path}")

    total_deleted = sum(p.get("deleted_count", 0) for p in rollback_report["phases"])
    total_errors = sum(p.get("error_count", 0) for p in rollback_report["phases"])
    if dry_run:
        log.info(f"\n[DRY RUN] Would have deleted {total_deleted} resources. No changes made.")
    else:
        log.info(f"\nRollback complete: {total_deleted} deleted, {total_errors} errors.")


def main():
    parser = argparse.ArgumentParser(
        description="Rollback / cleanup resources migrated to Amazon EKS"
    )
    parser.add_argument(
        "--report", type=str,
        default="migration-output/live/migration_report.json",
        help="Path to the migration report JSON (default: migration-output/live/migration_report.json)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Preview deletions without applying")
    parser.add_argument("--yes", "-y", action="store_true", help="Skip confirmation prompt")
    args = parser.parse_args()

    report_path = Path(args.report)

    if not args.dry_run and not args.yes:
        print(f"\n⚠️  This will DELETE all resources listed in: {report_path}")
        print("   Run with --dry-run first to preview.\n")
        confirm = input("Type 'rollback' to confirm: ").strip()
        if confirm != "rollback":
            print("Aborted.")
            sys.exit(0)

    run_rollback(report_path, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
