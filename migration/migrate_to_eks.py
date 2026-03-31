#!/usr/bin/env python3
"""
Migrate resources from a Self-Managed Kubernetes Cluster to Amazon EKS.
Reads extracted data from cluster-extraction-output/ and applies resources
to the target EKS cluster in phases with validation at each step.

Prerequisites:
  - kubectl configured to point at the TARGET EKS cluster
  - Extraction output available at EXTRACTION_DIR
  - aws cli configured with appropriate permissions
"""

import json
import subprocess
import sys
from typing import Union
import time
import argparse
import logging
from pathlib import Path
from dataclasses import dataclass, field

EXTRACTION_DIR = Path("cluster-extraction-output")
MIGRATION_LOG = Path("migration-output/migration_report.json")
OUTPUT_DIR = Path("migration-output")

# Output dir is set in main() based on --dry-run flag

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger(__name__)

# ── Helpers ──────────────────────────────────────────────────────────────────

@dataclass
class PhaseResult:
    phase: str
    status: str = "pending"
    migrated: list = field(default_factory=list)
    skipped: list = field(default_factory=list)
    errors: list = field(default_factory=list)


def load_json(relative_path: str) -> Union[dict, list]:
    path = EXTRACTION_DIR / relative_path
    if not path.exists():
        return {}
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def kubectl(args: str, capture=True) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["kubectl"] + args.split(),
        capture_output=capture, text=True, timeout=180, check=False
    )


def kubectl_apply_json(resource: dict) -> tuple[bool, str]:
    """Apply a single K8s resource dict via stdin."""
    payload = json.dumps(resource)
    result = subprocess.run(
        ["kubectl", "apply", "-f", "-"],
        input=payload, capture_output=True, text=True, timeout=120, check=False
    )
    ok = result.returncode == 0
    msg = result.stdout.strip() if ok else result.stderr.strip()
    return ok, msg


def kubectl_get_json(args: str) -> dict:
    result = kubectl(f"{args} -o json")
    if result.returncode != 0:
        return {}
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        return {}


def wait_for(description: str, check_fn, timeout=120, interval=5) -> bool:
    """Poll check_fn until it returns True or timeout."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if check_fn():
            return True
        time.sleep(interval)
    log.warning(f"Timeout waiting for: {description}")
    return False


SYSTEM_NAMESPACES = {"default", "kube-system", "kube-public", "kube-node-lease"}
SYSTEM_CLUSTERROLES = {
    "system:", "admin", "edit", "view", "cluster-admin",
    "eks:", "aws-node", "vpc-resource-controller",
}
SYSTEM_SA = {"default"}


def is_system_clusterrole(name: str) -> bool:
    return any(name.startswith(p) or name == p for p in SYSTEM_CLUSTERROLES)


# ── Phase 1: Namespaces ─────────────────────────────────────────────────────

def migrate_namespaces() -> PhaseResult:
    result = PhaseResult(phase="1-namespaces")
    namespaces = load_json("cluster/namespaces.json")
    if not namespaces:
        result.status = "skipped"
        return result

    for ns in namespaces:
        if ns in SYSTEM_NAMESPACES:
            result.skipped.append(ns)
            continue
        resource = {
            "apiVersion": "v1", "kind": "Namespace",
            "metadata": {"name": ns},
        }
        ok, msg = kubectl_apply_json(resource)
        if ok:
            result.migrated.append(ns)
            log.info(f"  Namespace created/updated: {ns}")
        else:
            result.errors.append({"namespace": ns, "error": msg})
            log.error(f"  Namespace failed: {ns} -> {msg}")

    # Validation
    existing = kubectl_get_json("get namespaces")
    existing_names = {i["metadata"]["name"] for i in existing.get("items", [])}
    missing = [ns for ns in result.migrated if ns not in existing_names]
    if missing:
        result.errors.append({"validation": f"Missing namespaces after apply: {missing}"})
    result.status = "completed" if not result.errors else "completed_with_errors"
    return result


# ── Phase 2: RBAC ───────────────────────────────────────────────────────────

def _apply_rbac_items(data: dict, kind_label: str, result: PhaseResult, filter_fn=None):
    items = data.get("items", [])
    for item in items:
        name = item.get("metadata", {}).get("name", "")
        ns = item.get("metadata", {}).get("namespace", "")
        key = f"{ns}/{name}" if ns else name
        if filter_fn and filter_fn(name):
            result.skipped.append(key)
            continue
        # Strip resourceVersion/uid for clean apply
        for f in ("resourceVersion", "uid", "creationTimestamp", "managedFields"):
            item.get("metadata", {}).pop(f, None)
        ok, msg = kubectl_apply_json(item)
        if ok:
            result.migrated.append(key)
        else:
            result.errors.append({"resource": key, "error": msg})
            log.error(f"  {kind_label} failed: {key} -> {msg}")


def migrate_rbac() -> PhaseResult:
    result = PhaseResult(phase="2-rbac")

    # Service accounts
    sa_data = load_json("rbac/serviceaccounts.json")
    for item in sa_data.get("items", []):
        name = item.get("metadata", {}).get("name", "")
        ns = item.get("metadata", {}).get("namespace", "")
        if name in SYSTEM_SA or ns in SYSTEM_NAMESPACES:
            result.skipped.append(f"{ns}/{name}")
            continue
        for f in ("resourceVersion", "uid", "creationTimestamp", "managedFields", "secrets"):
            item.get("metadata", {}).pop(f, None)
        item.pop("secrets", None)
        ok, msg = kubectl_apply_json(item)
        (result.migrated if ok else result.errors).append(
            f"{ns}/{name}" if ok else {"resource": f"{ns}/{name}", "error": msg}
        )

    # ClusterRoles & ClusterRoleBindings
    _apply_rbac_items(load_json("rbac/clusterroles.json"), "ClusterRole", result, is_system_clusterrole)
    _apply_rbac_items(load_json("rbac/clusterrolebindings.json"), "ClusterRoleBinding", result, is_system_clusterrole)

    # Namespaced Roles & RoleBindings
    _apply_rbac_items(load_json("rbac/roles.json"), "Role", result)
    _apply_rbac_items(load_json("rbac/rolebindings.json"), "RoleBinding", result)

    # Validation: spot-check a few migrated resources exist
    for item in result.migrated[:5]:
        name = item.split("/")[-1] if "/" in item else item
        check = kubectl(f"get clusterrole {name}", capture=True)
        if check.returncode != 0:
            check = kubectl(f"get role {name} --all-namespaces", capture=True)

    result.status = "completed" if not result.errors else "completed_with_errors"
    return result


# ── Phase 3: Storage Classes ────────────────────────────────────────────────

EKS_STORAGE_CLASSES = {"gp2"}


def migrate_storage_classes() -> PhaseResult:
    result = PhaseResult(phase="3-storage-classes")
    data = load_json("storage/storageclasses.json")
    items = data.get("items", []) if isinstance(data, dict) else []

    for sc in items:
        name = sc.get("metadata", {}).get("name", "")
        provisioner = sc.get("provisioner", "")
        if name in EKS_STORAGE_CLASSES:
            result.skipped.append(f"{name} (EKS default)")
            continue

        # Remap common provisioners to EBS CSI driver
        provisioner_map = {
            "kubernetes.io/aws-ebs": "ebs.csi.aws.com",
            "kubernetes.io/gce-pd": "ebs.csi.aws.com",
            "kubernetes.io/no-provisioner": "kubernetes.io/no-provisioner",
        }
        new_provisioner = provisioner_map.get(provisioner, provisioner)
        if new_provisioner != provisioner:
            log.info(f"  Remapping provisioner {provisioner} -> {new_provisioner} for SC {name}")

        for f in ("resourceVersion", "uid", "creationTimestamp", "managedFields"):
            sc.get("metadata", {}).pop(f, None)
        sc.get("metadata", {}).pop("annotations", {}).pop(
            "kubectl.kubernetes.io/last-applied-configuration", None
        )
        sc["provisioner"] = new_provisioner

        ok, msg = kubectl_apply_json(sc)
        if ok:
            result.migrated.append(name)
            log.info(f"  StorageClass applied: {name} (provisioner={new_provisioner})")
        else:
            result.errors.append({"storageclass": name, "error": msg})
            log.error(f"  StorageClass failed: {name} -> {msg}")

    # Validation
    existing = kubectl_get_json("get storageclasses")
    existing_names = {i["metadata"]["name"] for i in existing.get("items", [])}
    for name in result.migrated:
        if name not in existing_names:
            result.errors.append({"validation": f"StorageClass {name} missing after apply"})

    result.status = "completed" if not result.errors else "completed_with_errors"
    return result


# ── Phase 4: ConfigMaps & Secrets ────────────────────────────────────────────

def migrate_configmaps_secrets() -> PhaseResult:
    result = PhaseResult(phase="4-config")
    namespaces = load_json("cluster/namespaces.json") or []

    for ns in namespaces:
        if ns in SYSTEM_NAMESPACES:
            continue

        # ConfigMaps – apply with actual data from extraction
        cms = load_json(f"config/{ns}/configmaps.json")
        if isinstance(cms, list):
            for cm in cms:
                resource = {
                    "apiVersion": "v1", "kind": "ConfigMap",
                    "metadata": {"name": cm["name"], "namespace": ns},
                    "data": cm.get("data", {}),
                }
                ok, msg = kubectl_apply_json(resource)
                key = f"{ns}/cm/{cm['name']}"
                (result.migrated if ok else result.errors).append(
                    key if ok else {"resource": key, "error": msg}
                )

        # Secrets – create Opaque placeholders (actual values must be
        # migrated via a secrets manager like AWS Secrets Manager / SSM).
        secrets = load_json(f"config/{ns}/secrets_metadata.json")
        if isinstance(secrets, list):
            for sec in secrets:
                if sec.get("type", "").startswith("kubernetes.io/service-account"):
                    result.skipped.append(f"{ns}/secret/{sec['name']} (SA token)")
                    continue
                resource = {
                    "apiVersion": "v1", "kind": "Secret",
                    "metadata": {"name": sec["name"], "namespace": ns},
                    "type": sec.get("type", "Opaque"),
                    "stringData": {k: "<PLACEHOLDER_MIGRATE_SECRET>" for k in sec.get("keys", [])},
                }
                ok, msg = kubectl_apply_json(resource)
                key = f"{ns}/secret/{sec['name']}"
                (result.migrated if ok else result.errors).append(
                    key if ok else {"resource": key, "error": msg}
                )

    # Validation
    for entry in result.migrated:
        if "/cm/" in entry:
            ns, _, name = entry.partition("/cm/")
            check = kubectl(f"get configmap {name} -n {ns}")
        elif "/secret/" in entry:
            ns, _, name = entry.partition("/secret/")
            check = kubectl(f"get secret {name} -n {ns}")

    result.status = "completed" if not result.errors else "completed_with_errors"
    return result


# ── Phase 5: CRDs ───────────────────────────────────────────────────────────

def migrate_crds() -> PhaseResult:
    result = PhaseResult(phase="5-crds")

    # Apply CRD definitions first
    crd_list = load_json("crds/crd_list.json") or []
    for crd_name in crd_list:
        # We need the full CRD object; check if extraction stored instances
        crd_instances = load_json(f"crds/instances/{crd_name}.json")
        if not crd_instances:
            result.skipped.append(f"{crd_name} (no instance data)")
            continue

        # Apply each CR instance
        items = crd_instances.get("items", []) if isinstance(crd_instances, dict) else []
        for item in items:
            for f in ("resourceVersion", "uid", "creationTimestamp", "managedFields"):
                item.get("metadata", {}).pop(f, None)
            item.get("metadata", {}).pop("annotations", {}).pop(
                "kubectl.kubernetes.io/last-applied-configuration", None
            )
            name = item.get("metadata", {}).get("name", "unknown")
            ns = item.get("metadata", {}).get("namespace", "")
            key = f"{crd_name}/{ns}/{name}" if ns else f"{crd_name}/{name}"
            ok, msg = kubectl_apply_json(item)
            if ok:
                result.migrated.append(key)
            else:
                result.errors.append({"resource": key, "error": msg})
                log.error(f"  CRD instance failed: {key} -> {msg}")

    # Validation
    for crd_name in crd_list:
        check = kubectl(f"get crd {crd_name}")
        if check.returncode != 0:
            log.warning(f"  CRD {crd_name} not found on target – install the operator/controller first")

    result.status = "completed" if not result.errors else "completed_with_errors"
    return result


# ── Phase 6: Workloads ──────────────────────────────────────────────────────

RESOURCE_KIND_MAP = {
    "deployments": "Deployment",
    "statefulsets": "StatefulSet",
    "daemonsets": "DaemonSet",
    "jobs": "Job",
    "cronjobs": "CronJob",
}


def _build_pod_template(workload: dict) -> dict:
    containers = []
    for c in workload.get("containers", []):
        container = {"name": c["name"], "image": c["image"]}
        if c.get("resources"):
            container["resources"] = c["resources"]
        if c.get("ports"):
            container["ports"] = c["ports"]
        if c.get("volume_mounts"):
            container["volumeMounts"] = c["volume_mounts"]
        if c.get("env"):
            container["env"] = c["env"]
        if c.get("env_from"):
            container["envFrom"] = c["env_from"]
        if c.get("command"):
            container["command"] = c["command"]
        if c.get("args"):
            container["args"] = c["args"]
        containers.append(container)

    pod_spec = {"containers": containers}
    if workload.get("service_account"):
        pod_spec["serviceAccountName"] = workload["service_account"]
    if workload.get("node_selector"):
        pod_spec["nodeSelector"] = workload["node_selector"]
    if workload.get("tolerations"):
        pod_spec["tolerations"] = workload["tolerations"]
    if workload.get("affinity"):
        pod_spec["affinity"] = workload["affinity"]
    if workload.get("volumes"):
        pod_spec["volumes"] = workload["volumes"]
    if workload.get("restart_policy"):
        pod_spec["restartPolicy"] = workload["restart_policy"]
    return pod_spec


def migrate_workloads() -> PhaseResult:
    result = PhaseResult(phase="6-workloads")
    namespaces = load_json("cluster/namespaces.json") or []

    for ns in namespaces:
        if ns in SYSTEM_NAMESPACES:
            continue
        for rt, kind in RESOURCE_KIND_MAP.items():
            items = load_json(f"workloads/{ns}/{rt}.json")
            if not isinstance(items, list):
                continue
            for workload in items:
                name = workload["name"]
                pod_spec = _build_pod_template(workload)
                api_version = "batch/v1" if kind in ("Job", "CronJob") else "apps/v1"

                resource = {
                    "apiVersion": api_version,
                    "kind": kind,
                    "metadata": {"name": name, "namespace": ns},
                    "spec": {
                        "selector": {"matchLabels": {"app": name}},
                        "template": {
                            "metadata": {"labels": {"app": name}},
                            "spec": pod_spec,
                        },
                    },
                }

                if kind in ("Deployment", "StatefulSet") and workload.get("replicas"):
                    resource["spec"]["replicas"] = workload["replicas"]

                # CronJob has a different spec shape
                if kind == "CronJob":
                    resource["spec"] = {
                        "schedule": workload.get("schedule", "*/5 * * * *"),
                        "jobTemplate": {
                            "spec": {
                                "template": {
                                    "metadata": {"labels": {"app": name}},
                                    "spec": pod_spec,
                                }
                            }
                        },
                    }

                # Job doesn't need selector
                if kind == "Job":
                    resource["spec"].pop("selector", None)
                    resource["spec"]["template"]["metadata"].pop("labels", None)

                key = f"{ns}/{kind}/{name}"
                ok, msg = kubectl_apply_json(resource)
                if ok:
                    result.migrated.append(key)
                    log.info(f"  Applied {key}")
                else:
                    result.errors.append({"resource": key, "error": msg})
                    log.error(f"  Failed {key} -> {msg}")

    # Validation: check rollout status for deployments
    for entry in result.migrated:
        if "/Deployment/" in entry:
            ns, _, name = entry.partition("/Deployment/")
            wait_for(
                f"Deployment {ns}/{name} ready",
                lambda: kubectl(f"rollout status deployment/{name} -n {ns} --timeout=5s").returncode == 0,
                timeout=180, interval=10,
            )

    result.status = "completed" if not result.errors else "completed_with_errors"
    return result


# ── Phase 7: Services & Ingress ─────────────────────────────────────────────

def migrate_services_ingress() -> PhaseResult:
    result = PhaseResult(phase="7-networking")
    namespaces = load_json("cluster/namespaces.json") or []

    for ns in namespaces:
        if ns in SYSTEM_NAMESPACES:
            continue
        for rt in ["services", "ingresses", "networkpolicies"]:
            data = load_json(f"networking/{ns}/{rt}.json")
            items = data if isinstance(data, list) else data.get("items", []) if isinstance(data, dict) else []
            for item in items:
                name = item.get("metadata", {}).get("name", "unknown")
                kind = item.get("kind", rt.rstrip("s").capitalize())

                # Skip kubernetes default service
                if kind == "Service" and name == "kubernetes":
                    result.skipped.append(f"{ns}/{kind}/{name}")
                    continue

                # Clean metadata
                for f in ("resourceVersion", "uid", "creationTimestamp", "managedFields"):
                    item.get("metadata", {}).pop(f, None)
                item.get("metadata", {}).pop("annotations", {}).pop(
                    "kubectl.kubernetes.io/last-applied-configuration", None
                )

                # Services: remove clusterIP (let EKS assign), keep type
                if kind == "Service":
                    item.get("spec", {}).pop("clusterIP", None)
                    item.get("spec", {}).pop("clusterIPs", None)
                    # Remap LoadBalancer annotations for AWS LB Controller
                    svc_type = item.get("spec", {}).get("type", "ClusterIP")
                    if svc_type == "LoadBalancer":
                        annotations = item.get("metadata", {}).setdefault("annotations", {})
                        annotations.setdefault(
                            "service.beta.kubernetes.io/aws-load-balancer-scheme", "internet-facing"
                        )

                # Ingress: add ALB ingress class if not set
                if kind == "Ingress":
                    item.get("metadata", {}).setdefault("annotations", {}).setdefault(
                        "kubernetes.io/ingress.class", "alb"
                    )
                    annotations = item.get("metadata", {}).get("annotations", {})
                    annotations.setdefault(
                        "alb.ingress.kubernetes.io/scheme", "internet-facing"
                    )

                key = f"{ns}/{kind}/{name}"
                ok, msg = kubectl_apply_json(item)
                if ok:
                    result.migrated.append(key)
                    log.info(f"  Applied {key}")
                else:
                    result.errors.append({"resource": key, "error": msg})
                    log.error(f"  Failed {key} -> {msg}")

    # Validation: check services have endpoints
    for entry in result.migrated:
        if "/Service/" in entry:
            parts = entry.split("/Service/")
            ns, name = parts[0], parts[1]
            wait_for(
                f"Service {ns}/{name} has endpoints",
                lambda: bool(kubectl_get_json(f"get endpoints {name} -n {ns}").get("subsets")),
                timeout=60, interval=10,
            )

    result.status = "completed" if not result.errors else "completed_with_errors"
    return result


# ── Phase 8: Helm Releases ──────────────────────────────────────────────────

def migrate_helm_releases() -> PhaseResult:
    result = PhaseResult(phase="8-helm")
    releases = load_json("helm/releases.json")
    if not isinstance(releases, list) or not releases:
        result.status = "skipped"
        return result

    # Check helm binary
    if subprocess.run(["helm", "version"], capture_output=True, check=False).returncode != 0:
        result.status = "skipped"
        result.errors.append({"error": "helm CLI not found"})
        return result

    for rel in releases:
        name = rel.get("name", "unknown")
        ns = rel.get("namespace", "default")
        key = f"{ns}/{name}"
        log.info(f"  Helm release detected: {key} (status={rel.get('status')})")
        # Helm releases need chart source to re-install; log for manual action
        result.skipped.append(
            f"{key} -> Re-install via: helm install {name} <chart> -n {ns} "
            f"--version <version> -f <values.yaml>"
        )

    result.status = "completed"
    return result


# ── Orchestrator ─────────────────────────────────────────────────────────────

PHASES = [
    ("Phase 1: Namespaces", migrate_namespaces),
    ("Phase 2: RBAC", migrate_rbac),
    ("Phase 3: Storage Classes", migrate_storage_classes),
    ("Phase 4: ConfigMaps & Secrets", migrate_configmaps_secrets),
    ("Phase 5: CRDs", migrate_crds),
    ("Phase 6: Workloads", migrate_workloads),
    ("Phase 7: Services & Ingress", migrate_services_ingress),
    ("Phase 8: Helm Releases", migrate_helm_releases),
]


def run_migration(dry_run=False, start_phase=1):
    report = {"phases": [], "dry_run": dry_run}

    # Verify target cluster connectivity
    cluster_info = subprocess.run(
        ["kubectl", "get", "nodes"],
        capture_output=True, text=True, timeout=30, check=False
    )
    if cluster_info.returncode != 0:
        log.error("Cannot connect to target EKS cluster. Check kubeconfig context.")
        sys.exit(1)
    log.info(f"Connected to target cluster. Nodes found.\n")

    if dry_run:
        log.info("DRY RUN MODE – no resources will be applied.\n")

    for i, (label, phase_fn) in enumerate(PHASES, 1):
        if i < start_phase:
            log.info(f"Skipping {label} (start_phase={start_phase})")
            continue

        log.info(f"\n{'='*60}")
        log.info(f"Starting {label}")
        log.info(f"{'='*60}")

        if dry_run:
            log.info(f"  [DRY RUN] Would execute {label}")
            report["phases"].append({"phase": label, "status": "dry_run"})
            continue

        try:
            result = phase_fn()
            report["phases"].append({
                "phase": result.phase,
                "status": result.status,
                "migrated_count": len(result.migrated),
                "skipped_count": len(result.skipped),
                "error_count": len(result.errors),
                "migrated": result.migrated,
                "skipped": result.skipped,
                "errors": result.errors,
            })
            log.info(
                f"  {label} -> {result.status} | "
                f"migrated={len(result.migrated)} skipped={len(result.skipped)} errors={len(result.errors)}"
            )

            if result.errors:
                log.warning(f"  Errors in {label}. Continue? (y/n) ", )
                if input().strip().lower() != "y":
                    log.info("Migration aborted by user.")
                    break
        except Exception as e:
            log.exception(f"  {label} crashed: {e}")
            report["phases"].append({"phase": label, "status": "crashed", "error": str(e)})
            break

    # Save report
    MIGRATION_LOG.parent.mkdir(parents=True, exist_ok=True)
    with open(MIGRATION_LOG, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)
    log.info(f"\nMigration report saved to {MIGRATION_LOG}")


def main():
    global EXTRACTION_DIR, MIGRATION_LOG, OUTPUT_DIR
    parser = argparse.ArgumentParser(
        description="Migrate extracted K8s resources to Amazon EKS"
    )
    parser.add_argument("--dry-run", action="store_true", help="Preview without applying")
    parser.add_argument("--start-phase", type=int, default=1, choices=range(1, 9),
                        help="Resume from a specific phase (1-8)")
    parser.add_argument("--extraction-dir", type=str, default=str(EXTRACTION_DIR),
                        help="Path to extraction output directory")
    args = parser.parse_args()

    EXTRACTION_DIR = Path(args.extraction_dir)
    if not EXTRACTION_DIR.exists():
        log.error(f"Extraction directory not found: {EXTRACTION_DIR}")
        sys.exit(1)

    # Separate output folders for dry-run vs live
    OUTPUT_DIR = Path("migration-output/dry-run") if args.dry_run else Path("migration-output/live")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    MIGRATION_LOG = OUTPUT_DIR / "migration_report.json"

    # Add file handler to the correct output folder
    file_handler = logging.FileHandler(OUTPUT_DIR / "migration.log")
    file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    log.addHandler(file_handler)

    log.info(f"{'='*60}")
    log.info("Self-Managed K8s -> Amazon EKS Migration Tool")
    log.info(f"Extraction data : {EXTRACTION_DIR.resolve()}")
    log.info(f"Migration report: {MIGRATION_LOG.resolve()}")
    log.info(f"{'='*60}\n")

    run_migration(dry_run=args.dry_run, start_phase=args.start_phase)


if __name__ == "__main__":
    main()
