#!/usr/bin/env python3
"""
Extract information from a Self-Managed Kubernetes Cluster.
Captures cluster config, workloads, networking, storage, RBAC, and custom resources
to prepare for migration to Amazon EKS.
"""

import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Union, List


OUTPUT_DIR = Path("cluster-extraction-output")

# System namespaces to exclude from extraction
SYSTEM_NAMESPACES = {
    "kube-system", "kube-public", "kube-node-lease", "kube-flannel"
}

# The default namespace is included but filtered — only user-created resources are extracted

# System RBAC prefixes/names to exclude
SYSTEM_RBAC_PREFIXES = ("system:", "kubeadm:", "flannel")
SYSTEM_RBAC_NAMES = {
    "admin", "edit", "view", "cluster-admin",
}

# System configmaps to exclude
SYSTEM_CONFIGMAPS = {"kube-root-ca.crt"}


def run_kubectl(args: str) -> Union[dict, list, str]:
    try:
        result = subprocess.run(
            ["kubectl"] + args.split() + ["-o", "json"],
            capture_output=True, text=True, timeout=120, check=False
        )
        if result.returncode != 0:
            print(f"  WARN: kubectl {args} -> {result.stderr.strip()}")
            return {}
        return json.loads(result.stdout)
    except (json.JSONDecodeError, subprocess.TimeoutExpired):
        return {}


def run_kubectl_raw(args: str) -> str:
    try:
        result = subprocess.run(
            ["kubectl"] + args.split(),
            capture_output=True, text=True, timeout=120, check=False
        )
        return result.stdout.strip()
    except subprocess.TimeoutExpired:
        return ""


def save(filename: str, data):
    filepath = OUTPUT_DIR / filename
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str)
    print(f"  Saved: {filepath}")


def is_system_rbac(name: str) -> bool:
    """Check if a RBAC resource name is a system/built-in resource."""
    if name in SYSTEM_RBAC_NAMES:
        return True
    for prefix in SYSTEM_RBAC_PREFIXES:
        if name.startswith(prefix):
            return True
    return False


def extract_cluster_info():
    print("[1/10] Extracting cluster version and node info...")
    version = run_kubectl("version")
    nodes = run_kubectl("get nodes")
    node_summary = []
    for node in (nodes.get("items") or []):
        meta = node.get("metadata", {})
        status = node.get("status", {})
        capacity = status.get("capacity", {})
        labels = meta.get("labels", {})
        node_summary.append({
            "name": meta.get("name"),
            "labels": labels,
            "taints": node.get("spec", {}).get("taints", []),
            "capacity": capacity,
            "allocatable": status.get("allocatable", {}),
            "os_image": status.get("nodeInfo", {}).get("osImage"),
            "container_runtime": status.get("nodeInfo", {}).get("containerRuntimeVersion"),
            "kubelet_version": status.get("nodeInfo", {}).get("kubeletVersion"),
            "instance_type": labels.get("node.kubernetes.io/instance-type", "unknown"),
        })
    save("cluster/version.json", version)
    save("cluster/nodes.json", node_summary)


def extract_namespaces():
    print("[2/10] Extracting namespaces...")
    ns = run_kubectl("get namespaces")
    all_names = [item["metadata"]["name"] for item in (ns.get("items") or [])]
    app_namespaces = [n for n in all_names if n not in SYSTEM_NAMESPACES]
    save("cluster/namespaces.json", app_namespaces)
    return app_namespaces


def extract_workloads(namespaces: List[str]):
    print("[3/10] Extracting workloads (deployments, statefulsets, daemonsets, jobs, cronjobs)...")
    resource_types = ["deployments", "statefulsets", "daemonsets", "jobs", "cronjobs"]
    for ns in namespaces:
        for rt in resource_types:
            data = run_kubectl(f"get {rt} -n {ns}")
            items = data.get("items", [])
            if items:
                extracted = []
                for item in items:
                    spec = item.get("spec", {})
                    template_spec = spec.get("template", {}).get("spec", {})
                    # CronJobs have an extra jobTemplate layer
                    if rt == "cronjobs":
                        job_spec = spec.get("jobTemplate", {}).get("spec", {})
                        template_spec = job_spec.get("template", {}).get("spec", {})
                    containers = template_spec.get("containers", [])
                    workload_data = {
                        "name": item["metadata"]["name"],
                        "replicas": spec.get("replicas"),
                        "containers": [{
                            "name": c.get("name"),
                            "image": c.get("image"),
                            "resources": c.get("resources", {}),
                            "env": c.get("env", []),
                            "env_from": c.get("envFrom", []),
                            "command": c.get("command", []),
                            "args": c.get("args", []),
                            "volume_mounts": c.get("volumeMounts", []),
                            "ports": c.get("ports", []),
                        } for c in containers],
                        "volumes": template_spec.get("volumes", []),
                        "node_selector": template_spec.get("nodeSelector"),
                        "tolerations": template_spec.get("tolerations", []),
                        "affinity": template_spec.get("affinity"),
                        "service_account": template_spec.get("serviceAccountName"),
                        "restart_policy": template_spec.get("restartPolicy"),
                    }
                    if rt == "cronjobs":
                        workload_data["schedule"] = spec.get("schedule", "*/5 * * * *")
                    extracted.append(workload_data)
                save(f"workloads/{ns}/{rt}.json", extracted)


def extract_services_and_ingress(namespaces: List[str]):
    print("[4/10] Extracting services, ingresses, and network policies...")
    for ns in namespaces:
        for rt in ["services", "ingresses", "networkpolicies"]:
            data = run_kubectl(f"get {rt} -n {ns}")
            items = data.get("items", [])
            if rt == "services":
                # Filter out the default kubernetes API service
                items = [i for i in items if i.get("metadata", {}).get("name") != "kubernetes"]
            if items:
                save(f"networking/{ns}/{rt}.json", items)


def extract_storage(namespaces: List[str]):
    print("[5/10] Extracting storage classes, PVs, and PVCs...")
    save("storage/storageclasses.json", run_kubectl("get storageclasses"))
    save("storage/persistentvolumes.json", run_kubectl("get pv"))
    for ns in namespaces:
        pvcs = run_kubectl(f"get pvc -n {ns}")
        if pvcs.get("items"):
            save(f"storage/{ns}/pvcs.json", pvcs)


def extract_configmaps_secrets(namespaces: List[str]):
    print("[6/10] Extracting configmaps and secret metadata...")
    for ns in namespaces:
        cms = run_kubectl(f"get configmaps -n {ns}")
        if cms.get("items"):
            # Filter out system configmaps, keep full data for app configmaps
            app_cms = [i for i in cms["items"] if i["metadata"]["name"] not in SYSTEM_CONFIGMAPS]
            if app_cms:
                save(f"config/{ns}/configmaps.json", [
                    {"name": i["metadata"]["name"], "data": i.get("data", {})}
                    for i in app_cms
                ])
        secrets = run_kubectl(f"get secrets -n {ns}")
        if secrets.get("items"):
            # Filter out SA token secrets — only capture metadata, never values
            app_secrets = [i for i in secrets["items"]
                          if not i.get("type", "").startswith("kubernetes.io/service-account")]
            if app_secrets:
                save(f"config/{ns}/secrets_metadata.json", [
                    {"name": i["metadata"]["name"], "type": i.get("type"),
                     "keys": list(i.get("data", {}).keys())}
                    for i in app_secrets
                ])


def extract_rbac():
    print("[7/10] Extracting RBAC (roles, bindings, service accounts)...")

    # ClusterRoles — filter out system roles
    cr_data = run_kubectl("get clusterroles")
    if cr_data.get("items"):
        app_items = [i for i in cr_data["items"]
                     if not is_system_rbac(i["metadata"]["name"])]
        cr_data["items"] = app_items
    save("rbac/clusterroles.json", cr_data)

    # ClusterRoleBindings — filter out system bindings
    crb_data = run_kubectl("get clusterrolebindings")
    if crb_data.get("items"):
        app_items = [i for i in crb_data["items"]
                     if not is_system_rbac(i["metadata"]["name"])]
        crb_data["items"] = app_items
    save("rbac/clusterrolebindings.json", crb_data)

    # Roles — filter out system namespaces
    roles_data = run_kubectl("get roles --all-namespaces")
    if roles_data.get("items"):
        app_items = [i for i in roles_data["items"]
                     if i.get("metadata", {}).get("namespace") not in SYSTEM_NAMESPACES]
        roles_data["items"] = app_items
    save("rbac/roles.json", roles_data)

    # RoleBindings — filter out system namespaces
    rb_data = run_kubectl("get rolebindings --all-namespaces")
    if rb_data.get("items"):
        app_items = [i for i in rb_data["items"]
                     if i.get("metadata", {}).get("namespace") not in SYSTEM_NAMESPACES]
        rb_data["items"] = app_items
    save("rbac/rolebindings.json", rb_data)

    # ServiceAccounts — filter out system namespaces and default SA
    sa_data = run_kubectl("get serviceaccounts --all-namespaces")
    if sa_data.get("items"):
        app_items = [i for i in sa_data["items"]
                     if i.get("metadata", {}).get("namespace") not in SYSTEM_NAMESPACES
                     and i["metadata"]["name"] != "default"]
        sa_data["items"] = app_items
    save("rbac/serviceaccounts.json", sa_data)


def extract_crds():
    print("[8/10] Extracting Custom Resource Definitions...")
    crds = run_kubectl("get crds")
    crd_names = [i["metadata"]["name"] for i in (crds.get("items") or [])]
    save("crds/crd_list.json", crd_names)
    for crd in crd_names:
        data = run_kubectl(f"get {crd} --all-namespaces")
        if data.get("items"):
            save(f"crds/instances/{crd}.json", data)


def extract_helm_releases():
    print("[9/10] Extracting Helm releases...")
    output = run_kubectl_raw("get secrets --all-namespaces -l owner=helm -o json")
    try:
        data = json.loads(output)
        releases = []
        for item in data.get("items", []):
            labels = item.get("metadata", {}).get("labels", {})
            releases.append({
                "name": labels.get("name"),
                "namespace": item["metadata"]["namespace"],
                "version": labels.get("version"),
                "status": labels.get("status"),
            })
        save("helm/releases.json", releases)
    except json.JSONDecodeError:
        save("helm/releases.json", [])


def extract_resource_quotas_limits(namespaces: List[str]):
    print("[10/10] Extracting resource quotas and limit ranges...")
    for ns in namespaces:
        for rt in ["resourcequotas", "limitranges"]:
            data = run_kubectl(f"get {rt} -n {ns}")
            if data.get("items"):
                save(f"policies/{ns}/{rt}.json", data)


def generate_summary():
    summary = {
        "extraction_time": datetime.now(timezone.utc).isoformat(),
        "output_directory": str(OUTPUT_DIR.resolve()),
        "sections": [
            "cluster/ - Version, nodes, namespaces",
            "workloads/ - Deployments, StatefulSets, DaemonSets, Jobs, CronJobs",
            "networking/ - Services, Ingresses, NetworkPolicies",
            "storage/ - StorageClasses, PVs, PVCs",
            "config/ - ConfigMaps, Secret metadata (no secret values)",
            "rbac/ - Roles, Bindings, ServiceAccounts",
            "crds/ - Custom Resource Definitions and instances",
            "helm/ - Helm releases",
            "policies/ - ResourceQuotas, LimitRanges",
        ]
    }
    save("SUMMARY.json", summary)


def main():
    print(f"{'='*60}")
    print("Self-Managed K8s Cluster Extraction Tool")
    print(f"Output: {OUTPUT_DIR.resolve()}")
    print(f"{'='*60}\n")

    # Verify kubectl connectivity
    if not run_kubectl_raw("cluster-info"):
        print("ERROR: Cannot connect to cluster. Check your kubeconfig.")
        sys.exit(1)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    extract_cluster_info()
    namespaces = extract_namespaces()
    extract_workloads(namespaces)
    extract_services_and_ingress(namespaces)
    extract_storage(namespaces)
    extract_configmaps_secrets(namespaces)
    extract_rbac()
    extract_crds()
    extract_helm_releases()
    extract_resource_quotas_limits(namespaces)
    generate_summary()

    print(f"\nExtraction complete! Output at: {OUTPUT_DIR.resolve()}")


if __name__ == "__main__":
    main()
