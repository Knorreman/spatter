# Deployment examples

These examples exercise the same-binary SPMD wordcount application, not a
general executor service. Three ranks mean one driver plus two worker processes.

## Kubernetes with kind

Requires a working Docker-backed kind cluster, kubectl and Cargo source checkout.
Use a dedicated cluster; do not replace an existing cluster for this test.

```sh
kind create cluster --name spatter-docs
docker build -f deploy/Dockerfile -t localhost/spatter:latest .
kind load docker-image localhost/spatter:latest --name spatter-docs
kubectl --context kind-spatter-docs create namespace spatter-test
printf 'hello world hello\nfoo bar foo\n' > /tmp/spatter-input.txt
kubectl --context kind-spatter-docs -n spatter-test create configmap wc-input \
  --from-file=input.txt=/tmp/spatter-input.txt
kubectl --context kind-spatter-docs -n spatter-test apply -f deploy/k8s/wordcount.yaml
kubectl --context kind-spatter-docs -n spatter-test get pods
kubectl --context kind-spatter-docs -n spatter-test logs spatter-0
```

Expected driver metrics: `ranks=3 keys=4 sum=6`. Inspect `--previous` logs if
the container has restarted. The StatefulSet restarts completed applications;
this is a smoke-test deployment, not a one-shot Kubernetes Job. ConfigMaps are
for small test input; larger data needs identical mounts on all ranks. Replicas
and `SPATTER_N` must match. CI defines a bounded polling check in
`.github/workflows/ci.yml`; Kubernetes CI success must be checked on GitHub.

```sh
kind delete cluster --name spatter-docs
```

## Podman kube play (single host)

```sh
podman build -f deploy/Dockerfile -t localhost/spatter:latest .
podman kube play deploy/k8s/podman-pod.yaml
podman logs spatter-pod-rank0
podman kube down deploy/k8s/podman-pod.yaml
```

This manifest embeds input with 38 keys and 76 words. All three containers
share one pod/network namespace and connect over localhost. It tests container
execution and the TCP protocol, **not Kubernetes scheduling, cross-pod DNS or
multi-host networking**. Its default restart policy can rerun completed ranks;
tear it down after inspecting logs.

Podman-backed kind is a separate setup and can have provider/CNI-specific
networking requirements. Host `ip_forward=0` alone does not establish the cause
of a rootless networking failure. Diagnose the relevant network namespace and
DNS/service reachability before changing host settings.
