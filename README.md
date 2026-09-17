# ontap-cosi-driver

ONTAP COSI driver implementation for Container Object Storage Interface (COSI) API.

This driver is a modification of the example driver provided by the COSI team:
[kubernetes-sigs/cosi-driver-sample](https://github.com/kubernetes-sigs/cosi-driver-sample).

## Architecture: driver and sidecar

The deployment runs two containers in a single pod, sharing an `emptyDir`
volume at `/var/lib/cosi` that holds a unix socket (`cosi.sock`):

```
┌─────────────────────────────────────────────────────┐
│  Pod: ontap-cosi-driver (ns: ontap-cosi-driver-system)
│                                                     │
│  ┌──────────────────┐       ┌──────────────────┐   │
│  │   sidecar         │  gRPC  │   driver          │   │
│  │ (reconcilers)     │───────►│ (ProvisionerServer)│   │
│  │ watches K8s CRDs  │ unix   │ translates to     │   │
│  │ Bucket/BucketAccess│ socket │ ONTAP S3 REST API │   │
│  └──────────────────┘         └────────┬─────────┘   │
│                                        │             │
│                          /var/lib/cosi/cosi.sock     │
└────────────────────────────────────────┼─────────────┘
                                         │ HTTPS
                              ┌──────────▼──────────┐
                              │  ONTAP cluster or    │
                              │  ontap9-stub         │
                              │  (S3 REST API)       │
                              └─────────────────────┘
```

### Driver (`ontap-cosi-driver` container)

A stateless gRPC server implementing the COSI `Provisioner` interface — four
RPCs: `DriverCreateBucket`, `DriverDeleteBucket`, `DriverGrantBucketAccess`,
and `DriverRevokeBucketAccess`. It knows nothing about Kubernetes; its only
job is to translate those RPCs into ONTAP S3 REST API calls (POST/DELETE
buckets, create/delete users/groups/policies, poll async jobs). It listens on
the unix socket and is never reachable outside the pod.

### Sidecar (`objectstorage-provisioner-sidecar` container)

A controller-runtime manager that watches Kubernetes custom resources —
cluster-scoped `Bucket` objects and namespaced `BucketAccess` objects — and
reconciles them by calling the driver over the socket:

| Sidecar responsibility | What it does |
|---|---|
| **Finalizer management** | Adds `objectstorage.k8s.io/protection` finalizer on create; removes it only after the driver confirms backend deletion succeeded — preventing premature GC of the K8s object while backend resources still exist |
| **Create: `BucketReconciler`** | When a new `Bucket` appears, calls `DriverCreateBucket`, then writes `status.bucketID`, `status.protocols`, `status.bucketInfo` |
| **Delete: `BucketReconciler`** | When `deletionTimestamp` is set, calls `DriverDeleteBucket`; on success removes the finalizer so the `Bucket` object is garbage-collected; on failure writes `status.error` so it retries |
| **Create: `BucketAccessReconciler`** | When a new `BucketAccess` appears, calls `DriverGrantBucketAccess`, creates the user-specified Secret with `COSI_S3_*` credentials, writes `status.accountID` |
| **Delete: `BucketAccessReconciler`** | When `deletionTimestamp` is set, deletes owned Secrets, calls `DriverRevokeBucketAccess`, sets the `sidecar-cleanup-finished` annotation for handoff to the controller |

### Why two containers instead of one

This is the standard sidecar pattern used by CSI/COSI drivers:

1. **Separation of concerns** — The driver is a pure translation layer (COSI
   RPC → vendor REST API). The sidecar is pure Kubernetes machinery (watch
   CRDs, manage finalizers/status, create Secrets). This lets you swap drivers
   without touching reconciler logic and vice versa.

2. **Upstream-provided vs. vendor-specific** — The sidecar is generic COSI
   infrastructure maintained upstream
   (`kubernetes-sigs/container-object-storage-interface`). Every COSI vendor
   uses the same sidecar binary; only the driver is vendor-specific. In this
   project, the sidecar is a local branch (`tenant-namespace-injection`) of the
   upstream repo: bucket deletion now comes from upstream (merged PR #320), and
   the only local commit injects `cosi.tenantNamespace` into driver RPC
   parameters for multi-tenant credentials — but the architecture is still
   "one generic sidecar + one vendor driver."

3. **Unix socket boundary** — They share a pod so the gRPC connection is a
   loopback unix socket (`/var/lib/cosi/cosi.sock`), not a network endpoint.
   No TLS, no authentication, no network exposure.

### How this differs from the COSI controller

A third component in `container-object-storage-system` — the **COSI
controller** — is the user-facing layer. It watches `BucketClaim` /
`BucketAccess` objects (namespaced, user-created) and creates the
corresponding cluster-scoped `Bucket` / `BucketAccess` objects that the
sidecar watches. The full handoff:

```
User creates BucketClaim (ns: tenant-ns1)
  → Controller creates Bucket (cluster-scoped, bc-<uid>)
    → Sidecar sees Bucket, calls Driver over socket
      → Driver calls ONTAP S3 REST API
```

The sidecar is the bridge between Kubernetes custom resources and the
vendor-specific driver gRPC — it's the glue that turns "watch a CRD" into
"call a COSI driver."

## ONTAP backend configuration

The `s3:impl` backend mode uses ONTAP REST APIs:

- Bucket create/list: `/api/protocols/s3/services/{svm.uuid}/buckets`
- Bucket delete (by bucket UUID): `/api/protocols/s3/buckets/{svm.uuid}/{bucket.uuid}`
- Users: `/api/protocols/s3/services/{svm.uuid}/users`
- Groups: `/api/protocols/s3/services/{svm.uuid}/groups`
- Policies: `/api/protocols/s3/services/{svm.uuid}/policies`
- Async job polling: `/api/cluster/jobs/{uuid}`

Bucket create and delete are asynchronous: ONTAP answers `202` with a job
reference and the driver polls the job until it reaches a terminal state. A
failed job (for example, deleting a bucket that is not empty) surfaces as a
driver error and the sidecar retries until the condition is cleared.

### Required environment variables

Configure these values for the driver container:

- `ONTAP_MGMT_ENDPOINT`: ONTAP management endpoint (host:port or URL).
- `ONTAP_MGMT_SVM_UUID`: UUID of the ONTAP data SVM hosting the S3 service.
- `ONTAP_MGMT_USERNAME`: ONTAP username used for REST basic auth.
- `ONTAP_MGMT_PASSWORD`: ONTAP password used for REST basic auth.

Optional:

- `ONTAP_MGMT_USE_SSL`: `true` (default) or `false`.
- `ONTAP_S3_REGION`: reported in COSI S3 protocol info.
- `ONTAP_S3_ENDPOINT`: S3 endpoint advertised to consumers in the BucketAccess
  credentials Secret (`COSI_S3_ENDPOINT`); falls back to `ONTAP_MGMT_ENDPOINT`
  when unset.
- `ONTAP_S3_USE_SSL`: `true` or `false`; selects `https`/`http` for the
  advertised `COSI_S3_ENDPOINT`. Defaults to the `ONTAP_MGMT_USE_SSL` value
  when unset. A scheme embedded in `ONTAP_S3_ENDPOINT` is kept verbatim.
- `ONTAP_MGMT_CA_CERT_PATH`: path to a PEM CA certificate for the ONTAP
  management endpoint's TLS certificate.
- `ONTAP_MGMT_SKIP_TLS_VERIFY`: `true` to skip TLS certificate verification
  (insecure, testing only).
- `COSI_ENDPOINT`: driver socket endpoint.
- `X_COSI_DRIVER_NAME`: advertised COSI driver name.
- `X_COSI_CONFIG`: path to driver config file.

### Multi-tenant mode

Multi-tenant mode lets each namespace use a **different ONTAP system/target**:
the per-namespace Secret carries its own `ONTAP_MGMT_ENDPOINT` and
`ONTAP_MGMT_SVM_UUID`, so tenants can be spread across multiple ONTAP clusters
or SVMs. Opt-in via `ONTAP_MULTITENANT=true` (Helm value
`multiTenant.enabled`). When enabled, the driver resolves ONTAP credentials
per RPC from the Secret `ontap-cosi-driver-credentials` in the namespace that
triggered the RPC — the
`BucketClaim` namespace recorded in `Bucket.spec.bucketClaimRef` for
create/delete, and the `BucketAccess` namespace for grant/revoke. Objects with
no tenant namespace recorded (or a `Bucket` lacking `bucketClaimRef`) keep
using the deployment's own credentials.

The tenant Secret must carry the four identity keys `ONTAP_MGMT_ENDPOINT`,
`ONTAP_MGMT_SVM_UUID`, `ONTAP_MGMT_USERNAME`, `ONTAP_MGMT_PASSWORD`; a missing
or empty identity key fails provisioning until the Secret is fixed — identity
keys never silently fall back to operator credentials. All other keys are
optional and fall back to the deployment's own settings: `ONTAP_MGMT_USE_SSL`,
`ONTAP_MGMT_SKIP_TLS_VERIFY`, `ONTAP_MGMT_CA_CERT_PATH`, `ONTAP_S3_REGION`,
`ONTAP_S3_ENDPOINT`, `ONTAP_S3_USE_SSL`. `ONTAP_MGMT_CA_CERT` may additionally
carry an inline PEM CA certificate, which takes precedence over
`ONTAP_MGMT_CA_CERT_PATH` so tenants can ship their own CA without a mounted
file.

Credential material never crosses the gRPC socket: the sidecar injects only
the tenant namespace as a request parameter (`cosi.tenantNamespace`); the
driver fetches the Secret from the Kubernetes API and builds a fresh client
per reconcile, so Secret edits take effect on the next reconcile. Errors
resolving tenant credentials surface as retryable gRPC `Unavailable` — if the
Secret is missing, provisioning fails (recorded in `Bucket.status.error`,
`readyToUse: false`) and heals automatically once the Secret exists.

Operator notes:

- Objects provisioned before enabling the flag need their tenant Secret
  (with the credentials originally used) present for any later delete/revoke,
  or the flag disabled again. The sidecar keeps retrying until then; nothing
  is deleted or lost.
- The flag is inert without `s3:impl` mode.
- The chart's existing ClusterRole already grants the driver pod cluster-wide
  Secret access needed for this; no extra RBAC required (hardening that rule
  is a separate exercise).

### Driver config file

The config file (default path `/etc/cosi/config.yaml`, override with
`X_COSI_CONFIG`) selects the storage backend mode plus testing hooks:

- `mode`:
  - `"s3:impl"` — ONTAP S3 REST backend (this driver's purpose).
  - `"s3:fake"` — fake S3 backend; no ONTAP calls, for testing.
  - `"azure:fake"` — fake Azure Blob backend, for testing.
  - `"azure:impl"` — unimplemented; the driver panics at startup.
- `overrides.bucketID`: force the bucket ID used in driver operations (leave
  empty to use COSI-assigned IDs).
- `errors`: per-RPC error injection for testing. Keys: `getInfo`,
  `createBucket`, `deleteBucket`, `grantBucketAccess`, `revokeBucketAccess`;
  each takes a `message` and a gRPC status `code`.

Example:

```yaml
mode: "s3:impl"
overrides:
  bucketID: ""
errors: {}
```

### Kubernetes secret and config map

`config/default/kustomization.yaml` already includes ONTAP-oriented secret keys:

- `ONTAP_MGMT_ENDPOINT`
- `ONTAP_MGMT_SVM_UUID`
- `ONTAP_S3_REGION`
- `ONTAP_MGMT_USE_SSL`
- `ONTAP_MGMT_USERNAME`
- `ONTAP_MGMT_PASSWORD`
- `ONTAP_MGMT_CA_CERT_PATH`
- `ONTAP_MGMT_SKIP_TLS_VERIFY`
- `ONTAP_S3_ENDPOINT`
- `ONTAP_S3_USE_SSL`

Populate these before deploying.

## BucketClass parameters

The driver reads `BucketClass.spec.parameters`:

| Key | Required | Meaning |
|---|---|---|
| `size` | yes | Bucket size limit in GiB: a positive whole number (`"50"`) or with an explicit `Gi` suffix (`"50Gi"`). Missing, `0`, negative, or fractional values fail provisioning with `InvalidArgument`. |
| `comment` | no | Free-form comment stored on the ONTAP bucket. |
| `objectLocking` | no | `true`/`false`. When `true`, object locking is enabled with a governance retention default of one day (`governance`, `P1D`). |

Only `size` has equality semantics: if the bucket already exists with a
different configured size, `DriverCreateBucket` fails with `ALREADY_EXISTS`
instead of silently reusing the existing bucket; a bucket whose size matches
is reused as-is.

## Provisioning a bucket

`config/samples/cosi-resources.yaml` contains a working set of COSI objects:

```yaml
apiVersion: objectstorage.k8s.io/v1alpha2
kind: BucketClass
metadata:
  name: ontap-bucket-class
spec:
  driverName: ontap.objectstorage.k8s.io
  deletionPolicy: Delete
  parameters:
    size: "50Gi"
---
apiVersion: objectstorage.k8s.io/v1alpha2
kind: BucketClaim
metadata:
  name: ontap-test-bucket
  namespace: default
spec:
  bucketClassName: ontap-bucket-class
  protocols: [S3]
---
apiVersion: objectstorage.k8s.io/v1alpha2
kind: BucketAccessClass
metadata:
  name: ontap-access-class
spec:
  driverName: ontap.objectstorage.k8s.io
  authenticationType: Key
---
apiVersion: objectstorage.k8s.io/v1alpha2
kind: BucketAccess
metadata:
  name: ontap-test-access
  namespace: default
spec:
  bucketAccessClassName: ontap-access-class
  protocol: S3
  bucketClaims:
    - bucketClaimName: ontap-test-bucket
      accessModes:
        objectData: ReadWrite
      accessSecretName: ontap-test-credentials
```

This requires the COSI CRDs and the core controller to be installed first
(see [kubernetes-sigs/container-object-storage-interface](https://github.com/kubernetes-sigs/container-object-storage-interface)).
Once the `BucketAccess` reports `readyToUse: true`, the Secret named in
`accessSecretName` holds the S3 credentials: `COSI_S3_ACCESS_KEY_ID`,
`COSI_S3_ACCESS_SECRET_KEY`, `COSI_S3_ENDPOINT`, `COSI_S3_BUCKET_ID`,
`COSI_S3_REGION`, and `COSI_S3_ADDRESSING_STYLE`.

## Multi-bucket BucketAccess

A `BucketAccess` may reference several `BucketClaims` when its
`BucketAccessClass` sets `spec.multiBucketAccess: MultipleBuckets` (the
default, `SingleBucket`, rejects it). For multi-bucket requests the driver
creates **one shared account** (one set of S3 keys) and binds it to every
requested bucket via a per-bucket policy+group; the response carries one
`BucketInfo` entry per requested bucket. Each `bucketClaims[]` entry must
name its own `accessSecretName`; the resulting Secrets share the account's
keys but carry per-bucket `COSI_S3_BUCKET_ID`. Revocation unbinds the account
from every requested bucket and then deletes it.

## Deployment

The Helm chart in `charts/ontap-cosi-driver` deploys the driver + sidecar pod:

```sh
helm install ontap-cosi charts/ontap-cosi-driver \
  --namespace ontap-cosi-driver-system --create-namespace \
  --set driverImage.repository=registry.example.com/kubernetes/ontap-cosi-driver \
  --set driverImage.tag=mytag \
  --set sidecarImage.repository=registry.example.com/kubernetes/objectstorage-sidecar \
  --set sidecarImage.tag=mytag \
  --set ontap.mgmt.endpoint=ontap.example.com \
  --set ontap.mgmt.auth.username=admin \
  --set ontap.mgmt.auth.password='secret' \
  --set ontap.mgmt.svmUUID=<svm-uuid> \
  --set-file ontap.mgmt.tls.caCertContent=ca.pem \
  --set multiTenant.enabled=true
```

Key values (see `charts/ontap-cosi-driver/values.yaml`):

- `driverImage` / `sidecarImage` — driver and COSI sidecar images.
- `imagePullSecrets` — image pull secrets applied to the pod.
- `mode` — driver config mode: `"s3:impl"` (default), `"s3:fake"`,
  `"azure:fake"`.
- `multiTenant.enabled` — sets `ONTAP_MULTITENANT` on the driver
  (default `false`).
- `ontap.mgmt.*` — endpoint, `ssl`, `auth.username`/`auth.password`,
  `svmUUID`, and TLS: `tls.caCertPath` (in-container mount path, default
  `/etc/cosi/ca-cert/ca.crt`), `tls.caCertContent` (inline PEM rendered into a
  Secret mounted at that path), `tls.skipVerify`.
- `ontap.s3.*` — advertised S3 `endpoint`, `ssl`, and `region` (rendered into
  the credentials Secret as `ONTAP_S3_ENDPOINT`, `ONTAP_S3_USE_SSL`,
  `ONTAP_S3_REGION`).
- `overrideBucketID` — feeds the config file's `overrides.bucketID`.

The chart renders the credentials Secret `<release>-credentials` (consumed by
the driver via `envFrom`), the CA-cert Secret, a ConfigMap holding
`config.yaml`, the Deployment (driver + sidecar sharing the `cosi.sock`
emptyDir), and RBAC (ServiceAccount, ClusterRole granting COSI resource plus
Secret/Event/Lease access, ClusterRoleBinding).

A kustomize-based alternative lives in `config/default` (build and lint it
with `make lint-manifests`).

To deploy the driver in multi-tenant mode, a customized COSI sidecar container
is required: vanilla upstream sidecar binaries do not inject
`cosi.tenantNamespace`, so with multi-tenant mode enabled they silently fall
back to the operator's credentials. The code lives in the
[`tenant-namespace-injection` branch of
`joeyloman/container-object-storage-interface`](https://github.com/joeyloman/container-object-storage-interface/tree/tenant-namespace-injection).
Build the container with the Dockerfile located in the `sidecar/` directory
(the repository root is the build context), push it to a registry, and
configure it in the chart's `values.yaml` under the `sidecarImage` section
(`repository` and `tag`).

## Build and development

```sh
make test            # unit tests (go test -cover -race ./pkg/... ./cmd/...)
make lint            # golangci-lint
make lint-manifests  # kube-linter on the kustomize manifests
make build           # container image (DOCKER, SAMPLE_DRIVER_TAG, PLATFORM, BUILD_ARGS)
```

Lint and test tooling is pinned in `hack/tools/go.mod` and invoked via
`go tool`, so no global installs are required.

The ONTAP S3 client integration tests (`pkg/clients/s3`) are excluded from the
default run by the `integration` build tag. Run them against a live endpoint:

```sh
export TEST_S3_ENDPOINT=... TEST_S3_SVM_UUID=... TEST_S3_REGION=... \
       TEST_S3_SSL=true TEST_S3_ACCESS_KEY_ID=... TEST_S3_ACCESS_SECRET_KEY=...
go test -tags=integration ./pkg/clients/s3/
```

## Limitations

- **Dynamic provisioning only.** The driver implements `DriverCreateBucket`,
  `DriverDeleteBucket`, `DriverGrantBucketAccess`, and
  `DriverRevokeBucketAccess`; it does not implement `DriverGetBucket`, so
  static provisioning via `existingBucketID` (adopting a pre-existing bucket)
  is unsupported.
- **Deletion requires an empty bucket.** ONTAP fails the delete job for a
  non-empty bucket; the error is recorded in `Bucket.status.error` and the
  sidecar retries until the bucket is emptied.
