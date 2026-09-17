// Copyright 2026 The Kubernetes Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package driver

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"sigs.k8s.io/cosi-driver-sample/pkg/clients"
	"sigs.k8s.io/cosi-driver-sample/pkg/clients/s3"
)

// TenantNamespaceParam is the RPC parameters key carrying the namespace that
// owns the per-tenant credentials Secret. MUST stay in sync with the sidecar's
// injected key (container-object-storage-interface sidecar/pkg/reconciler,
// branch tenant-namespace-injection).
const TenantNamespaceParam = "cosi.tenantNamespace"

// tenantSecretName is the Secret every tenant namespace must carry; it holds
// the per-tenant ONTAP credentials overlaying the driver's own configuration.
const tenantSecretName = "ontap-cosi-driver-credentials"

// Secret keys understood in a tenant credentials Secret. The four identity
// keys are required; the rest are optional and fall back to the driver's own
// environment-derived configuration when absent.
const (
	mgmtEndpointKey   = "ONTAP_MGMT_ENDPOINT"
	mgmtSVMUUIDKey    = "ONTAP_MGMT_SVM_UUID"
	mgmtUsernameKey   = "ONTAP_MGMT_USERNAME"
	mgmtPasswordKey   = "ONTAP_MGMT_PASSWORD"
	mgmtUseSSLKey     = "ONTAP_MGMT_USE_SSL"
	mgmtSkipVerifyKey = "ONTAP_MGMT_SKIP_TLS_VERIFY"
	mgmtCACertPathKey = "ONTAP_MGMT_CA_CERT_PATH"
	mgmtCACertKey     = "ONTAP_MGMT_CA_CERT"
	s3RegionKey       = "ONTAP_S3_REGION"
	s3EndpointKey     = "ONTAP_S3_ENDPOINT"
	s3UseSSLKey       = "ONTAP_S3_USE_SSL"
)

// TenantCredentialsLoader returns a per-tenant backend client.
type TenantCredentialsLoader interface {
	Load(ctx context.Context, namespace string) (clients.Client, error)
}

// tenantCredentialsLoader resolves per-tenant ONTAP credentials from the
// Secret <namespace>/ontap-cosi-driver-credentials.
type tenantCredentialsLoader struct {
	kube     kubernetes.Interface
	defaults s3.Config
}

// NewTenantCredentialsLoader returns a loader that overlays the tenant
// namespace's credentials Secret on top of defaults (the driver's own
// environment-derived configuration).
func NewTenantCredentialsLoader(kube kubernetes.Interface, defaults s3.Config) *tenantCredentialsLoader {
	return &tenantCredentialsLoader{kube: kube, defaults: defaults}
}

// Load resolves the tenant's credentials and builds a dedicated backend
// client. Secrets are read fresh on every call with no caching: rotation lands
// with the next reconcile. The per-RPC transport setup and CA file read is
// accepted at this scale in exchange for that simplicity.
func (l *tenantCredentialsLoader) Load(ctx context.Context, namespace string) (clients.Client, error) {
	secret, err := l.kube.CoreV1().Secrets(namespace).Get(ctx, tenantSecretName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("tenant credentials: secret %s/%s: %w", namespace, tenantSecretName, err)
	}

	cfg, err := tenantConfig(l.defaults, secret)
	if err != nil {
		return nil, fmt.Errorf("tenant credentials: secret %s/%s: %w", namespace, tenantSecretName, err)
	}

	client, err := s3.NewFromConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("tenant credentials: secret %s/%s: %w", namespace, tenantSecretName, err)
	}

	return client, nil
}

// tenantConfig maps the tenant Secret's data onto a copy of the driver's own
// configuration. It is pure so tests can assert the mapping without HTTP.
//
// The four identity keys are required and never fall back to the operator
// credentials: a tenant Secret missing any of them is an error, so the
// failure surfaces in Bucket.status.error instead of silently provisioning
// with operator credentials.
func tenantConfig(defaults s3.Config, secret *corev1.Secret) (s3.Config, error) {
	get := func(key string) (string, bool) {
		v, ok := secret.Data[key]
		if !ok || len(v) == 0 {
			return "", false
		}
		return string(v), true
	}
	getBool := func(key string) (bool, bool, error) {
		raw, ok := get(key)
		if !ok {
			return false, false, nil
		}
		b, err := strconv.ParseBool(raw)
		if err != nil {
			return false, true, fmt.Errorf("invalid boolean for %s: %q", key, raw)
		}
		return b, true, nil
	}

	cfg := defaults

	var missing []string
	for key, dst := range map[string]*string{
		mgmtEndpointKey: &cfg.Endpoint,
		mgmtSVMUUIDKey:  &cfg.SVMUUID,
		mgmtUsernameKey: &cfg.Admin.AccessKeyID,
		mgmtPasswordKey: &cfg.Admin.AccessSecretKey,
	} {
		if v, ok := get(key); ok {
			*dst = v
		} else {
			missing = append(missing, key)
		}
	}
	if len(missing) > 0 {
		return s3.Config{}, fmt.Errorf("missing required key(s): %s", strings.Join(missing, ", "))
	}

	if useSSL, present, err := getBool(mgmtUseSSLKey); err != nil {
		return s3.Config{}, err
	} else if present {
		cfg.TLS.SSL = useSSL
	}

	if skipVerify, present, err := getBool(mgmtSkipVerifyKey); err != nil {
		return s3.Config{}, err
	} else if present {
		cfg.TLS.SkipVerify = skipVerify
	}

	if v, ok := get(mgmtCACertPathKey); ok {
		cfg.TLS.CACertPath = v
	}
	if v, ok := get(mgmtCACertKey); ok {
		// Inline PEM takes precedence over the path (enforced in
		// s3.NewFromConfig), so tenants can carry their own CA without a
		// mounted file.
		cfg.CACertPEM = v
	}

	if v, ok := get(s3RegionKey); ok {
		cfg.Region = v
	}
	if v, ok := get(s3EndpointKey); ok {
		cfg.S3Endpoint = v
	}
	if useSSL, present, err := getBool(s3UseSSLKey); err != nil {
		return s3.Config{}, err
	} else if present {
		cfg.S3UseSSL = useSSL
	}

	return cfg, nil
}
