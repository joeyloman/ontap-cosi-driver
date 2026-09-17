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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sfake "k8s.io/client-go/kubernetes/fake"

	cosi "sigs.k8s.io/container-object-storage-interface/proto"
	"sigs.k8s.io/cosi-driver-sample/pkg/clients"
	"sigs.k8s.io/cosi-driver-sample/pkg/clients/fake"
	"sigs.k8s.io/cosi-driver-sample/pkg/clients/s3"
)

// markerClient wraps a clients.Client and advertises distinct endpoint/region
// markers, so tests can assert which client instance the handlers used.
type markerClient struct {
	clients.Client
	endpoint string
	region   string
}

func (m *markerClient) S3Endpoint() string { return m.endpoint }
func (m *markerClient) S3Region() string   { return m.region }

// stubLoader answers Load without touching an API server and records the
// namespace it was asked about.
type stubLoader struct {
	client clients.Client
	err    error
	ns     string
}

func (l *stubLoader) Load(_ context.Context, namespace string) (clients.Client, error) {
	l.ns = namespace
	if l.err != nil {
		return nil, l.err
	}
	return l.client, nil
}

func TestResolveClient(t *testing.T) {
	t.Parallel()

	base := fake.New("s3")
	server := &ProvisionerServer{Client: base}

	t.Run("nil loader returns the instance client", func(t *testing.T) {
		t.Parallel()
		client, err := server.resolveClient(context.Background(), map[string]string{TenantNamespaceParam: "tenant-ns1"})
		require.NoError(t, err)
		assert.Same(t, base, client)
	})

	t.Run("loader set but no tenant parameter returns the instance client", func(t *testing.T) {
		t.Parallel()
		loader := &stubLoader{client: &markerClient{endpoint: "tenant"}}
		server := &ProvisionerServer{Client: base, Tenant: loader}
		client, err := server.resolveClient(context.Background(), map[string]string{"size": "50"})
		require.NoError(t, err)
		assert.Same(t, base, client)
		assert.Empty(t, loader.ns, "loader must not be consulted without a tenant namespace")
	})

	t.Run("loader consulted for the tenant namespace parameter", func(t *testing.T) {
		t.Parallel()
		tenant := fake.New("s3")
		loader := &stubLoader{client: tenant}
		server := &ProvisionerServer{Client: base, Tenant: loader}
		client, err := server.resolveClient(context.Background(), map[string]string{TenantNamespaceParam: "tenant-ns1"})
		require.NoError(t, err)
		assert.Same(t, tenant, client)
		assert.Equal(t, "tenant-ns1", loader.ns)
	})

	t.Run("loader error surfaces as gRPC Unavailable", func(t *testing.T) {
		t.Parallel()
		loader := &stubLoader{err: errors.New("boom")}
		server := &ProvisionerServer{Client: base, Tenant: loader}
		_, err := server.resolveClient(context.Background(), map[string]string{TenantNamespaceParam: "tenant-ns1"})
		require.Error(t, err)
		assert.Equal(t, codes.Unavailable, status.Code(err))
	})
}

func TestHandlersUseTenantClient(t *testing.T) {
	t.Parallel()

	const (
		tenantEndpoint = "http://tenant-s3:1010"
		tenantRegion   = "tenant-region-1"
	)

	newServer := func(loader *stubLoader) (*ProvisionerServer, *fake.Client) {
		base := fake.New("s3")
		require.NoError(t, base.CreateBucket(context.Background(), "bucket-1", nil))
		return &ProvisionerServer{Client: base, Tenant: loader}, base
	}

	t.Run("grant advertises tenant endpoint and region", func(t *testing.T) {
		t.Parallel()
		tenant := fake.New("s3")
		require.NoError(t, tenant.CreateBucket(context.Background(), "bucket-1", nil))
		loader := &stubLoader{client: &markerClient{Client: tenant, endpoint: tenantEndpoint, region: tenantRegion}}
		server, _ := newServer(loader)

		resp, err := server.DriverGrantBucketAccess(context.Background(), &cosi.DriverGrantBucketAccessRequest{
			AccountName: "ba-tenant",
			Parameters:  map[string]string{TenantNamespaceParam: "tenant-ns1"},
			Buckets:     []*cosi.DriverGrantBucketAccessRequest_AccessedBucket{{BucketId: "bucket-1"}},
		})
		require.NoError(t, err)
		require.NotEmpty(t, resp.Buckets)
		resolved := resp.Buckets[0].BucketInfo.GetS3()
		require.NotNil(t, resolved)
		assert.Equal(t, tenantEndpoint, resolved.GetEndpoint())
		assert.Equal(t, tenantRegion, resolved.GetRegion())
		assert.Equal(t, "tenant-ns1", loader.ns)
	})

	t.Run("grant without tenant parameter keeps operator endpoint", func(t *testing.T) {
		t.Parallel()
		loader := &stubLoader{client: &markerClient{Client: fake.New("s3"), endpoint: tenantEndpoint, region: tenantRegion}}
		server, _ := newServer(loader)

		resp, err := server.DriverGrantBucketAccess(context.Background(), &cosi.DriverGrantBucketAccessRequest{
			AccountName: "ba-operator",
			Buckets:     []*cosi.DriverGrantBucketAccessRequest_AccessedBucket{{BucketId: "bucket-1"}},
		})
		require.NoError(t, err)
		resolved := resp.Buckets[0].BucketInfo.GetS3()
		require.NotNil(t, resolved)
		assert.NotEqual(t, tenantEndpoint, resolved.GetEndpoint())
		assert.Equal(t, "http://fake-s3:8080", resolved.GetEndpoint())
		assert.Empty(t, loader.ns, "loader must not be consulted without a tenant namespace")
	})

	t.Run("create advertises tenant endpoint and region", func(t *testing.T) {
		t.Parallel()
		tenant := fake.New("s3")
		loader := &stubLoader{client: &markerClient{Client: tenant, endpoint: tenantEndpoint, region: tenantRegion}}
		server, _ := newServer(loader)

		resp, err := server.DriverCreateBucket(context.Background(), &cosi.DriverCreateBucketRequest{
			Name:       "tenant-bucket",
			Parameters: map[string]string{TenantNamespaceParam: "tenant-ns1"},
		})
		require.NoError(t, err)
		require.NotNil(t, resp.GetProtocols().GetS3())
		assert.Equal(t, tenantEndpoint, resp.GetProtocols().GetS3().GetEndpoint())
		assert.Equal(t, tenantRegion, resp.GetProtocols().GetS3().GetRegion())
		assert.Equal(t, "tenant-ns1", loader.ns)
	})

	t.Run("create without tenant parameter keeps operator endpoint", func(t *testing.T) {
		t.Parallel()
		loader := &stubLoader{client: &markerClient{Client: fake.New("s3"), endpoint: tenantEndpoint, region: tenantRegion}}
		server, _ := newServer(loader)

		resp, err := server.DriverCreateBucket(context.Background(), &cosi.DriverCreateBucketRequest{
			Name: "operator-bucket",
		})
		require.NoError(t, err)
		assert.Equal(t, "http://fake-s3:8080", resp.GetProtocols().GetS3().GetEndpoint())
		assert.Empty(t, loader.ns, "loader must not be consulted without a tenant namespace")
	})
}

func TestTenantConfig(t *testing.T) {
	t.Parallel()

	operatorDefaults := s3.Config{
		Endpoint:   "https://ontap.operator.example.com",
		S3Endpoint: "https://s3.operator.example.com",
		Region:     "operator-region",
		SVMUUID:    "default-svm",
		Admin:      s3.S3Credentials{AccessKeyID: "op-user", AccessSecretKey: "op-pass"},
		TLS:        s3.TLSConfig{SSL: true, CACertPath: "/operator/ca.crt"},
		S3UseSSL:   true,
	}
	identitySecret := func(overrides map[string]string) *corev1.Secret {
		data := map[string][]byte{
			mgmtEndpointKey: []byte("https://ontap.tenant.example.com"),
			mgmtSVMUUIDKey:  []byte("tenant-svm"),
			mgmtUsernameKey: []byte("tenant-user"),
			mgmtPasswordKey: []byte("tenant-pass"),
		}
		for key, value := range overrides {
			data[key] = []byte(value)
		}
		return &corev1.Secret{Data: data}
	}

	t.Run("full secret maps every key", func(t *testing.T) {
		t.Parallel()
		secret := identitySecret(map[string]string{
			mgmtUseSSLKey:     "false",
			mgmtSkipVerifyKey: "true",
			mgmtCACertPathKey: "/tenant/ca.crt",
			mgmtCACertKey:     "-----BEGIN CERTIFICATE-----\ninline\n-----END CERTIFICATE-----\n",
			s3RegionKey:       "tenant-region-1",
			s3EndpointKey:     "s3.tenant.example.com",
			s3UseSSLKey:       "true",
		})

		cfg, err := tenantConfig(operatorDefaults, secret)
		require.NoError(t, err)
		assert.Equal(t, "https://ontap.tenant.example.com", cfg.Endpoint)
		assert.Equal(t, "tenant-svm", cfg.SVMUUID)
		assert.Equal(t, "tenant-user", cfg.Admin.AccessKeyID)
		assert.Equal(t, "tenant-pass", cfg.Admin.AccessSecretKey)
		assert.False(t, cfg.TLS.SSL)
		assert.True(t, cfg.TLS.SkipVerify)
		assert.Equal(t, "/tenant/ca.crt", cfg.TLS.CACertPath)
		assert.Equal(t, "-----BEGIN CERTIFICATE-----\ninline\n-----END CERTIFICATE-----\n", cfg.CACertPEM)
		assert.Equal(t, "tenant-region-1", cfg.Region)
		assert.Equal(t, "s3.tenant.example.com", cfg.S3Endpoint)
		assert.True(t, cfg.S3UseSSL)
	})

	t.Run("absent optional keys inherit the operator defaults", func(t *testing.T) {
		t.Parallel()
		cfg, err := tenantConfig(operatorDefaults, identitySecret(nil))
		require.NoError(t, err)
		assert.Equal(t, operatorDefaults.TLS, cfg.TLS)
		assert.Equal(t, operatorDefaults.Region, cfg.Region)
		assert.Equal(t, operatorDefaults.S3Endpoint, cfg.S3Endpoint)
		assert.Equal(t, operatorDefaults.S3UseSSL, cfg.S3UseSSL)
		assert.Empty(t, cfg.CACertPEM)
	})

	t.Run("missing and empty identity keys are rejected", func(t *testing.T) {
		t.Parallel()
		for _, tc := range []struct {
			name      string
			overrides map[string]string
			wantKey   string
		}{
			{"missing password", map[string]string{mgmtPasswordKey: ""}, mgmtPasswordKey},
			{"missing endpoint", map[string]string{mgmtEndpointKey: ""}, mgmtEndpointKey},
			{"missing svm uuid", map[string]string{mgmtSVMUUIDKey: ""}, mgmtSVMUUIDKey},
			{"missing username", map[string]string{mgmtUsernameKey: ""}, mgmtUsernameKey},
		} {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				_, err := tenantConfig(operatorDefaults, identitySecret(tc.overrides))
				require.Error(t, err)
				assert.ErrorContains(t, err, tc.wantKey)
			})
		}
	})

	t.Run("invalid boolean value is rejected with the key named", func(t *testing.T) {
		t.Parallel()
		_, err := tenantConfig(operatorDefaults, identitySecret(map[string]string{mgmtUseSSLKey: "maybe"}))
		require.Error(t, err)
		assert.ErrorContains(t, err, mgmtUseSSLKey)
		assert.ErrorContains(t, err, "maybe")
	})
}

func TestTenantCredentialsLoader_Load(t *testing.T) {
	t.Parallel()

	t.Run("missing secret reports namespace and secret name", func(t *testing.T) {
		t.Parallel()
		loader := NewTenantCredentialsLoader(k8sfake.NewSimpleClientset(), s3.Config{TLS: s3.TLSConfig{SSL: false}})
		client, err := loader.Load(context.Background(), "tenant-ns1")
		require.Error(t, err)
		assert.Nil(t, client)
		assert.ErrorContains(t, err, "tenant-ns1")
		assert.ErrorContains(t, err, tenantSecretName)
	})

	t.Run("identity-only secret yields a working client", func(t *testing.T) {
		t.Parallel()
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: tenantSecretName, Namespace: "tenant-ns1"},
			Data: map[string][]byte{
				mgmtEndpointKey: []byte("ontap.tenant.example.com"),
				mgmtSVMUUIDKey:  []byte("tenant-svm"),
				mgmtUsernameKey: []byte("tenant-user"),
				mgmtPasswordKey: []byte("tenant-pass"),
			},
		}
		loader := NewTenantCredentialsLoader(k8sfake.NewSimpleClientset(secret), s3.Config{})
		client, err := loader.Load(context.Background(), "tenant-ns1")
		require.NoError(t, err)
		require.NotNil(t, client)
	})

	t.Run("inline CA takes precedence over a nonexistent CA path", func(t *testing.T) {
		t.Parallel()
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: tenantSecretName, Namespace: "tenant-ns1"},
			Data: map[string][]byte{
				mgmtEndpointKey:   []byte("https://ontap.tenant.example.com"),
				mgmtSVMUUIDKey:    []byte("tenant-svm"),
				mgmtUsernameKey:   []byte("tenant-user"),
				mgmtPasswordKey:   []byte("tenant-pass"),
				mgmtUseSSLKey:     []byte("true"),
				mgmtCACertPathKey: []byte("/definitely/nonexistent/ca.pem"),
				mgmtCACertKey:     []byte("--- not a pem block ---"),
			},
		}
		loader := NewTenantCredentialsLoader(k8sfake.NewSimpleClientset(secret), s3.Config{})

		_, err := loader.Load(context.Background(), "tenant-ns1")
		require.Error(t, err)
		assert.ErrorContains(t, err, "no valid CA certificates found in")
		assert.NotContains(t, err.Error(), "unable to read CA certificate")
	})
}
