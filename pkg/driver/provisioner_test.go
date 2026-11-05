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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"sigs.k8s.io/container-object-storage-interface/client/apis/objectstorage/consts"
	cosi "sigs.k8s.io/container-object-storage-interface/proto"
	"sigs.k8s.io/cosi-driver-sample/pkg/clients"
	"sigs.k8s.io/cosi-driver-sample/pkg/clients/fake"
	"sigs.k8s.io/cosi-driver-sample/pkg/config"
)

func newTestProvisionerServer(t *testing.T) (*ProvisionerServer, *fake.Client) {
	t.Helper()

	client := fake.New(string(consts.S3Key))
	for _, bucketID := range []string{"bucket-1", "bucket-2"} {
		require.NoError(t, client.CreateBucket(context.Background(), bucketID, nil))
	}

	return &ProvisionerServer{
		Client: client,
		Config: config.Config{Errors: config.Errors{}},
	}, client
}

func TestDriverGrantBucketAccess_MultiBucket(t *testing.T) {
	t.Parallel()

	server, _ := newTestProvisionerServer(t)

	resp, err := server.DriverGrantBucketAccess(context.Background(),
		&cosi.DriverGrantBucketAccessRequest{
			AccountName: "ba-account-1",
			Buckets: []*cosi.DriverGrantBucketAccessRequest_AccessedBucket{
				{BucketId: "bucket-1"},
				{BucketId: "bucket-2"},
			},
		})
	require.NoError(t, err)

	// Every requested bucket must appear exactly once in the response; the COSI
	// sidecar fails the grant if the response misses any request bucket.
	assert.Equal(t, "ba-account-1", resp.AccountId)
	require.Len(t, resp.Buckets, 2)
	gotBucketIds := map[string]bool{}
	for _, info := range resp.Buckets {
		gotBucketIds[info.BucketId] = true
		assert.NotNil(t, info.BucketInfo.GetS3())
		assert.Equal(t, info.BucketId, info.BucketInfo.GetS3().GetBucketId())
		assert.Equal(t, "http://fake-s3:8080", info.BucketInfo.GetS3().GetEndpoint())
	}
	assert.True(t, gotBucketIds["bucket-1"], "response missing bucket-1")
	assert.True(t, gotBucketIds["bucket-2"], "response missing bucket-2")

	// A shared account must actually be bound to both buckets.
	assert.NotNil(t, resp.Credentials.GetS3())
	assert.NotEmpty(t, resp.Credentials.GetS3().GetAccessKeyId())
	assert.NotEmpty(t, resp.Credentials.GetS3().GetAccessSecretKey())
}

func TestDriverGrantBucketAccess_Errors(t *testing.T) {
	t.Parallel()

	server, _ := newTestProvisionerServer(t)

	t.Run("no buckets", func(t *testing.T) {
		t.Parallel()

		_, err := server.DriverGrantBucketAccess(context.Background(),
			&cosi.DriverGrantBucketAccessRequest{AccountName: "ba-account-1"})
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
	})

	t.Run("nonexistent bucket", func(t *testing.T) {
		t.Parallel()

		_, err := server.DriverGrantBucketAccess(context.Background(),
			&cosi.DriverGrantBucketAccessRequest{
				AccountName: "ba-account-1",
				Buckets:     []*cosi.DriverGrantBucketAccessRequest_AccessedBucket{{BucketId: "missing"}},
			})
		assert.Equal(t, codes.NotFound, status.Code(err))
	})
}

func TestDriverRevokeBucketAccess_MultiBucket(t *testing.T) {
	t.Parallel()

	server, client := newTestProvisionerServer(t)
	var err error
	_, err = client.CreateBucketAccess(context.Background(), "bucket-1", "ba-account-1")
	require.NoError(t, err)
	_, err = client.CreateBucketAccess(context.Background(), "bucket-2", "ba-account-1")
	require.NoError(t, err)

	_, err = server.DriverRevokeBucketAccess(context.Background(),
		&cosi.DriverRevokeBucketAccessRequest{
			AccountId: "ba-account-1",
			Buckets: []*cosi.DriverRevokeBucketAccessRequest_AccessedBucket{
				{BucketId: "bucket-1"},
				{BucketId: "bucket-2"},
			},
		})
	require.NoError(t, err)

	assert.NotContains(t, client.Accesses, "ba-account-1")
}

func TestDriverRevokeBucketAccess_NoBuckets(t *testing.T) {
	t.Parallel()

	server, _ := newTestProvisionerServer(t)

	_, err := server.DriverRevokeBucketAccess(context.Background(),
		&cosi.DriverRevokeBucketAccessRequest{AccountId: "ba-account-1"})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

// quotaExhaustedClient wraps the fake client and fails bucket creation the way
// the ONTAP client does when the object store's bucket quota is exhausted.
type quotaExhaustedClient struct {
	*fake.Client
}

func (c *quotaExhaustedClient) CreateBucket(context.Context, string, map[string]string) error {
	return fmt.Errorf("failed to initiate bucket creation: %w", clients.ErrQuotaExhausted)
}

func TestDriverCreateBucket_QuotaExhausted(t *testing.T) {
	t.Parallel()

	server := &ProvisionerServer{
		Client: &quotaExhaustedClient{fake.New(string(consts.S3Key))},
		Config: config.Config{Errors: config.Errors{}},
	}

	_, err := server.DriverCreateBucket(context.Background(), &cosi.DriverCreateBucketRequest{
		Name:       "bucket-3",
		Protocols:  []*cosi.ObjectProtocol{{Type: cosi.ObjectProtocol_S3}},
		Parameters: map[string]string{"size": "1"},
	})

	require.Error(t, err)
	assert.Equal(t, codes.ResourceExhausted, status.Code(err))
	assert.Contains(t, status.Convert(err).Message(), "bucket quota exhausted")
}

func TestDriverCreateBucket_ExistingBucketReturnsProtocols(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		requestName string
		overrides   config.Overrides
	}{
		"existing bucket with matching parameters": {
			requestName: "bucket-1",
		},
		"existing bucket via bucketID override": {
			requestName: "ignored-name",
			overrides:   config.Overrides{BucketID: "bucket-1"},
		},
	}

	for name, tc := range tests {
		tc := tc
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			client := fake.New(string(consts.S3Key))
			require.NoError(t, client.CreateBucket(context.Background(), "bucket-1", nil))

			server := &ProvisionerServer{
				Client: client,
				Config: config.Config{Overrides: tc.overrides, Errors: config.Errors{}},
			}

			resp, err := server.DriverCreateBucket(context.Background(), &cosi.DriverCreateBucketRequest{
				Name:      tc.requestName,
				Protocols: []*cosi.ObjectProtocol{{Type: cosi.ObjectProtocol_S3}},
			})

			require.NoError(t, err)
			assert.Equal(t, "bucket-1", resp.BucketId)
			require.NotNil(t, resp.Protocols, "reused buckets must carry protocol info")
			require.NotNil(t, resp.Protocols.S3)
			assert.Equal(t, "bucket-1", resp.Protocols.S3.BucketId)
			assert.Equal(t, "http://fake-s3:8080", resp.Protocols.S3.Endpoint)
			assert.Equal(t, "fake-region", resp.Protocols.S3.Region)
		})
	}
}
