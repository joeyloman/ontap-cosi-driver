// Copyright 2024 The Kubernetes Authors.
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

//go:build integration
// +build integration

package s3

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type env string

func (e env) String() string { return string(e) }
func (e env) Bool() bool     { b, _ := strconv.ParseBool(e.String()); return b }

func requiredEnv(key string) env {
	val, found := os.LookupEnv(key)
	if !found || val == "" {
		panic(fmt.Sprintf("%s is required but not set", key))
	}
	return env(val)
}

/*
Example configuration (https://min.io/docs/minio/linux/administration/minio-console.html):
	export TEST_S3_ENDPOINT="play.min.io"
	export TEST_S3_REGION="us-east-1"
	export TEST_S3_SSL="true"
	export TEST_S3_ACCESS_KEY_ID="Q3AM3UQ867SPQQA43P2F"
	export TEST_S3_ACCESS_SECRET_KEY="zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG"
*/

func bucketName(prefix string) string {
	return fmt.Sprintf(
		"%scosi-%d",
		prefix,
		time.Now().Unix(),
	)
}

var (
	testEndpoint        = requiredEnv("TEST_S3_ENDPOINT").String()
	testSVMUUID         = requiredEnv("TEST_S3_SVM_UUID").String()
	testRegion          = requiredEnv("TEST_S3_REGION").String()
	testSSL             = requiredEnv("TEST_S3_SSL").Bool()
	testAccessKeyID     = requiredEnv("TEST_S3_ACCESS_KEY_ID").String()
	testAccessSecretKey = requiredEnv("TEST_S3_ACCESS_SECRET_KEY").String()

	testCreds = S3Credentials{
		AccessKeyID:     testAccessKeyID,
		AccessSecretKey: testAccessSecretKey,
	}
)

func TestClient_New(t *testing.T) {
	t.Parallel()

	tlsCfg := TLSConfig{
		SSL: testSSL,
	}
	client, err := NewFromConfig(Config{
		Endpoint:   testEndpoint,
		S3Endpoint: testEndpoint,
		Region:     testRegion,
		SVMUUID:    testSVMUUID,
		Admin:      testCreds,
		TLS:        tlsCfg,
		S3UseSSL:   testSSL,
	})
	assert.NoError(t, err)
	assert.NotNil(t, client)
}

func TestClient_CreateBucket(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		bucketName  string
		parameters  map[string]string
		expectError bool
	}{
		"valid bucket creation": {
			bucketName: bucketName("valid"),
			parameters: map[string]string{"region": testRegion},
		},
		"invalid bucket creation with empty name": {
			bucketName:  "",
			expectError: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			tlsCfg := TLSConfig{
				SSL: testSSL,
			}
			client, err := NewFromConfig(Config{
				Endpoint:   testEndpoint,
				S3Endpoint: testEndpoint,
				Region:     testRegion,
				SVMUUID:    testSVMUUID,
				Admin:      testCreds,
				TLS:        tlsCfg,
				S3UseSSL:   testSSL,
			})
			require.NoError(t, err)
			defer client.DeleteBucket(context.Background(), tc.bucketName) //nolint:errcheck // best effort call

			err = client.CreateBucket(context.Background(), tc.bucketName, tc.parameters)
			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)

				exists, err := client.BucketExists(context.Background(), tc.bucketName)
				assert.NoError(t, err)
				assert.True(t, exists)
			}
		})
	}
}

func TestClient_BucketExists(t *testing.T) {
	t.Parallel()

	existing := bucketName("exists")
	nonexisting := bucketName("not-exists")

	tlsCfg := TLSConfig{
		SSL: testSSL,
	}
	client, err := NewFromConfig(Config{
		Endpoint:   testEndpoint,
		S3Endpoint: testEndpoint,
		Region:     testRegion,
		SVMUUID:    testSVMUUID,
		Admin:      testCreds,
		TLS:        tlsCfg,
		S3UseSSL:   testSSL,
	})
	require.NoError(t, err)
	defer client.DeleteBucket(context.Background(), existing) //nolint:errcheck // best effort call

	err = client.CreateBucket(context.Background(), existing, map[string]string{"region": testRegion})
	require.NoError(t, err)

	tests := map[string]struct {
		bucketName  string
		expected    bool
		expectError bool
	}{
		"existing bucket": {
			bucketName: existing,
			expected:   true,
		},
		"non-existing bucket": {
			bucketName: nonexisting,
			expected:   false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			exists, err := client.BucketExists(context.Background(), tc.bucketName)
			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, exists)
			}
		})
	}
}

func TestClient_CreateBucketAccess(t *testing.T) {
	t.Parallel()

	bucket := bucketName("access")

	tlsCfg := TLSConfig{
		SSL: testSSL,
	}
	client, err := NewFromConfig(Config{
		Endpoint:   testEndpoint,
		S3Endpoint: testEndpoint,
		Region:     testRegion,
		SVMUUID:    testSVMUUID,
		Admin:      testCreds,
		TLS:        tlsCfg,
		S3UseSSL:   testSSL,
	})
	require.NoError(t, err)
	defer client.DeleteBucket(context.Background(), bucket) //nolint:errcheck // best effort call

	err = client.CreateBucket(context.Background(), bucket, map[string]string{"region": testRegion})
	require.NoError(t, err)

	user, err := client.CreateBucketAccess(context.Background(), bucket, "test-user")
	assert.NoError(t, err)
	assert.Equal(t, "test-user", user.Name())
	assert.Equal(t, "s3", user.Platform())
	assert.Equal(t, map[string]string{
		"accessKeyId":     testAccessKeyID,
		"accessSecretKey": testAccessSecretKey,
	}, user.Credentials())
}

func TestClient_DeleteBucketAccess(t *testing.T) {
	t.Parallel()

	bucket := bucketName("del-access")

	tlsCfg := TLSConfig{
		SSL: testSSL,
	}
	client, err := NewFromConfig(Config{
		Endpoint:   testEndpoint,
		S3Endpoint: testEndpoint,
		Region:     testRegion,
		SVMUUID:    testSVMUUID,
		Admin:      testCreds,
		TLS:        tlsCfg,
		S3UseSSL:   testSSL,
	})
	require.NoError(t, err)
	defer client.DeleteBucket(context.Background(), bucket) //nolint:errcheck // best effort call

	t.Run("successful deletion of existing bucket access", func(t *testing.T) {
		t.Parallel()

		err := client.CreateBucket(context.Background(), bucket, map[string]string{"region": testRegion})
		require.NoError(t, err)

		// Create access first.
		user, err := client.CreateBucketAccess(context.Background(), bucket, "test-delete-user")
		require.NoError(t, err)
		assert.Equal(t, "test-delete-user", user.Name())

		// Delete access.
		err = client.DeleteBucketAccess(context.Background(), bucket, "test-delete-user")
		assert.NoError(t, err)

		// Deleting again should be a no-op.
		err = client.DeleteBucketAccess(context.Background(), bucket, "test-delete-user")
		assert.NoError(t, err)
	})

	t.Run("deletion of non-existent bucket access", func(t *testing.T) {
		t.Parallel()

		err := client.DeleteBucketAccess(context.Background(), bucket, "never-created-user")
		assert.NoError(t, err)
	})
}

func TestClient_DeleteBucket(t *testing.T) {
	t.Parallel()

	tlsCfg := TLSConfig{
		SSL: testSSL,
	}
	client, err := NewFromConfig(Config{
		Endpoint:   testEndpoint,
		S3Endpoint: testEndpoint,
		Region:     testRegion,
		SVMUUID:    testSVMUUID,
		Admin:      testCreds,
		TLS:        tlsCfg,
		S3UseSSL:   testSSL,
	})
	require.NoError(t, err)

	t.Run("successful deletion of existing bucket", func(t *testing.T) {
		bucket := bucketName("delete")

		// Create the bucket first.
		err := client.CreateBucket(context.Background(), bucket, map[string]string{"region": testRegion})
		require.NoError(t, err)

		// Verify it exists.
		exists, err := client.BucketExists(context.Background(), bucket)
		require.NoError(t, err)
		assert.True(t, exists)

		// Delete the bucket.
		err = client.DeleteBucket(context.Background(), bucket)
		assert.NoError(t, err)

		// Verify it no longer exists.
		exists, err = client.BucketExists(context.Background(), bucket)
		require.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("deletion of non-existent bucket", func(t *testing.T) {
		bucket := bucketName("never-exists")

		// Deleting a bucket that was never created should be a no-op.
		err := client.DeleteBucket(context.Background(), bucket)
		assert.NoError(t, err)
	})
}

func TestClient_ProtocolInfo(t *testing.T) {
	t.Parallel()

	tlsCfg := TLSConfig{
		SSL: testSSL,
	}
	client, err := NewFromConfig(Config{
		Endpoint:   testEndpoint,
		S3Endpoint: testEndpoint,
		Region:     testRegion,
		SVMUUID:    testSVMUUID,
		Admin:      testCreds,
		TLS:        tlsCfg,
		S3UseSSL:   testSSL,
	})
	require.NoError(t, err)

	protocol := client.ProtocolInfo()
	assert.NotNil(t, protocol)
	assert.Equal(t, testRegion, protocol.GetS3().Region)
}
