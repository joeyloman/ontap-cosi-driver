// Copyright 2024 The Kubernetes Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package s3

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	testSVMUUID  = "11111111-2222-3333-4444-555555555555"
	testJobUUID  = "job-1"
	testBucketID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
)

// newFakeONTAPServer emulates the subset of the ONTAP S3 REST API the client
// uses: async bucket creation (POST -> job ref), job polling (GET), and
// name-filtered bucket listing (GET). A nil bucket means the backend has no
// bucket. When createBody is non-nil, the POST body is decoded into it (nil
// when the handler is never hit).
func newFakeONTAPServer(t *testing.T, bucket *ontapBucketRecord, createBody *map[string]any) *httptest.Server {
	t.Helper()

	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/protocols/s3/services/{svm}/buckets", func(w http.ResponseWriter, r *http.Request) {
		if createBody != nil {
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Errorf("failed to decode create request body: %v", err)
			}
			*createBody = body
		}
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"job": map[string]any{"uuid": testJobUUID},
		})
	})
	mux.HandleFunc("GET /api/cluster/jobs/{uuid}", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"state": "success"})
	})
	mux.HandleFunc("GET /api/protocols/s3/services/{svm}/buckets", func(w http.ResponseWriter, _ *http.Request) {
		records := []ontapBucketRecord{}
		if bucket != nil {
			records = append(records, *bucket)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"num_records": len(records),
			"records":     records,
		})
	})

	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	return ts
}

// sizeInBody returns the size value carried by a create request body, or -1
// when the key is absent.
func sizeInBody(t *testing.T, body map[string]any) int64 {
	t.Helper()

	v, found := body["size"]
	if !found {
		return -1
	}
	n, ok := v.(float64) // JSON numbers decode as float64
	require.True(t, ok, "size key has unexpected type %T", v)
	return int64(n)
}

func TestCreateBucketSize(t *testing.T) {
	t.Parallel()

	const tenGiB = int64(10) * gib

	tests := map[string]struct {
		params   map[string]string
		wantSize int64 // -1 = key absent
		wantErr  bool
		wantCode codes.Code
	}{
		"size in GiB is passed to the backend as bytes": {
			params:   map[string]string{"size": "10"},
			wantSize: tenGiB,
		},
		"explicit Gi suffix is accepted": {
			params:   map[string]string{"size": "10Gi"},
			wantSize: tenGiB,
		},
		"lowercase gi suffix is accepted": {
			params:   map[string]string{"size": "10gi"},
			wantSize: tenGiB,
		},
		"absent size parameter is rejected": {
			params:   map[string]string{"comment": "hello"},
			wantErr:  true,
			wantCode: codes.InvalidArgument,
		},
		"empty size parameter is rejected": {
			params:   map[string]string{"size": "  "},
			wantErr:  true,
			wantCode: codes.InvalidArgument,
		},
		"zero size is rejected": {
			params:   map[string]string{"size": "0"},
			wantErr:  true,
			wantCode: codes.InvalidArgument,
		},
		"fractional GiB is rejected": {
			params:   map[string]string{"size": "10.5"},
			wantErr:  true,
			wantCode: codes.InvalidArgument,
		},
		"unrecognized unit suffix is rejected": {
			params:   map[string]string{"size": "10G"},
			wantErr:  true,
			wantCode: codes.InvalidArgument,
		},
		"negative size is rejected": {
			params:   map[string]string{"size": "-1"},
			wantErr:  true,
			wantCode: codes.InvalidArgument,
		},
	}

	for name, tc := range tests {
		tc := tc
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			var createBody map[string]any
			server := newFakeONTAPServer(t, &ontapBucketRecord{Name: "test-bucket"}, &createBody)
			client := newTestClient(t, server)

			err := client.CreateBucket(t.Context(), "test-bucket", tc.params)

			if tc.wantErr {
				require.Error(t, err)
				assert.Equal(t, tc.wantCode, status.Code(err))
				assert.Nil(t, createBody, "no create request should be sent for invalid parameters")
				return
			}

			require.NoError(t, err)
			require.NotNil(t, createBody, "create request should have been sent")
			assert.Equal(t, tc.wantSize, sizeInBody(t, createBody))
		})
	}
}

func TestIsBucketEqualSize(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		params  map[string]string
		bucket  *ontapBucketRecord
		want    bool
		wantErr bool
	}{
		"matching size": {
			params: map[string]string{"size": "100"},
			bucket: &ontapBucketRecord{Name: "test-bucket", Size: 100 * gib},
			want:   true,
		},
		"differing size": {
			params: map[string]string{"size": "100"},
			bucket: &ontapBucketRecord{Name: "test-bucket", Size: 200 * gib},
			want:   false,
		},
		"absent size parameter keeps conservative equality": {
			params: map[string]string{"comment": "x"},
			want:   true,
		},
		"bucket missing on backend": {
			params: map[string]string{"size": "100"},
			want:   false,
		},
		"invalid size parameter errors": {
			params:  map[string]string{"size": "big"},
			bucket:  &ontapBucketRecord{Name: "test-bucket"},
			wantErr: true,
		},
		"zero size parameter errors": {
			params:  map[string]string{"size": "0"},
			bucket:  &ontapBucketRecord{Name: "test-bucket"},
			wantErr: true,
		},
	}

	for name, tc := range tests {
		tc := tc
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			server := newFakeONTAPServer(t, tc.bucket, nil)
			client := newTestClient(t, server)

			equal, err := client.IsBucketEqual(t.Context(), "test-bucket", tc.params)

			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, equal)
		})
	}
}

func newTestClient(t *testing.T, server *httptest.Server) *Client {
	t.Helper()

	client, err := NewFromConfig(Config{
		Endpoint: server.URL,
		Region:   "us-east-1",
		SVMUUID:  testSVMUUID,
	})
	require.NoError(t, err)
	return client
}
