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

package s3

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"sigs.k8s.io/cosi-driver-sample/pkg/clients"
)

// quotaExhaustedMessage is the rejection ONTAP returns when the object store's
// bucket quota is exhausted (observed against the lab's edge-s3 admission
// webhook).
const quotaExhaustedMessage = `admission webhook "edges3bucket-edge-s3-controller-webhook.edge-s3.svc" denied the request: bucket quota exhausted for object store "nootdorp"`

func TestCreateBucketQuotaExhausted(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		statusCode int
		message    string
		wantQuota  bool
	}{
		"403 quota exhausted rejection is classified": {
			statusCode: http.StatusForbidden,
			message:    quotaExhaustedMessage,
			wantQuota:  true,
		},
		"403 quota exceeded rejection is classified": {
			statusCode: http.StatusForbidden,
			message:    "denied the request: object store quota exceeded",
			wantQuota:  true,
		},
		"403 for another reason is not classified": {
			statusCode: http.StatusForbidden,
			message:    "denied the request: insufficient privileges",
			wantQuota:  false,
		},
	}

	for name, tc := range tests {
		tc := tc
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.statusCode)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"error": map[string]any{"message": tc.message},
				})
			}))
			t.Cleanup(server.Close)

			client := newTestClient(t, server)
			err := client.CreateBucket(t.Context(), "test-bucket", map[string]string{"size": "1"})

			require.Error(t, err)
			assert.Equal(t, tc.wantQuota, errors.Is(err, clients.ErrQuotaExhausted))
		})
	}
}

func TestCreateBucketJobQuotaExhausted(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/protocols/s3/services/{svm}/buckets", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"job": map[string]any{"uuid": testJobUUID},
		})
	})
	mux.HandleFunc("GET /api/cluster/jobs/{uuid}", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"state": "failure",
			"error": map[string]any{"message": quotaExhaustedMessage},
		})
	})

	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := newTestClient(t, server)
	err := client.CreateBucket(t.Context(), "test-bucket", map[string]string{"size": "1"})

	require.Error(t, err)
	assert.True(t, errors.Is(err, clients.ErrQuotaExhausted))
}
