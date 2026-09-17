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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClient_S3Endpoint(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		mgmtEndpoint string
		s3Endpoint   string
		mgmtSSL      bool
		s3SSL        bool
		want         string
	}{
		"dedicated s3 endpoint takes precedence": {
			mgmtEndpoint: "ontap-mgmt.example.com",
			s3Endpoint:   "s3.example.com",
			want:         "http://s3.example.com",
		},
		"dedicated s3 endpoint with scheme is kept verbatim": {
			mgmtEndpoint: "http://ontap-mgmt.example.com",
			s3Endpoint:   "https://s3.example.com",
			want:         "https://s3.example.com",
		},
		"https scheme applied to bare s3 endpoint when s3 ssl enabled": {
			mgmtEndpoint: "ontap-mgmt.example.com",
			s3Endpoint:   "s3.example.com",
			s3SSL:        true,
			want:         "https://s3.example.com",
		},
		"management ssl does not force the s3 endpoint scheme": {
			mgmtEndpoint: "ontap-mgmt.example.com",
			s3Endpoint:   "s3.example.com",
			mgmtSSL:      true,
			want:         "http://s3.example.com",
		},
		"falls back to management endpoint when s3 endpoint unset": {
			mgmtEndpoint: "ontap-mgmt.example.com",
			want:         "http://ontap-mgmt.example.com",
		},
		"fallback switches to https when s3 ssl enabled": {
			mgmtEndpoint: "ontap-mgmt.example.com",
			s3SSL:        true,
			want:         "https://ontap-mgmt.example.com",
		},
		"fallback keeps https when s3 ssl matches management ssl": {
			mgmtEndpoint: "ontap-mgmt.example.com",
			mgmtSSL:      true,
			s3SSL:        true,
			want:         "https://ontap-mgmt.example.com",
		},
	}

	for name, tc := range tests {
		tc := tc
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			client, err := NewFromConfig(Config{
				Endpoint:   tc.mgmtEndpoint,
				S3Endpoint: tc.s3Endpoint,
				Region:     "us-east-1",
				SVMUUID:    "11111111-2222-3333-4444-555555555555",
				Admin:      S3Credentials{},
				TLS:        TLSConfig{SSL: tc.mgmtSSL},
				S3UseSSL:   tc.s3SSL,
			})
			require.NoError(t, err)
			assert.Equal(t, tc.want, client.S3Endpoint())
		})
	}
}
