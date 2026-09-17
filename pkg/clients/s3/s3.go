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

// Package s3 provides an ONTAP S3 client implementation to interact with ONTAP REST APIs.
package s3

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"sigs.k8s.io/container-object-storage-interface/client/apis/objectstorage/consts"

	cosi "sigs.k8s.io/container-object-storage-interface/proto"
	"sigs.k8s.io/cosi-driver-sample/pkg/clients"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	regionKey        = "region"
	objectLockingKey = "objectLocking"
	commentKey       = "comment"
	// sizeKey is a required bucket parameter: the size limit in GiB,
	// interpreted as a positive whole number (an explicit "Gi" suffix is also
	// accepted). Converted to bytes when sent to the backend.
	sizeKey = "size"

	// defaultJobPollInterval is the default interval between job status polls.
	defaultJobPollInterval = 2 * time.Second

	// defaultJobPollTimeout is the maximum time to wait for a job to complete.
	defaultJobPollTimeout = 5 * time.Minute
)

// Client represents an S3 client instance.
// It provides methods for performing operations on S3 buckets and managing user access.
type Client struct {
	httpClient *http.Client
	baseURL    *url.URL
	// s3Endpoint is the S3 data endpoint advertised to consumers (e.g. written
	// to the BucketAccess Secret as COSI_S3_ENDPOINT). When empty, the
	// management endpoint (baseURL) is advertised instead.
	s3Endpoint string
	// s3UseSSL selects https (true) or http (false) for the S3 endpoint
	// advertised to consumers. It applies when s3Endpoint has no explicit
	// scheme, and when s3Endpoint is empty and the management endpoint is
	// advertised instead. The management REST connection itself always
	// follows tlsCfg.
	s3UseSSL bool
	admin    S3Credentials
	svmUUID  string
	region   string
}

// Verify that Client implements the clients.Client interface.
var _ clients.Client = (*Client)(nil)

// S3Credentials represents the access credentials for an S3 service.
type S3Credentials struct {
	AccessKeyID     string // The access key ID for S3 authentication.
	AccessSecretKey string // The secret key for S3 authentication.
}

// TLSConfig holds the TLS configuration for connecting to the S3 endpoint.
type TLSConfig struct {
	// SSL enables HTTPS when true. When false, the connection uses plain HTTP.
	SSL bool

	// CACertPath points to a PEM-encoded CA certificate file used to verify
	// the S3 endpoint's TLS certificate. Leave empty to use the system CA pool.
	CACertPath string

	// SkipVerify disables TLS certificate verification entirely.
	// WARNING: This is insecure and should only be used for testing.
	SkipVerify bool
}

// user implements the clients.User interface and represents a user with access to an S3 bucket.
type user struct {
	S3Credentials        // Embedded S3 credentials for the user.
	name          string // The name of the user.
}

// Verify that user implements the clients.User interface.
var _ clients.User = (*user)(nil)

// Name returns the name of the user.
func (u *user) Name() string {
	return u.name
}

// Credentials returns a map of the user's S3 access credentials.
func (u *user) Credentials() map[string]string {
	return map[string]string{
		consts.S3SecretAccessKeyID:     u.AccessKeyID,
		consts.S3SecretAccessSecretKey: u.AccessSecretKey,
	}
}

// Platform returns the name of the platform associated with the user.
func (u *user) Platform() string {
	return consts.S3Key
}

// Config fully describes a per-instance ONTAP S3 client.
type Config struct {
	// Endpoint is the ONTAP management endpoint used for REST API calls.
	Endpoint string
	// S3Endpoint is the consumer-facing S3 data endpoint advertised to
	// consumers (may be empty, in which case Endpoint is advertised).
	S3Endpoint string
	// Region is the S3 region.
	Region string
	// SVMUUID is the UUID of the storage virtual machine hosting the buckets.
	SVMUUID string
	// Admin holds the management API credentials.
	Admin S3Credentials
	// TLS configures the management connection's TLS behavior.
	TLS TLSConfig
	// S3UseSSL selects https (true) or http (false) for the S3 endpoint
	// advertised to consumers.
	S3UseSSL bool
	// CACertPEM is an inline PEM-encoded CA certificate. When non-empty it is
	// used directly instead of TLS.CACertPath, so tenants can carry their own
	// CA without a mounted file.
	CACertPEM string
}

// NewFromConfig creates a new ONTAP S3 client instance from cfg.
func NewFromConfig(cfg Config) (*Client, error) {
	if strings.TrimSpace(cfg.Endpoint) == "" {
		return nil, errors.New("endpoint is required")
	}
	if strings.TrimSpace(cfg.SVMUUID) == "" {
		return nil, errors.New("svm UUID is required")
	}

	if !strings.Contains(cfg.Endpoint, "://") {
		scheme := "http"
		if cfg.TLS.SSL {
			scheme = "https"
		}
		cfg.Endpoint = scheme + "://" + cfg.Endpoint
	}

	if cfg.S3Endpoint != "" && !strings.Contains(cfg.S3Endpoint, "://") {
		scheme := "http"
		if cfg.S3UseSSL {
			scheme = "https"
		}
		cfg.S3Endpoint = scheme + "://" + cfg.S3Endpoint
	}

	u, err := url.Parse(cfg.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("unable to parse endpoint: %w", err)
	}

	u.Path = ""
	u.RawQuery = ""
	u.Fragment = ""

	httpClient := &http.Client{}
	if cfg.TLS.SSL {
		tlsConfig := &tls.Config{
			MinVersion: tls.VersionTLS12,
		}

		if cfg.TLS.SkipVerify {
			tlsConfig.InsecureSkipVerify = true
		}

		// caCert holds the PEM bytes to seed the root pool with. Inline
		// CACertPEM takes precedence over the CA file path.
		var caCert []byte
		if cfg.CACertPEM != "" {
			caCert = []byte(cfg.CACertPEM)
		} else if cfg.TLS.CACertPath != "" {
			ca, err := os.ReadFile(cfg.TLS.CACertPath)
			if err != nil {
				return nil, fmt.Errorf("unable to read CA certificate from %q: %w", cfg.TLS.CACertPath, err)
			}
			caCert = ca
		}

		if len(caCert) != 0 {
			caCertPool, err := x509.SystemCertPool()
			if err != nil {
				return nil, fmt.Errorf("unable to load system CA pool: %w", err)
			}

			if !caCertPool.AppendCertsFromPEM(caCert) {
				caSource := cfg.TLS.CACertPath
				if cfg.CACertPEM != "" {
					caSource = "inline CA certificate"
				}
				return nil, fmt.Errorf("no valid CA certificates found in %q", caSource)
			}

			tlsConfig.RootCAs = caCertPool
		}

		httpClient.Transport = &http.Transport{
			TLSClientConfig: tlsConfig,
		}
	}

	return &Client{
		httpClient: httpClient,
		baseURL:    u,
		s3Endpoint: cfg.S3Endpoint,
		s3UseSSL:   cfg.S3UseSSL,
		admin:      cfg.Admin,
		svmUUID:    cfg.SVMUUID,
		region:     cfg.Region,
	}, nil
}

// BucketExists checks if a bucket exists in the S3 service.
func (c *Client) BucketExists(ctx context.Context, bucket string) (bool, error) {
	records, err := c.listBucketsByName(ctx, bucket)
	if err != nil {
		return false, err
	}

	return len(records) > 0, nil
}

// IsBucketEqual checks if an existing bucket matches the requested parameters.
// Only the size limit is compared: when a size parameter is requested, the
// bucket is only considered equal if its configured size matches. Other
// parameters (e.g. comment) have no equality semantics and are ignored, keeping
// the behavior conservative for this sample.
func (c *Client) IsBucketEqual(ctx context.Context, bucket string, params map[string]string) (bool, error) {
	rawSize, found := params[sizeKey]
	if !found || strings.TrimSpace(rawSize) == "" {
		return true, nil
	}

	want, err := parseBucketSize(rawSize)
	if err != nil {
		return false, err
	}

	rec, err := c.bucketByName(ctx, bucket)
	if err != nil {
		return false, err
	}
	if rec == nil {
		return false, nil
	}

	return rec.Size == want, nil
}

// gib is the number of bytes in one GiB.
const gib = int64(1024 * 1024 * 1024)

// parseBucketSize parses the size parameter, which is expressed in GiB (a
// positive whole number; an explicit "Gi" suffix is also accepted). It returns
// the size in bytes. Zero and negative values are rejected: a bucket must be
// created with a positive size limit.
func parseBucketSize(raw string) (int64, error) {
	s := strings.TrimSpace(raw)
	if len(s) >= 2 && strings.EqualFold(s[len(s)-2:], "Gi") {
		s = s[:len(s)-2]
	}

	size, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil || size <= 0 {
		return 0, fmt.Errorf("invalid bucket size parameter %q: must be a positive whole number of GiB (e.g. \"10\" or \"10Gi\")", raw)
	}
	if size > math.MaxInt64/gib {
		return 0, fmt.Errorf("invalid bucket size parameter %q: too large", raw)
	}

	return size * gib, nil
}

// CreateBucket creates a new bucket in the ONTAP S3 service.
//
// The ONTAP API returns an asynchronous job. This method:
// 1. Sends a POST request to the ONTAP buckets API (returning a job reference).
// 2. Polls the job status until it completes.
// 3. If the job fails, returns an error with details.
func (c *Client) CreateBucket(ctx context.Context, bucket string, params map[string]string) error {
	// The bucket size limit is a required parameter: provisioning without a
	// size would silently create an unbounded bucket. 0 (unlimited) is also
	// rejected so that "required" is meaningful.
	rawSize := strings.TrimSpace(params[sizeKey])
	if rawSize == "" {
		return status.Error(codes.InvalidArgument, "size parameter is required: the bucket size limit in GiB (must be a positive whole number)")
	}
	size, err := parseBucketSize(rawSize)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}

	body := map[string]any{
		"name": bucket,
		"size": size,
	}
	if comment := strings.TrimSpace(params[commentKey]); comment != "" {
		body["comment"] = comment
	}
	var objectLocking bool
	if ol, found := params[objectLockingKey]; found && ol != "" {
		objectLocking, err = strconv.ParseBool(ol)
		if err != nil {
			return err
		}
	}
	if objectLocking {
		// Enabling object locking requires retention config in ONTAP.
		// Use a safe default retention to keep provisioning behavior deterministic.
		body["retention"] = map[string]string{
			"mode":           "governance",
			"default_period": "P1D",
		}
	}

	// Submit the async creation request and get the job reference.
	jobRef, err := c.createBucketAsync(ctx, body)
	if err != nil {
		return fmt.Errorf("failed to initiate bucket creation: %w", err)
	}

	// Poll the job until completion.
	jobStatus, err := c.waitForJob(ctx, jobRef.Job.UUID)
	if err != nil {
		return fmt.Errorf("failed waiting for bucket creation job: %w", err)
	}

	// Handle job failure.
	if jobStatus.State == "failure" {
		errMsg := jobStatus.Error.Message
		if errMsg == "" {
			errMsg = jobStatus.Message
		}
		return &ontapJobError{
			operation:  "create",
			jobState:   jobStatus.State,
			errorCode:  jobStatus.Error.Code,
			errorMsg:   errMsg,
			bucketName: bucket,
		}
	}

	return nil
}

// DeleteBucket deletes a bucket from the ONTAP S3 service.
//
// The ONTAP API returns an asynchronous job. This method:
// 1. Looks up the bucket by name to find its UUID.
// 2. Sends a DELETE request to the ONTAP buckets API (returning a job reference).
// 3. Polls the job status until it completes.
// 4. If the job fails (e.g., non-empty bucket), returns an error with details.
func (c *Client) DeleteBucket(ctx context.Context, bucket string) error {
	existing, err := c.bucketByName(ctx, bucket)
	if err != nil {
		return err
	}
	if existing == nil {
		return nil
	}

	// Submit the async deletion request and get the job reference.
	jobRef, err := c.deleteBucketAsync(ctx, existing.UUID)
	if err != nil {
		return fmt.Errorf("failed to initiate bucket deletion: %w", err)
	}

	// Poll the job until completion.
	jobStatus, err := c.waitForJob(ctx, jobRef.Job.UUID)
	if err != nil {
		return fmt.Errorf("failed waiting for bucket deletion job: %w", err)
	}

	// Handle job failure.
	if jobStatus.State == "failure" {
		errMsg := jobStatus.Error.Message
		if errMsg == "" {
			errMsg = jobStatus.Message
		}
		return &ontapJobError{
			operation:  "deletion",
			jobState:   jobStatus.State,
			errorCode:  jobStatus.Error.Code,
			errorMsg:   errMsg,
			bucketName: bucket,
		}
	}

	return nil
}

// CreateBucketAccess creates access credentials for a bucket.
func (c *Client) CreateBucketAccess(ctx context.Context, bucket, userID string) (clients.User, error) {
	policyName := normalizedName(fmt.Sprintf("cosi-%s-%s-policy", bucket, userID), 128)
	groupName := normalizedName(fmt.Sprintf("cosi-%s-%s-group", bucket, userID), 64)

	if err := c.createOrUpdatePolicy(ctx, policyName, bucket); err != nil {
		return nil, err
	}

	creds, err := c.createOrRotateUser(ctx, userID)
	if err != nil {
		return nil, err
	}

	if err := c.createGroupIfNeeded(ctx, groupName, userID, policyName); err != nil {
		return nil, err
	}

	return &user{
		S3Credentials: creds,
		name:          userID,
	}, nil
}

// DeleteBucketAccess removes access credentials for a bucket.
func (c *Client) DeleteBucketAccess(ctx context.Context, bucket, userID string) error {
	policyName := normalizedName(fmt.Sprintf("cosi-%s-%s-policy", bucket, userID), 128)
	groupName := normalizedName(fmt.Sprintf("cosi-%s-%s-group", bucket, userID), 64)

	if err := c.deleteGroupIfExists(ctx, groupName); err != nil {
		return err
	}
	if err := c.deletePolicyIfExists(ctx, policyName); err != nil {
		return err
	}
	if err := c.deleteUserIfExists(ctx, userID); err != nil {
		return err
	}

	return nil
}

// Protocol returns detailed information about protocol supported by the storage backend.
func (c *Client) ProtocolInfo() *cosi.ObjectProtocol {
	return &cosi.ObjectProtocol{
		Type: cosi.ObjectProtocol_S3,
	}
}

// S3Endpoint returns the S3 endpoint URL advertised to consumers. When no
// dedicated S3 endpoint is configured, the management endpoint is used. The
// scheme always follows s3UseSSL: https when enabled, http otherwise.
func (c *Client) S3Endpoint() string {
	if c.s3Endpoint != "" {
		return c.s3Endpoint
	}
	scheme := "http"
	if c.s3UseSSL {
		scheme = "https"
	}
	if c.baseURL.Scheme == scheme {
		return c.baseURL.String()
	}
	u := *c.baseURL
	u.Scheme = scheme
	return u.String()
}

// S3Region returns the S3 region.
func (c *Client) S3Region() string {
	return c.region
}

type ontapBucketRecord struct {
	UUID string `json:"uuid"`
	Name string `json:"name"`
	Size int64  `json:"size,omitempty"`
}

type ontapUserRecord struct {
	Name      string `json:"name"`
	AccessKey string `json:"access_key"`
	SecretKey string `json:"secret_key"`
}

// ontapGroupRecord is a group listed by the ONTAP S3 API.
// Real ONTAP serializes the group ID as a JSON number; the ONTAP9 stub
// serializes it as a JSON string. groupID accepts both forms.
type groupID string

// UnmarshalJSON accepts a group ID serialized as either a JSON number or a string.
func (id *groupID) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		*id = groupID(s)
		return nil
	}

	var n int64
	if err := json.Unmarshal(data, &n); err != nil {
		return fmt.Errorf("invalid group id %q: %w", string(data), err)
	}
	*id = groupID(strconv.FormatInt(n, 10))
	return nil
}

type ontapGroupRecord struct {
	ID   groupID `json:"id"`
	Name string  `json:"name"`
}

type ontapPolicyRecord struct {
	Name string `json:"name"`
}

type ontapRecordsResponse[T any] struct {
	NumRecords int `json:"num_records"`
	Records    []T `json:"records"`
}

type ontapErrorResponse struct {
	Error struct {
		Message string `json:"message"`
		Code    string `json:"code"`
	} `json:"error"`
}

type ontapHTTPError struct {
	statusCode int
	message    string
}

func (e *ontapHTTPError) Error() string {
	return fmt.Sprintf("ontap API error: status=%d message=%q", e.statusCode, e.message)
}

// ontapJobRefResponse represents the response from an async ONTAP operation
// that returns a job reference.
type ontapJobRefResponse struct {
	Job struct {
		UUID string `json:"uuid"`
	} `json:"job"`
}

// ontapJobStatus represents the status of an ONTAP job.
type ontapJobStatus struct {
	UUID        string `json:"uuid"`
	Description string `json:"description"`
	State       string `json:"state"`
	Message     string `json:"message"`
	Code        int    `json:"code"`
	StartTime   string `json:"start_time"`
	EndTime     string `json:"end_time"`
	Error       struct {
		Message string `json:"message"`
		Code    string `json:"code"`
	} `json:"error"`
}

// ontapJobError is returned when an async ONTAP job (create or delete) fails.
type ontapJobError struct {
	operation  string
	jobState   string
	errorCode  string
	errorMsg   string
	bucketName string
}

func (e *ontapJobError) Error() string {
	return fmt.Sprintf("bucket %s job failed: state=%q bucket=%q code=%q message=%q",
		e.operation, e.jobState, e.bucketName, e.errorCode, e.errorMsg)
}

func (c *Client) servicesPath(parts ...string) string {
	allParts := append([]string{"api", "protocols", "s3", "services", c.svmUUID}, parts...)
	escaped := make([]string, 0, len(allParts))
	for _, p := range allParts {
		escaped = append(escaped, url.PathEscape(strings.TrimSpace(p)))
	}

	return "/" + path.Join(escaped...)
}

func (c *Client) doJSON(
	ctx context.Context,
	method, endpointPath string,
	query map[string]string,
	requestBody any,
	responseBody any,
) error {
	var bodyReader io.Reader
	if requestBody != nil {
		payload, err := json.Marshal(requestBody)
		if err != nil {
			return fmt.Errorf("failed to marshal request body: %w", err)
		}
		bodyReader = bytes.NewReader(payload)
	}

	requestURL := *c.baseURL
	requestURL.Path = endpointPath

	q := requestURL.Query()
	for k, v := range query {
		q.Set(k, v)
	}
	requestURL.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, method, requestURL.String(), bodyReader)
	if err != nil {
		return fmt.Errorf("failed to build request: %w", err)
	}

	req.Header.Set("Accept", "application/json")
	if requestBody != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.SetBasicAuth(c.admin.AccessKeyID, c.admin.AccessSecretKey)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck // best effort

	rawBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		apiErr := ontapErrorResponse{}
		if len(rawBody) > 0 {
			_ = json.Unmarshal(rawBody, &apiErr)
		}
		msg := strings.TrimSpace(apiErr.Error.Message)
		if msg == "" {
			msg = strings.TrimSpace(string(rawBody))
		}

		return &ontapHTTPError{
			statusCode: resp.StatusCode,
			message:    msg,
		}
	}

	if responseBody != nil && len(rawBody) > 0 {
		if err := json.Unmarshal(rawBody, responseBody); err != nil {
			return fmt.Errorf("failed to decode response body: %w", err)
		}
	}

	return nil
}

func (c *Client) listBucketsByName(ctx context.Context, name string) ([]ontapBucketRecord, error) {
	query := map[string]string{
		"name":           name,
		"return_records": "true",
	}
	resp := ontapRecordsResponse[ontapBucketRecord]{}
	if err := c.doJSON(ctx, http.MethodGet, c.servicesPath("buckets"), query, nil, &resp); err != nil {
		return nil, err
	}

	return resp.Records, nil
}

func (c *Client) bucketByName(ctx context.Context, name string) (*ontapBucketRecord, error) {
	records, err := c.listBucketsByName(ctx, name)
	if err != nil {
		return nil, err
	}
	for _, r := range records {
		if r.Name == name {
			return &r, nil
		}
	}

	return nil, nil
}

func (c *Client) createOrUpdatePolicy(ctx context.Context, policyName, bucket string) error {
	body := map[string]any{
		"name":    policyName,
		"comment": fmt.Sprintf("COSI access policy for bucket %s", bucket),
		"statements": []map[string]any{
			{
				"sid":     "COSIAllowBucketAccess",
				"effect":  "allow",
				"actions": []string{"*"},
				"resources": []string{
					bucket,
					fmt.Sprintf("%s/*", bucket),
				},
			},
		},
	}

	err := c.doJSON(ctx, http.MethodPost, c.servicesPath("policies"), nil, body, nil)
	if err == nil {
		return nil
	}
	if !isHTTPStatus(err, http.StatusConflict) {
		return err
	}

	return c.doJSON(ctx, http.MethodPatch, c.servicesPath("policies", policyName), nil, map[string]any{
		"comment":    body["comment"],
		"statements": body["statements"],
	}, nil)
}

func (c *Client) createOrRotateUser(ctx context.Context, userName string) (S3Credentials, error) {
	body := map[string]string{
		"name":    userName,
		"comment": "COSI managed bucket access user",
	}
	resp := ontapRecordsResponse[ontapUserRecord]{}
	err := c.doJSON(ctx, http.MethodPost, c.servicesPath("users"), nil, body, &resp)
	if err != nil {
		if !isHTTPStatus(err, http.StatusConflict) {
			return S3Credentials{}, err
		}
		if err := c.doJSON(ctx, http.MethodPatch, c.servicesPath("users", userName), map[string]string{
			"regenerate_keys": "true",
		}, map[string]string{}, &resp); err != nil {
			return S3Credentials{}, err
		}
	}

	if len(resp.Records) == 0 {
		return S3Credentials{}, errors.New("ONTAP response did not include generated user credentials")
	}

	return S3Credentials{
		AccessKeyID:     resp.Records[0].AccessKey,
		AccessSecretKey: resp.Records[0].SecretKey,
	}, nil
}

func (c *Client) createGroupIfNeeded(ctx context.Context, groupName, userName, policyName string) error {
	body := map[string]any{
		"name":    groupName,
		"comment": "COSI managed S3 access group",
		"users": []map[string]string{
			{"name": userName},
		},
		"policies": []map[string]string{
			{"name": policyName},
		},
	}
	err := c.doJSON(ctx, http.MethodPost, c.servicesPath("groups"), nil, body, nil)
	if err != nil && !isHTTPStatus(err, http.StatusConflict) {
		return err
	}

	return nil
}

func (c *Client) listGroupsByName(ctx context.Context, name string) ([]ontapGroupRecord, error) {
	query := map[string]string{
		"name":           name,
		"return_records": "true",
	}
	resp := ontapRecordsResponse[ontapGroupRecord]{}
	if err := c.doJSON(ctx, http.MethodGet, c.servicesPath("groups"), query, nil, &resp); err != nil {
		return nil, err
	}

	return resp.Records, nil
}

func (c *Client) deleteGroupIfExists(ctx context.Context, groupName string) error {
	groups, err := c.listGroupsByName(ctx, groupName)
	if err != nil {
		return err
	}
	for _, g := range groups {
		if g.Name != groupName {
			continue
		}

		if err := c.doJSON(ctx, http.MethodDelete, c.servicesPath("groups", string(g.ID)), nil, nil, nil); err != nil && !isHTTPStatus(err, http.StatusNotFound) {
			return err
		}
	}

	return nil
}

func (c *Client) deletePolicyIfExists(ctx context.Context, policyName string) error {
	err := c.doJSON(ctx, http.MethodDelete, c.servicesPath("policies", policyName), nil, nil, nil)
	if err != nil && !isHTTPStatus(err, http.StatusNotFound) {
		return err
	}

	return nil
}

func (c *Client) deleteUserIfExists(ctx context.Context, userName string) error {
	err := c.doJSON(ctx, http.MethodDelete, c.servicesPath("users", userName), nil, nil, nil)
	if err != nil && !isHTTPStatus(err, http.StatusNotFound) {
		return err
	}

	return nil
}

func isHTTPStatus(err error, code int) bool {
	var apiErr *ontapHTTPError
	if !errors.As(err, &apiErr) {
		return false
	}

	return apiErr.statusCode == code
}

func normalizedName(name string, maxLen int) string {
	name = strings.ToLower(name)
	replacer := strings.NewReplacer(
		"/", "-",
		" ", "-",
		":", "-",
	)
	name = replacer.Replace(name)
	if len(name) <= maxLen {
		return name
	}

	return name[:maxLen]
}

// bucketsPath constructs the path to the ONTAP buckets API for the configured SVM.
// This uses the SVM-scoped path: /api/protocols/s3/buckets/{svmUuid}[/{bucketUuid}]
func (c *Client) bucketsPath(parts ...string) string {
	allParts := append([]string{"api", "protocols", "s3", "buckets", c.svmUUID}, parts...)
	escaped := make([]string, 0, len(allParts))
	for _, p := range allParts {
		escaped = append(escaped, url.PathEscape(strings.TrimSpace(p)))
	}

	return "/" + path.Join(escaped...)
}

// jobsPath constructs the path to the ONTAP cluster jobs API.
func (c *Client) jobsPath(jobUUID string) string {
	escaped := url.PathEscape(strings.TrimSpace(jobUUID))
	return "/" + path.Join("api", "cluster", "jobs", escaped)
}

// createBucketAsync sends a POST request to the ONTAP S3 services buckets API and returns
// the job reference.
func (c *Client) createBucketAsync(ctx context.Context, body map[string]any) (*ontapJobRefResponse, error) {
	var jobRef ontapJobRefResponse
	if err := c.doJSON(ctx, http.MethodPost, c.servicesPath("buckets"), nil, body, &jobRef); err != nil {
		return nil, err
	}

	if jobRef.Job.UUID == "" {
		return nil, errors.New("ONTAP did not return a job UUID for bucket creation")
	}

	return &jobRef, nil
}

// deleteBucketAsync sends a DELETE request to the ONTAP buckets API and returns
// the job reference. The bucket is identified by its UUID on the ONTAP side.
func (c *Client) deleteBucketAsync(ctx context.Context, bucketUUID string) (*ontapJobRefResponse, error) {
	var jobRef ontapJobRefResponse
	if err := c.doJSON(ctx, http.MethodDelete, c.bucketsPath(bucketUUID), nil, nil, &jobRef); err != nil {
		return nil, err
	}

	if jobRef.Job.UUID == "" {
		return nil, errors.New("ONTAP did not return a job UUID for bucket deletion")
	}

	return &jobRef, nil
}

// waitForJob polls the ONTAP job status endpoint until the job reaches a terminal
// state ("success" or "failure"), or until the context is cancelled or the timeout
// is reached.
func (c *Client) waitForJob(ctx context.Context, jobUUID string) (*ontapJobStatus, error) {
	pollCtx, cancel := context.WithTimeout(ctx, defaultJobPollTimeout)
	defer cancel()

	ticker := time.NewTicker(defaultJobPollInterval)
	defer ticker.Stop()

	for {
		var status ontapJobStatus
		if err := c.doJSON(pollCtx, http.MethodGet, c.jobsPath(jobUUID), nil, nil, &status); err != nil {
			return nil, err
		}

		switch status.State {
		case "success":
			return &status, nil
		case "failure":
			return &status, nil
		case "":
			// Job state may be empty if it hasn't started yet; treat as pending.
		}

		select {
		case <-pollCtx.Done():
			return nil, fmt.Errorf("timed out waiting for job %s to complete: %w", jobUUID, pollCtx.Err())
		case <-ticker.C:
			// Continue polling.
		case <-ctx.Done():
			return nil, fmt.Errorf("context cancelled while waiting for job %s: %w", jobUUID, ctx.Err())
		}
	}
}
