// Copyright 2021-2024 The Kubernetes Authors.
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

package driver

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"k8s.io/klog/v2"
	"sigs.k8s.io/container-object-storage-interface/client/apis/objectstorage/consts"
	cosi "sigs.k8s.io/container-object-storage-interface/proto"
	"sigs.k8s.io/cosi-driver-sample/pkg/clients"
	"sigs.k8s.io/cosi-driver-sample/pkg/config"
)

var ErrBucketNotFound = errors.New("bucket not found")

// ProvisionerServer implements the COSI driver server interface.
type ProvisionerServer struct {
	cosi.UnimplementedProvisionerServer

	Client clients.Client
	Config config.Config
	// Tenant resolves per-tenant credentials when multi-tenant mode is on;
	// nil disables it.
	Tenant TenantCredentialsLoader
}

// resolveClient returns the backend client for this request: the
// tenant-scoped client when a tenant namespace parameter is present and
// multi-tenant is on, otherwise the driver's own instance-configured client.
func (s *ProvisionerServer) resolveClient(ctx context.Context, parameters map[string]string) (clients.Client, error) {
	if s.Tenant == nil {
		return s.Client, nil
	}
	ns := parameters[TenantNamespaceParam]
	if ns == "" {
		return s.Client, nil
	}
	client, err := s.Tenant.Load(ctx, ns)
	if err != nil {
		// Unavailable keeps the sidecar retrying so a tenant heals once its
		// credentials Secret is created or fixed; configuration errors are
		// fixable in place, so they surface as retryable too.
		return nil, status.Errorf(codes.Unavailable, "%s", err)
	}
	return client, nil
}

// DriverCreateBucket creates a bucket if it does not already exist.
// If the bucket exists and the parameters match, it returns success without error.
// If the bucket exists but the parameters differ, it returns a conflict error.
//
// Return values:
//   - nil: The bucket was successfully created or already exists with matching parameters.
//   - codes.AlreadyExists: The bucket already exists but with different parameters.
//   - error: Internal error requiring retries.
func (s *ProvisionerServer) DriverCreateBucket(
	ctx context.Context,
	req *cosi.DriverCreateBucketRequest,
) (*cosi.DriverCreateBucketResponse, error) {
	client, err := s.resolveClient(ctx, req.GetParameters())
	if err != nil {
		return nil, err
	}
	bucketName, overridden := s.getName(req)
	parameters := req.GetParameters()

	if err := s.Config.Errors.CreateBucket; err != nil {
		klog.ErrorS(err, "Purposefully failing DriverCreateBucket call", "bucket", bucketName, "parameters", parameters)
		return nil, status.Error(err.Code, err.Message)
	}

	exists, err := client.BucketExists(ctx, bucketName)
	if err != nil {
		klog.ErrorS(err, "Failed to check bucket existence", "bucket", bucketName, "parameters", parameters)
		return nil, status.Errorf(codes.Internal, "%s", err)
	}
	if exists {
		if overridden {
			klog.InfoS("Overridden bucket exists, skipping validation", "bucket", bucketName, "parameters", parameters)
			return &cosi.DriverCreateBucketResponse{BucketId: bucketName}, nil
		}

		equal, err := client.IsBucketEqual(ctx, bucketName, parameters)
		if err != nil {
			klog.ErrorS(err, "Failed to compare bucket with expected parameters", "bucket", bucketName, "parameters", parameters)
			return nil, status.Errorf(codes.Internal, "%s", err)
		}
		if equal {
			klog.InfoS("Bucket already exists with matching parameters", "bucket", bucketName)
			return &cosi.DriverCreateBucketResponse{BucketId: bucketName}, nil
		}

		klog.InfoS("Bucket already exists with differing parameters", "bucket", bucketName)
		return nil, status.Errorf(codes.AlreadyExists, "bucket already exists: %s", bucketName)
	}

	if err := client.CreateBucket(ctx, bucketName, parameters); err != nil {
		klog.ErrorS(err, "Failed to create bucket", "bucket", bucketName)
		return nil, err
	}

	protocolInfo := client.ProtocolInfo()
	protocols := &cosi.ObjectProtocolAndBucketInfo{}
	switch protocolInfo.GetType() {
	case cosi.ObjectProtocol_S3:
		protocols.S3 = &cosi.S3BucketInfo{
			BucketId: bucketName,
			Endpoint: client.S3Endpoint(),
			Region:   client.S3Region(),
			AddressingStyle: &cosi.S3AddressingStyle{
				Style: cosi.S3AddressingStyle_PATH,
			},
		}
	case cosi.ObjectProtocol_AZURE:
		protocols.Azure = &cosi.AzureBucketInfo{}
	}
	return &cosi.DriverCreateBucketResponse{
		BucketId:  bucketName,
		Protocols: protocols,
	}, nil
}

// DriverDeleteBucket deletes a bucket if it exists. If the bucket does not exist, it returns success.
//
// Return values:
//   - nil: The bucket was successfully deleted or does not exist.
//   - error: Internal error requiring retries.
func (s *ProvisionerServer) DriverDeleteBucket(
	ctx context.Context,
	req *cosi.DriverDeleteBucketRequest,
) (*cosi.DriverDeleteBucketResponse, error) {
	client, err := s.resolveClient(ctx, req.GetParameters())
	if err != nil {
		return nil, err
	}
	bucketId := s.getBucketID(req)

	if err := s.Config.Errors.DeleteBucket; err != nil {
		klog.ErrorS(err, "Purposefully failing DriverDeleteBucket call", "bucket", bucketId)
		return nil, status.Error(err.Code, err.Message)
	}

	if err := client.DeleteBucket(ctx, bucketId); err != nil {
		klog.ErrorS(err, "Failed to delete bucket", "bucket", bucketId)
		return nil, status.Errorf(codes.Internal, "%s", err)
	}

	klog.InfoS("Bucket successfully deleted", "bucket", bucketId)
	return &cosi.DriverDeleteBucketResponse{}, nil
}

// DriverGrantBucketAccess grants access to a bucket. It creates an access account for the given bucket and user.
//
// Multi-bucket requests: one shared account (same S3 keys) is created and
// bound to every requested bucket via a per-bucket policy+group, matching the
// single-bucket layout. The response carries one BucketInfo entry per
// requested bucket.
//
// Return values:
//   - nil: Access successfully granted.
//   - error: Internal error requiring retries.
func (s *ProvisionerServer) DriverGrantBucketAccess(
	ctx context.Context,
	req *cosi.DriverGrantBucketAccessRequest,
) (*cosi.DriverGrantBucketAccessResponse, error) {
	client, err := s.resolveClient(ctx, req.GetParameters())
	if err != nil {
		return nil, err
	}
	buckets := req.GetBuckets()
	if len(buckets) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "grant request contains no buckets")
	}
	name := req.GetAccountName()

	if err := s.Config.Errors.GrantBucketAccess; err != nil {
		klog.ErrorS(err, "Purposefully failing DriverGrantBucketAccess call", "account", name)
		return nil, status.Error(err.Code, err.Message)
	}

	bucketInfos := make([]*cosi.DriverGrantBucketAccessResponse_BucketInfo, 0, len(buckets))
	var credentialInfo *cosi.CredentialInfo
	for _, b := range buckets {
		bucketId := b.GetBucketId()

		exists, err := client.BucketExists(ctx, bucketId)
		if err != nil {
			klog.ErrorS(err, "Failed to check bucket existence", "bucket", bucketId, "account", name)
			return nil, status.Errorf(codes.Internal, "%s", err)
		}
		if !exists {
			klog.ErrorS(ErrBucketNotFound, "Cannot grant access to nonexistent bucket", "bucket", bucketId, "account", name)
			return nil, status.Errorf(codes.NotFound, "%s", ErrBucketNotFound)
		}

		access, err := client.CreateBucketAccess(ctx, bucketId, name)
		if err != nil {
			klog.ErrorS(err, "Failed to create bucket access", "bucket", bucketId, "account", name)
			return nil, status.Errorf(codes.Internal, "%s", err)
		}

		// All buckets share one account, so the credentials are the same for
		// every iteration; the last value is representative of the final keys.
		creds := access.Credentials()
		credentialInfo = &cosi.CredentialInfo{}
		switch access.Platform() {
		case consts.S3Key:
			credentialInfo.S3 = &cosi.S3CredentialInfo{
				AccessKeyId:     creds[consts.S3SecretAccessKeyID],
				AccessSecretKey: creds[consts.S3SecretAccessSecretKey],
			}
		case consts.AzureKey:
			credentialInfo.Azure = &cosi.AzureCredentialInfo{
				AccessToken: creds[consts.AzureSecretAccessToken],
			}
		}

		bucketInfos = append(bucketInfos, &cosi.DriverGrantBucketAccessResponse_BucketInfo{
			BucketId: bucketId,
			BucketInfo: &cosi.ObjectProtocolAndBucketInfo{
				S3: &cosi.S3BucketInfo{
					BucketId: bucketId,
					Endpoint: client.S3Endpoint(),
					Region:   client.S3Region(),
					AddressingStyle: &cosi.S3AddressingStyle{
						Style: cosi.S3AddressingStyle_PATH,
					},
				},
			},
		})
	}

	return &cosi.DriverGrantBucketAccessResponse{
		AccountId:   name,
		Buckets:     bucketInfos,
		Credentials: credentialInfo,
	}, nil
}

// DriverRevokeBucketAccess revokes access to a bucket for a specific account.
// If the access does not exist, it returns success.
//
// Multi-bucket requests: the shared account is unbound from every requested
// bucket. Deleting the shared user is idempotent (missing user -> 404 is
// tolerated), so per-bucket revocation converges regardless of iteration order.
//
// Return values:
//   - nil: Access successfully revoked or does not exist.
//   - error: Internal error requiring retries.
func (s *ProvisionerServer) DriverRevokeBucketAccess(
	ctx context.Context,
	req *cosi.DriverRevokeBucketAccessRequest,
) (*cosi.DriverRevokeBucketAccessResponse, error) {
	client, err := s.resolveClient(ctx, req.GetParameters())
	if err != nil {
		return nil, err
	}
	buckets := req.GetBuckets()
	if len(buckets) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "revoke request contains no buckets")
	}
	accountId := req.GetAccountId()

	if err := s.Config.Errors.RevokeBucketAccess; err != nil {
		klog.ErrorS(err, "Purposefully failing DriverRevokeBucketAccess call", "account", accountId)
		return nil, status.Error(err.Code, err.Message)
	}

	for _, b := range buckets {
		bucketId := b.GetBucketId()
		if id := s.Config.Overrides.BucketID; id != "" {
			bucketId = id
		}

		if err := client.DeleteBucketAccess(ctx, bucketId, accountId); err != nil {
			klog.ErrorS(err, "Failed to revoke bucket access", "bucket", bucketId, "account", accountId)
			return nil, status.Errorf(codes.Internal, "%s", err)
		}

		klog.InfoS("Bucket access successfully revoked", "bucket", bucketId, "account", accountId)
	}

	return &cosi.DriverRevokeBucketAccessResponse{}, nil
}

func (s *ProvisionerServer) getName(req interface{ GetName() string }) (string, bool) {
	if id := s.Config.Overrides.BucketID; id != "" {
		return id, false
	}

	return req.GetName(), true
}

func (s *ProvisionerServer) getBucketID(req interface{ GetBucketId() string }) string {
	if id := s.Config.Overrides.BucketID; id != "" {
		return id
	}

	return req.GetBucketId()
}
