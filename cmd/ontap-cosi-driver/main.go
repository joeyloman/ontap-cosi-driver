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

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	cosi "sigs.k8s.io/container-object-storage-interface/proto"
	"sigs.k8s.io/cosi-driver-sample/pkg/clients"
	"sigs.k8s.io/cosi-driver-sample/pkg/clients/fake"
	"sigs.k8s.io/cosi-driver-sample/pkg/clients/s3"
	"sigs.k8s.io/cosi-driver-sample/pkg/config"
	"sigs.k8s.io/cosi-driver-sample/pkg/driver"
	yaml "sigs.k8s.io/yaml/goyaml.v3"
)

type runOptions struct {
	driverName   string
	cosiEndpoint string
	configPath   string

	mgmtEndpoint string
	s3Endpoint   string
	svmUUID      string
	region       string
	admin        s3.S3Credentials
	tls          s3.TLSConfig
	s3UseSSL     bool
	multitenant  bool
}

func defaultEnv(key, defaultValue string) string {
	val, found := os.LookupEnv(key)
	if !found || val == "" {
		return defaultValue
	}

	return strings.TrimSpace(val)
}

func asBool(v string) bool {
	b, _ := strconv.ParseBool(v)
	return b
}

func main() {
	klog.InitFlags(nil)
	flag.Parse()

	mgmtSSL := asBool(defaultEnv("ONTAP_MGMT_USE_SSL", "true"))
	opts := runOptions{
		cosiEndpoint: defaultEnv("COSI_ENDPOINT", "unix:///var/lib/cosi/cosi.sock"),
		driverName:   defaultEnv("X_COSI_DRIVER_NAME", "ontap.objectstorage.k8s.io"),
		configPath:   defaultEnv("X_COSI_CONFIG", "/etc/cosi/config.yaml"),
		mgmtEndpoint: defaultEnv("ONTAP_MGMT_ENDPOINT", ""),
		s3Endpoint:   defaultEnv("ONTAP_S3_ENDPOINT", ""),
		svmUUID:      defaultEnv("ONTAP_MGMT_SVM_UUID", ""),
		region:       defaultEnv("ONTAP_S3_REGION", ""),
		admin: s3.S3Credentials{
			AccessKeyID:     defaultEnv("ONTAP_MGMT_USERNAME", ""),
			AccessSecretKey: defaultEnv("ONTAP_MGMT_PASSWORD", ""),
		},
		tls: s3.TLSConfig{
			SSL:        mgmtSSL,
			CACertPath: defaultEnv("ONTAP_MGMT_CA_CERT_PATH", ""),
			SkipVerify: asBool(defaultEnv("ONTAP_MGMT_SKIP_TLS_VERIFY", "false")),
		},
		// Whether the S3 endpoint advertised to consumers (COSI_S3_ENDPOINT)
		// uses https. Falls back to the management SSL setting so deployments
		// without ONTAP_S3_USE_SSL keep their previous behavior.
		s3UseSSL:    asBool(defaultEnv("ONTAP_S3_USE_SSL", strconv.FormatBool(mgmtSSL))),
		multitenant: asBool(defaultEnv("ONTAP_MULTITENANT", "false")),
	}

	if err := run(context.Background(), opts); err != nil {
		klog.ErrorS(err, "Exiting on error")
		os.Exit(1)
	}
}

func run(ctx context.Context, opts runOptions) error {
	ctx, stop := signal.NotifyContext(ctx,
		os.Interrupt,
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	defer stop()

	cfg := config.Config{}

	f, err := os.Open(opts.configPath)
	if err != nil {
		return fmt.Errorf("unable to open config: %w", err)
	}
	defer f.Close() //nolint:errcheck // best effort call

	if err := yaml.NewDecoder(f).Decode(&cfg); err != nil {
		return fmt.Errorf("unable to read config: %w", err)
	}

	var (
		c            clients.Client
		tenantLoader driver.TenantCredentialsLoader
	)
	switch cfg.Mode {
	case config.ModeAzure:
		// TODO: implement real minimal Azure connector?
		panic("unimplemented")

	case config.ModeS3:
		s3Defaults := s3.Config{
			Endpoint:   opts.mgmtEndpoint,
			S3Endpoint: opts.s3Endpoint,
			Region:     opts.region,
			SVMUUID:    opts.svmUUID,
			Admin:      opts.admin,
			TLS:        opts.tls,
			S3UseSSL:   opts.s3UseSSL,
		}
		c, err = s3.NewFromConfig(s3Defaults)
		if err != nil {
			return fmt.Errorf("unable to create s3 client: %w", err)
		}

		if opts.multitenant {
			kubeConfig, err := rest.InClusterConfig()
			if err != nil {
				return fmt.Errorf("multi-tenant mode requires an in-cluster configuration: %w", err)
			}
			clientset, err := kubernetes.NewForConfig(kubeConfig)
			if err != nil {
				return fmt.Errorf("unable to create kubernetes client for multi-tenant mode: %w", err)
			}
			tenantLoader = driver.NewTenantCredentialsLoader(clientset, s3Defaults)
			klog.InfoS("Multi-tenant mode enabled")
		}

	case config.ModeAzureFake:
		c = fake.New("azure")

	case config.ModeS3Fake:
		c = fake.New("s3")
	}
	if opts.multitenant && tenantLoader == nil {
		klog.InfoS("Multi-tenant mode requested but ignored outside s3:impl mode", "mode", string(cfg.Mode))
	}

	identityServer := &driver.IdentityServer{Name: opts.driverName}
	provisionerServer := &driver.ProvisionerServer{
		Client: c,
		Config: cfg,
		Tenant: tenantLoader,
	}

	server, err := grpcServer(identityServer, provisionerServer)
	if err != nil {
		return fmt.Errorf("gRPC server creation failed: %w", err)
	}

	lis, cleanup, err := listener(ctx, opts.cosiEndpoint)
	if err != nil {
		return fmt.Errorf("failed to create listener for %s: %w", opts.cosiEndpoint, err)
	}
	defer cleanup()

	var wg sync.WaitGroup
	wg.Add(1)
	go shutdown(ctx, &wg, server)

	if err = server.Serve(lis); err != nil {
		return fmt.Errorf("gRPC server failed: %w", err)
	}

	wg.Wait()
	return nil
}

func listener(
	ctx context.Context,
	cosiEndpoint string,
) (net.Listener, func(), error) {
	endpointURL, err := url.Parse(cosiEndpoint)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to parse COSI endpoint: %w", err)
	}

	listenConfig := net.ListenConfig{}

	if endpointURL.Scheme == "unix" {
		_ = os.Remove(endpointURL.Path) // Cleanup stale socket
	}

	listener, err := listenConfig.Listen(ctx, endpointURL.Scheme, endpointURL.Path)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to create listener: %w", err)
	}

	cleanup := func() {
		if endpointURL.Scheme == "unix" {
			if err := os.Remove(endpointURL.Path); err != nil {
				klog.ErrorS(err, "Failed to remove old socket")
			}
		}
	}

	return listener, cleanup, nil
}

func grpcServer(
	identity cosi.IdentityServer,
	provisioner cosi.ProvisionerServer,
) (*grpc.Server, error) {
	if identity == nil || provisioner == nil {
		return nil, errors.New("identity and provisioner servers cannot be nil")
	}

	server := grpc.NewServer()

	cosi.RegisterIdentityServer(server, identity)
	cosi.RegisterProvisionerServer(server, provisioner)

	return server, nil
}

const (
	gracePeriod = 5 * time.Second
)

func shutdown(
	ctx context.Context,
	wg *sync.WaitGroup,
	g *grpc.Server,
) {
	<-ctx.Done()
	defer wg.Done()
	defer klog.InfoS("Stopped")

	klog.InfoS("Shutting down")

	dctx, stop := context.WithTimeout(context.Background(), gracePeriod)
	defer stop()

	c := make(chan struct{}, 1)

	if g != nil {
		go func() {
			g.GracefulStop()
			c <- struct{}{}
		}()

		for {
			select {
			case <-dctx.Done():
				klog.InfoS("Forcing shutdown")
				g.Stop()
				return
			case <-c:
				return
			}
		}
	}
}
