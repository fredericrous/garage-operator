/*
Copyright 2026 Raj Singh.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	garagev1alpha1 "github.com/rajsinghtech/garage-operator/api/v1alpha1"
	"github.com/rajsinghtech/garage-operator/internal/garage"
)

// Common status phases
const (
	PhaseReady    = "Ready"
	PhasePending  = "Pending"
	PhaseError    = "Error"
	PhaseDeleting = "Deleting"
)

// Layout policy constants
const (
	LayoutPolicyManual = "Manual"
	LayoutPolicyAuto   = "Auto"
)

// Common secret keys
const (
	DefaultAdminTokenKey = "admin-token"
	// DefaultMetricsTokenKey is the key holding the Garage metrics bearer token
	DefaultMetricsTokenKey = "metrics-token"
	// DefaultRPCSecretKey is the key holding the Garage inter-node RPC secret
	DefaultRPCSecretKey = "rpc-secret"
)

// Recommended Kubernetes label keys applied to every resource the operator owns.
// See https://kubernetes.io/docs/concepts/overview/working-with-objects/common-labels/
const (
	// LabelAppName identifies the application the resource belongs to
	LabelAppName = "app.kubernetes.io/name"
	// LabelAppInstance identifies the individual instance of the application
	LabelAppInstance = "app.kubernetes.io/instance"
	// LabelAppComponent identifies the role the resource plays within the application
	LabelAppComponent = "app.kubernetes.io/component"
	// LabelAppManagedBy identifies the tool managing the resource
	LabelAppManagedBy = "app.kubernetes.io/managed-by"
	// ManagedByGarageOperator is the LabelAppManagedBy value this operator stamps
	ManagedByGarageOperator = "garage-operator"
)

// Operator-owned label keys
const (
	// LabelCluster names the GarageCluster a resource belongs to
	LabelCluster = "garage.rajsingh.info/cluster"
)

// Garage application naming
const (
	// AppNameGarage is the application/container name used for Garage workloads
	AppNameGarage = "garage"
	// DefaultS3Region is the S3 region advertised when the cluster does not set one
	DefaultS3Region = "garage"
)

// StatefulSet volume names and container mount paths
const (
	// VolumeNameConfig holds the rendered garage.toml
	VolumeNameConfig = "config"
	// VolumeNameMetadata holds the Garage metadata directory
	VolumeNameMetadata = "metadata"
	// VolumeNameData holds the Garage block data directory
	VolumeNameData = "data"
	// MountPathData is where the data volume is mounted inside the Garage container
	MountPathData = "/data/data"
)

// Default Garage ports
const (
	DefaultS3Port    = int32(3900)
	DefaultRPCPort   = int32(3901)
	DefaultWebPort   = int32(3902)
	DefaultAdminPort = int32(3903)
	DefaultK2VPort   = int32(3904)
)

// Reconciliation timing constants
const (
	// RequeueAfterImmediate is a prompt retry used after the operator has just
	// mutated the object itself (e.g. adding a finalizer) and wants to reconcile
	// again from the updated state. It replaces the deprecated ctrl.Result{Requeue: true}.
	RequeueAfterImmediate = 1 * time.Second
	// RequeueAfterError is the delay before requeuing after an error
	RequeueAfterError = 30 * time.Second
	// RequeueAfterUnhealthy is a fast delay for reconnecting unhealthy clusters
	RequeueAfterUnhealthy = 10 * time.Second
	// RequeueAfterShort is a short delay for periodic reconciliation
	RequeueAfterShort = 1 * time.Minute
	// RequeueAfterLong is a longer delay for stable resources
	RequeueAfterLong = 5 * time.Minute
	// StatusUpdateMaxRetries is the maximum number of retries for status updates
	StatusUpdateMaxRetries = 3
)

// Finalization constants
const (
	// FinalizationMaxRetries is tracked via annotation on the resource
	FinalizationRetryAnnotation = "garage.rajsingh.info/finalization-retries"
	// FinalizationMaxRetries before giving up and removing finalizer
	FinalizationMaxRetries = 5
)

// GetGarageClient creates a Garage Admin API client for the given cluster.
// This is a shared helper used by all controllers that need to interact with Garage.
// GetAdminToken retrieves the admin token from the cluster's secret.
func GetAdminToken(ctx context.Context, c client.Client, cluster *garagev1alpha1.GarageCluster) (string, error) {
	if cluster.Spec.Admin == nil || cluster.Spec.Admin.AdminTokenSecretRef == nil {
		return "", fmt.Errorf("admin token not configured on cluster")
	}

	secret := &corev1.Secret{}
	if err := c.Get(ctx, types.NamespacedName{
		Name:      cluster.Spec.Admin.AdminTokenSecretRef.Name,
		Namespace: cluster.Namespace,
	}, secret); err != nil {
		return "", fmt.Errorf("failed to get admin token secret: %w", err)
	}

	if secret.Data == nil {
		return "", fmt.Errorf("admin token secret %s has no data", secret.Name)
	}

	tokenKey := DefaultAdminTokenKey
	if cluster.Spec.Admin.AdminTokenSecretRef.Key != "" {
		tokenKey = cluster.Spec.Admin.AdminTokenSecretRef.Key
	}

	tokenData, ok := secret.Data[tokenKey]
	if !ok {
		return "", fmt.Errorf("admin token key %q not found in secret %s", tokenKey, secret.Name)
	}
	adminToken := string(tokenData)
	if adminToken == "" {
		return "", fmt.Errorf("admin token is empty in secret %s", secret.Name)
	}

	return adminToken, nil
}

func GetGarageClient(ctx context.Context, c client.Client, cluster *garagev1alpha1.GarageCluster) (*garage.Client, error) {
	adminPort := DefaultAdminPort
	if cluster.Spec.Admin != nil && cluster.Spec.Admin.BindPort != 0 {
		adminPort = cluster.Spec.Admin.BindPort
	}
	adminEndpoint := fmt.Sprintf("http://%s.%s.svc.cluster.local:%d", cluster.Name, cluster.Namespace, adminPort)

	adminToken, err := GetAdminToken(ctx, c, cluster)
	if err != nil {
		return nil, err
	}

	return garage.NewClient(adminEndpoint, adminToken), nil
}

// UpdateStatusWithRetry updates the status subresource with retry on conflict.
// This handles the race condition where concurrent reconciliations may conflict.
func UpdateStatusWithRetry(ctx context.Context, c client.Client, obj client.Object) error {
	for i := 0; i < StatusUpdateMaxRetries; i++ {
		err := c.Status().Update(ctx, obj)
		if err == nil {
			return nil
		}
		if !errors.IsConflict(err) {
			return err
		}
		// On conflict, re-fetch the object and retry
		if i < StatusUpdateMaxRetries-1 {
			if err := c.Get(ctx, client.ObjectKeyFromObject(obj), obj); err != nil {
				return fmt.Errorf("failed to re-fetch object after conflict: %w", err)
			}
		}
	}
	return fmt.Errorf("failed to update status after %d retries due to conflicts", StatusUpdateMaxRetries)
}

// GetFinalizationRetryCount returns the current finalization retry count from annotations
func GetFinalizationRetryCount(obj client.Object) int {
	annotations := obj.GetAnnotations()
	if annotations == nil {
		return 0
	}
	countStr, ok := annotations[FinalizationRetryAnnotation]
	if !ok {
		return 0
	}
	count, err := strconv.Atoi(countStr)
	if err != nil {
		return 0
	}
	return count
}

// IncrementFinalizationRetryCount increments the finalization retry count annotation
func IncrementFinalizationRetryCount(obj client.Object) {
	annotations := obj.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string)
	}
	count := GetFinalizationRetryCount(obj)
	annotations[FinalizationRetryAnnotation] = strconv.Itoa(count + 1)
	obj.SetAnnotations(annotations)
}

// ShouldSkipFinalization returns true if finalization has failed too many times
func ShouldSkipFinalization(obj client.Object) bool {
	return GetFinalizationRetryCount(obj) >= FinalizationMaxRetries
}
