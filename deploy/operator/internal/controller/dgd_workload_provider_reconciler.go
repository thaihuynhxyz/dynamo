/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package controller

import (
	"context"
	"fmt"
	"strings"

	nvidiacomv1beta1 "github.com/ai-dynamo/dynamo/deploy/operator/api/v1beta1"
	"github.com/ai-dynamo/dynamo/deploy/operator/internal/consts"
	"github.com/ai-dynamo/dynamo/deploy/operator/internal/features"
	grovev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type workloadProvider string

const (
	workloadProviderComponent workloadProvider = consts.WorkloadProviderComponent
	workloadProviderGrove     workloadProvider = consts.WorkloadProviderGrove
)

type workloadProviderSelection struct {
	Provider       workloadProvider
	NewlyPersisted bool
}

// dgdWorkloadProviderReconciler resolves and persists one durable graph-level
// workload provider before a workload program is allowed to mutate resources.
type dgdWorkloadProviderReconciler struct {
	client       client.Client
	groveEnabled bool
}

func newDGDWorkloadProviderReconciler(
	kubeClient client.Client,
	gate features.Gate,
) *dgdWorkloadProviderReconciler {
	return &dgdWorkloadProviderReconciler{
		client:       kubeClient,
		groveEnabled: gate.Enabled(features.Grove),
	}
}

func (r *dgdWorkloadProviderReconciler) Reconcile(
	ctx context.Context,
	dgd *nvidiacomv1beta1.DynamoGraphDeployment,
) (workloadProviderSelection, error) {
	// Reuse a previously selected provider without consulting mutable intent or
	// current cluster capabilities.
	if value, exists := dgd.Annotations[consts.KubeAnnotationSelectedWorkloadProvider]; exists {
		provider, err := parseWorkloadProvider(value)
		if err != nil {
			return workloadProviderSelection{}, err
		}
		return workloadProviderSelection{Provider: provider}, nil
	}

	// Adopt the provider represented by existing owned workload resources so an
	// upgrade does not reinterpret a legacy DGD under current feature settings.
	provider, found, err := r.providerFromOwnedWorkloads(ctx, dgd)
	if err != nil {
		return workloadProviderSelection{}, err
	}
	if !found {
		provider = providerFromCurrentIntent(r.groveEnabled, dgd)
	}

	// Persist the decision as its own retry boundary before invoking a workload
	// program. Optimistic locking makes concurrent intent changes retry from the
	// latest DGD, while Patch preserves the successful write's resource version.
	base := dgd.DeepCopy()
	if dgd.Annotations == nil {
		dgd.Annotations = make(map[string]string)
	}
	dgd.Annotations[consts.KubeAnnotationSelectedWorkloadProvider] = string(provider)
	patch := client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})
	if err := r.client.Patch(ctx, dgd, patch); err != nil {
		return workloadProviderSelection{}, fmt.Errorf("persist selected workload provider %q: %w", provider, err)
	}

	return workloadProviderSelection{Provider: provider, NewlyPersisted: true}, nil
}

func (r *dgdWorkloadProviderReconciler) providerFromOwnedWorkloads(
	ctx context.Context,
	dgd *nvidiacomv1beta1.DynamoGraphDeployment,
) (workloadProvider, bool, error) {
	// Observe both workload families independently so mixed legacy state is not
	// silently attributed to whichever family happens to be listed first.
	hasComponents, err := r.hasOwnedComponentWorkloads(ctx, dgd)
	if err != nil {
		return "", false, err
	}
	hasGrove, err := r.hasOwnedGroveWorkloads(ctx, dgd)
	if err != nil {
		return "", false, err
	}

	switch {
	case hasComponents && hasGrove:
		return "", false, fmt.Errorf(
			"cannot select a workload provider for DynamoGraphDeployment %s/%s: owned DynamoComponentDeployments and Grove PodCliqueSets both exist",
			dgd.Namespace,
			dgd.Name,
		)
	case hasGrove:
		return workloadProviderGrove, true, nil
	case hasComponents:
		return workloadProviderComponent, true, nil
	default:
		return "", false, nil
	}
}

func (r *dgdWorkloadProviderReconciler) hasOwnedComponentWorkloads(
	ctx context.Context,
	dgd *nvidiacomv1beta1.DynamoGraphDeployment,
) (bool, error) {
	// The DGD label narrows the cached list; the controller reference is the
	// authoritative ownership check.
	list := &nvidiacomv1beta1.DynamoComponentDeploymentList{}
	if err := r.client.List(
		ctx,
		list,
		client.InNamespace(dgd.Namespace),
		client.MatchingLabels{consts.KubeLabelDynamoGraphDeploymentName: dgd.Name},
	); err != nil {
		return false, fmt.Errorf("list DynamoComponentDeployments owned by DynamoGraphDeployment %s/%s: %w", dgd.Namespace, dgd.Name, err)
	}
	for i := range list.Items {
		if metav1.IsControlledBy(&list.Items[i], dgd) {
			return true, nil
		}
	}
	return false, nil
}

func (r *dgdWorkloadProviderReconciler) hasOwnedGroveWorkloads(
	ctx context.Context,
	dgd *nvidiacomv1beta1.DynamoGraphDeployment,
) (bool, error) {
	// A missing Grove API means no Grove workload can currently be observed. It
	// must not prevent a component-only cluster from recording its selection.
	list := &grovev1alpha1.PodCliqueSetList{}
	if err := r.client.List(
		ctx,
		list,
		client.InNamespace(dgd.Namespace),
		client.MatchingLabels{consts.KubeLabelDynamoGraphDeploymentName: dgd.Name},
	); err != nil {
		if meta.IsNoMatchError(err) {
			return false, nil
		}
		return false, fmt.Errorf("list Grove PodCliqueSets owned by DynamoGraphDeployment %s/%s: %w", dgd.Namespace, dgd.Name, err)
	}
	for i := range list.Items {
		if metav1.IsControlledBy(&list.Items[i], dgd) {
			return true, nil
		}
	}
	return false, nil
}

func providerFromCurrentIntent(
	groveEnabled bool,
	dgd *nvidiacomv1beta1.DynamoGraphDeployment,
) workloadProvider {
	if groveEnabled && strings.ToLower(dgd.Annotations[consts.KubeAnnotationEnableGrove]) != consts.KubeLabelValueFalse {
		return workloadProviderGrove
	}
	return workloadProviderComponent
}

func parseWorkloadProvider(value string) (workloadProvider, error) {
	switch workloadProvider(value) {
	case workloadProviderComponent:
		return workloadProviderComponent, nil
	case workloadProviderGrove:
		return workloadProviderGrove, nil
	default:
		return "", fmt.Errorf(
			"DynamoGraphDeployment annotation %q has unsupported workload provider %q",
			consts.KubeAnnotationSelectedWorkloadProvider,
			value,
		)
	}
}
