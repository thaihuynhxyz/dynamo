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
	"errors"
	"maps"
	"testing"

	configv1alpha1 "github.com/ai-dynamo/dynamo/deploy/operator/api/config/v1alpha1"
	nvidiacomv1beta1 "github.com/ai-dynamo/dynamo/deploy/operator/api/v1beta1"
	"github.com/ai-dynamo/dynamo/deploy/operator/internal/consts"
	commoncontroller "github.com/ai-dynamo/dynamo/deploy/operator/internal/controller_common"
	"github.com/ai-dynamo/dynamo/deploy/operator/internal/features"
	grovev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestDGDWorkloadProviderReconcilerReconcile(t *testing.T) {
	tests := []struct {
		name               string
		groveEnabled       bool
		annotations        map[string]string
		ownedProviders     []workloadProvider
		foreignDCD         bool
		wantProvider       workloadProvider
		wantNewlyPersisted bool
		wantErr            string
	}{
		{
			name: "existing selection remains authoritative when Grove becomes unavailable",
			annotations: map[string]string{
				consts.KubeAnnotationSelectedWorkloadProvider: consts.WorkloadProviderGrove,
				consts.KubeAnnotationEnableGrove:              consts.KubeLabelValueFalse,
			},
			wantProvider: workloadProviderGrove,
		},
		{
			name:               "new DGD selects Grove from current intent",
			groveEnabled:       true,
			wantProvider:       workloadProviderGrove,
			wantNewlyPersisted: true,
		},
		{
			name:         "explicit Grove opt out selects component provider",
			groveEnabled: true,
			annotations: map[string]string{
				consts.KubeAnnotationEnableGrove: consts.KubeLabelValueFalse,
			},
			wantProvider:       workloadProviderComponent,
			wantNewlyPersisted: true,
		},
		{
			name:               "owned DCD adopts component provider despite current Grove default",
			groveEnabled:       true,
			ownedProviders:     []workloadProvider{workloadProviderComponent},
			wantProvider:       workloadProviderComponent,
			wantNewlyPersisted: true,
		},
		{
			name:               "owned PodCliqueSet adopts Grove despite current opt out",
			annotations:        map[string]string{consts.KubeAnnotationEnableGrove: consts.KubeLabelValueFalse},
			ownedProviders:     []workloadProvider{workloadProviderGrove},
			wantProvider:       workloadProviderGrove,
			wantNewlyPersisted: true,
		},
		{
			name:           "mixed owned workload families fail closed",
			groveEnabled:   true,
			ownedProviders: []workloadProvider{workloadProviderComponent, workloadProviderGrove},
			wantErr:        "owned DynamoComponentDeployments and Grove PodCliqueSets both exist",
		},
		{
			name:               "foreign DCD with matching label is ignored",
			groveEnabled:       true,
			foreignDCD:         true,
			wantProvider:       workloadProviderGrove,
			wantNewlyPersisted: true,
		},
		{
			name: "unsupported persisted value fails closed",
			annotations: map[string]string{
				consts.KubeAnnotationSelectedWorkloadProvider: "unknown",
			},
			wantErr: "unsupported workload provider",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Log("Build one DGD and any workload resources observed during provider adoption")
			dgd := &nvidiacomv1beta1.DynamoGraphDeployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "test-dgd",
					Namespace:   "default",
					UID:         types.UID("dgd-uid"),
					Annotations: maps.Clone(tt.annotations),
				},
			}
			objects := []client.Object{dgd}
			for _, provider := range tt.ownedProviders {
				objects = append(objects, workloadProviderTestResource(dgd, provider, true))
			}
			if tt.foreignDCD {
				objects = append(objects, workloadProviderTestResource(dgd, workloadProviderComponent, false))
			}
			kubeClient := fake.NewClientBuilder().
				WithScheme(newDynamoGraphDeploymentControllerTestScheme(t)).
				WithObjects(objects...).
				Build()
			var requestDGD nvidiacomv1beta1.DynamoGraphDeployment
			require.NoError(t, kubeClient.Get(t.Context(), client.ObjectKeyFromObject(dgd), &requestDGD))
			dgd = &requestDGD
			reconciler := newDGDWorkloadProviderReconciler(
				kubeClient,
				features.Gates{Grove: tt.groveEnabled},
			)

			t.Log("Reconcile one durable provider selection")
			selection, err := reconciler.Reconcile(t.Context(), dgd)

			t.Log("Verify selection, persistence, and fail-closed behavior")
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				var stored nvidiacomv1beta1.DynamoGraphDeployment
				require.NoError(t, kubeClient.Get(t.Context(), client.ObjectKeyFromObject(dgd), &stored))
				if tt.annotations[consts.KubeAnnotationSelectedWorkloadProvider] == "" {
					assert.NotContains(t, stored.Annotations, consts.KubeAnnotationSelectedWorkloadProvider)
				}
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantProvider, selection.Provider)
			assert.Equal(t, tt.wantNewlyPersisted, selection.NewlyPersisted)

			var stored nvidiacomv1beta1.DynamoGraphDeployment
			require.NoError(t, kubeClient.Get(t.Context(), client.ObjectKeyFromObject(dgd), &stored))
			assert.Equal(t, string(tt.wantProvider), stored.Annotations[consts.KubeAnnotationSelectedWorkloadProvider])
		})
	}
}

func TestDGDWorkloadProviderReconcilerDoesNotPersistFailedPatch(t *testing.T) {
	patchErr := errors.New("patch failed")
	dgd := &nvidiacomv1beta1.DynamoGraphDeployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-dgd",
			Namespace: "default",
			UID:       types.UID("dgd-uid"),
		},
	}
	kubeClient := fake.NewClientBuilder().
		WithScheme(newDynamoGraphDeploymentControllerTestScheme(t)).
		WithObjects(dgd).
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(
				context.Context,
				client.WithWatch,
				client.Object,
				client.Patch,
				...client.PatchOption,
			) error {
				return patchErr
			},
		}).
		Build()
	var requestDGD nvidiacomv1beta1.DynamoGraphDeployment
	require.NoError(t, kubeClient.Get(t.Context(), client.ObjectKeyFromObject(dgd), &requestDGD))
	dgd = &requestDGD
	reconciler := newDGDWorkloadProviderReconciler(kubeClient, features.Gates{})

	t.Log("Attempt to persist the initial provider through a failing client")
	_, err := reconciler.Reconcile(t.Context(), dgd)
	require.ErrorIs(t, err, patchErr)

	t.Log("Verify the API object remains unselected so the operation can retry")
	var stored nvidiacomv1beta1.DynamoGraphDeployment
	require.NoError(t, kubeClient.Get(t.Context(), client.ObjectKeyFromObject(dgd), &stored))
	assert.NotContains(t, stored.Annotations, consts.KubeAnnotationSelectedWorkloadProvider)
}

func TestDynamoGraphDeploymentReconcilePersistsProviderBeforeWorkloads(t *testing.T) {
	dgd := &nvidiacomv1beta1.DynamoGraphDeployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-dgd",
			Namespace: "default",
			UID:       types.UID("dgd-uid"),
		},
		Spec: nvidiacomv1beta1.DynamoGraphDeploymentSpec{
			Components: []nvidiacomv1beta1.DynamoComponentDeploymentSharedSpec{{
				ComponentName: "worker",
			}},
		},
	}
	kubeClient := fake.NewClientBuilder().
		WithScheme(newDynamoGraphDeploymentControllerTestScheme(t)).
		WithObjects(dgd).
		WithStatusSubresource(&nvidiacomv1beta1.DynamoGraphDeployment{}).
		Build()
	reconciler := &DynamoGraphDeploymentReconciler{
		Client:        kubeClient,
		Config:        &configv1alpha1.OperatorConfiguration{},
		RuntimeConfig: &commoncontroller.RuntimeConfig{},
		Recorder:      record.NewFakeRecorder(10),
	}

	t.Log("Run the first outer reconciliation for an unselected DGD")
	result, err := reconciler.Reconcile(t.Context(), ctrl.Request{
		NamespacedName: client.ObjectKeyFromObject(dgd),
	})
	require.NoError(t, err)
	assert.Equal(t, workloadProviderPersistenceRequeueDelay, result.RequeueAfter)

	t.Log("Verify only controller metadata changed at the provider boundary")
	var stored nvidiacomv1beta1.DynamoGraphDeployment
	require.NoError(t, kubeClient.Get(t.Context(), client.ObjectKeyFromObject(dgd), &stored))
	assert.True(t, commoncontroller.ContainsFinalizer(&stored))
	assert.Equal(t, consts.WorkloadProviderComponent, stored.Annotations[consts.KubeAnnotationSelectedWorkloadProvider])
	assert.Empty(t, stored.Status)

	var dcds nvidiacomv1beta1.DynamoComponentDeploymentList
	require.NoError(t, kubeClient.List(t.Context(), &dcds, client.InNamespace(dgd.Namespace)))
	assert.Empty(t, dcds.Items)
	var podCliqueSets grovev1alpha1.PodCliqueSetList
	require.NoError(t, kubeClient.List(t.Context(), &podCliqueSets, client.InNamespace(dgd.Namespace)))
	assert.Empty(t, podCliqueSets.Items)
}

func workloadProviderTestResource(
	dgd *nvidiacomv1beta1.DynamoGraphDeployment,
	provider workloadProvider,
	owned bool,
) client.Object {
	labels := map[string]string{consts.KubeLabelDynamoGraphDeploymentName: dgd.Name}
	ownerReferences := []metav1.OwnerReference(nil)
	if owned {
		ownerReferences = []metav1.OwnerReference{
			*metav1.NewControllerRef(dgd, nvidiacomv1beta1.DynamoGraphDeploymentGVK),
		}
	}

	switch provider {
	case workloadProviderComponent:
		return &nvidiacomv1beta1.DynamoComponentDeployment{ObjectMeta: metav1.ObjectMeta{
			Name:            "test-dgd-worker",
			Namespace:       dgd.Namespace,
			Labels:          labels,
			OwnerReferences: ownerReferences,
		}}
	case workloadProviderGrove:
		return &grovev1alpha1.PodCliqueSet{ObjectMeta: metav1.ObjectMeta{
			Name:            "test-dgd",
			Namespace:       dgd.Namespace,
			Labels:          labels,
			OwnerReferences: ownerReferences,
		}}
	default:
		panic("unsupported test workload provider " + string(provider))
	}
}
