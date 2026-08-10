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
	"fmt"
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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
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
		workloads          []workloadProviderTestWorkload
		groveListErr       error
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
			name: "cluster without Grove API selects component provider",
			groveListErr: fmt.Errorf("discover Grove API: %w", &meta.NoKindMatchError{
				GroupKind:        grovev1alpha1.SchemeGroupVersion.WithKind("PodCliqueSet").GroupKind(),
				SearchedVersions: []string{grovev1alpha1.SchemeGroupVersion.Version},
			}),
			wantProvider:       workloadProviderComponent,
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
			name:         "owned DCD adopts component provider despite current Grove default",
			groveEnabled: true,
			workloads: []workloadProviderTestWorkload{{
				name:     "test-dgd-worker",
				provider: workloadProviderComponent,
				owned:    true,
				labeled:  true,
			}},
			wantProvider:       workloadProviderComponent,
			wantNewlyPersisted: true,
		},
		{
			name:        "owned PodCliqueSet adopts Grove despite current opt out",
			annotations: map[string]string{consts.KubeAnnotationEnableGrove: consts.KubeLabelValueFalse},
			workloads: []workloadProviderTestWorkload{{
				name:     "test-dgd",
				provider: workloadProviderGrove,
				owned:    true,
				labeled:  true,
			}},
			wantProvider:       workloadProviderGrove,
			wantNewlyPersisted: true,
		},
		{
			name:         "owned DCD without graph label adopts component provider",
			groveEnabled: true,
			workloads: []workloadProviderTestWorkload{{
				name:     "test-dgd-worker",
				provider: workloadProviderComponent,
				owned:    true,
			}},
			wantProvider:       workloadProviderComponent,
			wantNewlyPersisted: true,
		},
		{
			name:        "owned PodCliqueSet without graph label adopts Grove",
			annotations: map[string]string{consts.KubeAnnotationEnableGrove: consts.KubeLabelValueFalse},
			workloads: []workloadProviderTestWorkload{{
				name:     "test-dgd",
				provider: workloadProviderGrove,
				owned:    true,
			}},
			wantProvider:       workloadProviderGrove,
			wantNewlyPersisted: true,
		},
		{
			name:         "mixed owned workloads fail closed regardless of labels",
			groveEnabled: true,
			workloads: []workloadProviderTestWorkload{
				{
					name:     "test-dgd-worker",
					provider: workloadProviderComponent,
					owned:    true,
				},
				{
					name:     "test-dgd",
					provider: workloadProviderGrove,
					owned:    true,
					labeled:  true,
				},
			},
			wantErr: "owned DynamoComponentDeployments and Grove PodCliqueSets both exist",
		},
		{
			name:         "foreign labeled and unlabeled workloads are ignored",
			groveEnabled: true,
			workloads: []workloadProviderTestWorkload{
				{
					name:     "foreign-dcd",
					provider: workloadProviderComponent,
					labeled:  true,
				},
				{
					name:     "foreign-pcs",
					provider: workloadProviderGrove,
				},
			},
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
			for _, workload := range tt.workloads {
				objects = append(objects, workloadProviderTestResource(dgd, workload))
			}
			clientBuilder := fake.NewClientBuilder().
				WithScheme(newDynamoGraphDeploymentControllerTestScheme(t)).
				WithObjects(objects...)
			if tt.groveListErr != nil {
				clientBuilder = clientBuilder.WithInterceptorFuncs(interceptor.Funcs{
					List: func(
						ctx context.Context,
						kubeClient client.WithWatch,
						list client.ObjectList,
						opts ...client.ListOption,
					) error {
						if _, ok := list.(*grovev1alpha1.PodCliqueSetList); ok {
							return tt.groveListErr
						}
						return kubeClient.List(ctx, list, opts...)
					},
				})
			}
			kubeClient := clientBuilder.Build()
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

	t.Log("Verify both the request and API object remain unselected so the operation can retry")
	assert.Nil(t, dgd.Annotations)
	var stored nvidiacomv1beta1.DynamoGraphDeployment
	require.NoError(t, kubeClient.Get(t.Context(), client.ObjectKeyFromObject(dgd), &stored))
	assert.NotContains(t, stored.Annotations, consts.KubeAnnotationSelectedWorkloadProvider)
}

func TestDynamoGraphDeploymentReconcileTreatsProviderConflictAsPending(t *testing.T) {
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
	conflict := apierrors.NewConflict(
		schema.GroupResource{Group: nvidiacomv1beta1.GroupVersion.Group, Resource: "dynamographdeployments"},
		dgd.Name,
		errors.New("stale resource version"),
	)
	kubeClient := fake.NewClientBuilder().
		WithScheme(newDynamoGraphDeploymentControllerTestScheme(t)).
		WithObjects(dgd).
		WithStatusSubresource(&nvidiacomv1beta1.DynamoGraphDeployment{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(
				context.Context,
				client.WithWatch,
				client.Object,
				client.Patch,
				...client.PatchOption,
			) error {
				return conflict
			},
		}).
		Build()
	reconciler := &DynamoGraphDeploymentReconciler{
		Client:        kubeClient,
		Config:        &configv1alpha1.OperatorConfiguration{},
		RuntimeConfig: &commoncontroller.RuntimeConfig{},
		Recorder:      record.NewFakeRecorder(10),
	}

	t.Log("Reconcile an unselected DGD whose provider patch conflicts with a newer API object")
	result, err := reconciler.Reconcile(t.Context(), ctrl.Request{
		NamespacedName: client.ObjectKeyFromObject(dgd),
	})
	require.NoError(t, err)
	assert.Equal(t, workloadProviderPersistenceRequeueDelay, result.RequeueAfter)

	t.Log("Verify the transient conflict did not publish a provider-selection failure")
	var stored nvidiacomv1beta1.DynamoGraphDeployment
	require.NoError(t, kubeClient.Get(t.Context(), client.ObjectKeyFromObject(dgd), &stored))
	assert.NotContains(t, stored.Annotations, consts.KubeAnnotationSelectedWorkloadProvider)
	assert.Empty(t, stored.Status)
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

type workloadProviderTestWorkload struct {
	name     string
	provider workloadProvider
	owned    bool
	labeled  bool
}

func workloadProviderTestResource(
	dgd *nvidiacomv1beta1.DynamoGraphDeployment,
	workload workloadProviderTestWorkload,
) client.Object {
	var labels map[string]string
	if workload.labeled {
		labels = map[string]string{consts.KubeLabelDynamoGraphDeploymentName: dgd.Name}
	}
	ownerReferences := []metav1.OwnerReference(nil)
	if workload.owned {
		ownerReferences = []metav1.OwnerReference{
			*metav1.NewControllerRef(dgd, nvidiacomv1beta1.DynamoGraphDeploymentGVK),
		}
	}

	switch workload.provider {
	case workloadProviderComponent:
		return &nvidiacomv1beta1.DynamoComponentDeployment{ObjectMeta: metav1.ObjectMeta{
			Name:            workload.name,
			Namespace:       dgd.Namespace,
			Labels:          labels,
			OwnerReferences: ownerReferences,
		}}
	case workloadProviderGrove:
		return &grovev1alpha1.PodCliqueSet{ObjectMeta: metav1.ObjectMeta{
			Name:            workload.name,
			Namespace:       dgd.Namespace,
			Labels:          labels,
			OwnerReferences: ownerReferences,
		}}
	default:
		panic("unsupported test workload provider " + string(workload.provider))
	}
}
