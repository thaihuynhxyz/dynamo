//go:build !clustertest

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
	"testing"

	nvidiacomv1alpha1 "github.com/ai-dynamo/dynamo/deploy/operator/api/v1alpha1"
	nvidiacomv1beta1 "github.com/ai-dynamo/dynamo/deploy/operator/api/v1beta1"
	"github.com/ai-dynamo/dynamo/deploy/operator/internal/consts"
	"github.com/ai-dynamo/dynamo/deploy/operator/internal/dynamo"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestActiveWorkerDCDHashMatchesDesiredHashAfterCompletedRestart(t *testing.T) {
	ctx := context.Background()
	env := sharedEnv.ForTest(t)
	kubeClient := env.Client()

	t.Log("create a normal DGD and record its completed restart")
	dgd := createTestDGD("restart-hash-parity", map[string]*nvidiacomv1alpha1.DynamoComponentDeploymentSharedSpec{
		"worker": {ComponentType: consts.ComponentTypeWorker},
	})
	dgd.Namespace = env.Namespace()
	dgd.Spec.BackendFramework = "vllm"
	dgd.Spec.Components[0].PodTemplate = &corev1.PodTemplateSpec{
		Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Name:  consts.MainContainerName,
			Image: "registry.example/dynamo:1.5.0",
		}}},
	}
	require.NoError(t, kubeClient.Create(ctx, dgd))
	dgd.Spec.Restart = &nvidiacomv1beta1.Restart{ID: "restart-1"}
	require.NoError(t, kubeClient.Update(ctx, dgd))
	dgd.Status.State = nvidiacomv1beta1.DGDStateSuccessful
	dgd.Status.Restart = &nvidiacomv1beta1.RestartStatus{
		ObservedID: "restart-1",
		Phase:      nvidiacomv1beta1.RestartPhaseCompleted,
	}
	require.NoError(t, kubeClient.Status().Update(ctx, dgd))
	require.NoError(t, kubeClient.Get(ctx, client.ObjectKeyFromObject(dgd), dgd))

	restartState := dynamo.DetermineRestartState(dgd, dgd.Status.Restart)
	require.NotNil(t, restartState)

	t.Log("persist worker DCDs through normal component reconciliation")
	recorder := record.NewFakeRecorder(10)
	rollout := newDGDWorkerRolloutReconciler(kubeClient, recorder)
	workloads := newComponentWorkloadsReconciler(kubeClient, recorder, rollout)
	_, err := workloads.Reconcile(ctx, dgd, restartState, nil)
	require.NoError(t, err)

	t.Log("list persisted worker DCDs after the completed restart")
	dcdList := &nvidiacomv1beta1.DynamoComponentDeploymentList{}
	require.NoError(t, kubeClient.List(
		ctx,
		dcdList,
		client.InNamespace(env.Namespace()),
		client.MatchingLabels{consts.KubeLabelDynamoGraphDeploymentName: dgd.Name},
	))
	workerDCDs := make([]*nvidiacomv1beta1.DynamoComponentDeployment, 0, len(dcdList.Items))
	for i := range dcdList.Items {
		dcd := &dcdList.Items[i]
		if !dynamo.IsWorkerComponent(string(dcd.Spec.ComponentType)) {
			continue
		}
		require.NotNil(t, dcd.Spec.PodTemplate)
		require.Equal(t, "restart-1", dcd.Spec.PodTemplate.Annotations[consts.RestartAnnotation])
		workerDCDs = append(workerDCDs, dcd)
	}
	require.NotEmpty(t, workerDCDs)

	t.Log("compare the desired hash with the active DCD hash")
	desired, err := dynamo.ComputeDGDWorkersSpecHash(dgd)
	require.NoError(t, err)
	active, err := dynamo.ComputeDCDWorkersSpecHash(workerDCDs)
	require.NoError(t, err)
	require.Equal(t, desired, active)
}
