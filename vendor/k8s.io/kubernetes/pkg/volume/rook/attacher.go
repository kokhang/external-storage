/*
Copyright 2017 The Kubernetes Authors.

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

package rook

import (
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	"github.com/golang/glog"
	"github.com/pborman/uuid"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/kubernetes/pkg/util/exec"
	"k8s.io/kubernetes/pkg/util/mount"
	"k8s.io/kubernetes/pkg/volume"
	volumeutil "k8s.io/kubernetes/pkg/volume/util"
)

type rookAttacher struct {
	host            volume.VolumeHost
	volumeAttachTPR string
}

const (
	volumeAttachConfigMap = "rook-volume-attach"
	devicePathKey         = "devicePath"
	mountOptionsKey       = "mountOptions"
	checkSleepDuration    = time.Second
)

var _ volume.Attacher = &rookAttacher{}

// Attach maps a rook volume to the host and returns the attachment ID.
func (attacher *rookAttacher) Attach(spec *volume.Spec, nodeName types.NodeName) (string, error) {
	volumeSource, _, err := getVolumeSource(spec)
	if err != nil {
		return "", err
	}

	// Create a VolumeAttach TPR instance
	tprName := fmt.Sprintf("attach-%s", string(uuid.NewUUID()))
	volumeAttach := &VolumeAttach{
		ObjectMeta: metav1.ObjectMeta{
			Name: tprName,
		},
		Spec: VolumeAttachSpec{
			VolumeID:       volumeSource.VolumeID,
			VolumeGroup:    volumeSource.VolumeGroup,
			Node:           string(nodeName),
			VolumeMetadata: volumeSource.VolumeMetadata,
		},
		Status: VolumeAttachStatus{
			State:   VolumeAttachStatePending,
			Message: "Created Volume Attach TPR. Not processed yet",
		},
	}
	var result VolumeAttach
	err = attacher.host.GetKubeClient().Core().RESTClient().Post().
		Namespace(volumeSource.Cluster).
		Resource(attachmentPluralResources).
		Body(volumeAttach).
		Do().Into(&result)
	if err != nil {
		glog.Errorf("Rook: Failed to create VolumeAttach TPR: %+v", err)
		return "", err
	}
	return tprName, nil
}

func (attacher *rookAttacher) VolumesAreAttached(specs []*volume.Spec, nodeName types.NodeName) (map[*volume.Spec]bool, error) {
	// Fetch a list of VolumeAttach TPRs
	volumeAttachTPRs := VolumeAttachList{}
	err := attacher.host.GetKubeClient().Core().RESTClient().Get().
		Resource(attachmentPluralResources).
		Do().
		Into(&volumeAttachTPRs)
	if err != nil {
		glog.Errorf("Rook: Failed to create VolumeAttach TPR: %+v", err)
		return nil, err
	}

	volumesAttached := make(map[string]bool)
	for _, volumeAttach := range volumeAttachTPRs.VolumeAttachs {
		if volumeAttach.Spec.Node == string(nodeName) && volumeAttach.Status.State == VolumeAttachStateAttached {
			volumesAttached[fmt.Sprintf("%s-%s-%s", volumeAttach.ClusterName, volumeAttach.Spec.VolumeGroup, volumeAttach.Spec.VolumeID)] = true
		}
	}

	volumesAttachedCheck := make(map[*volume.Spec]bool)
	for _, spec := range specs {
		volumeSource, _, err := getVolumeSource(spec)
		if err != nil {
			glog.Errorf("Error getting volume (%q) source : %v", spec.Name(), err)
			continue
		}
		key := fmt.Sprintf("%s-%s-%s", volumeSource.Cluster, volumeSource.VolumeGroup, volumeSource.VolumeID)
		_, ok := volumesAttached[key]
		volumesAttachedCheck[spec] = ok
	}
	return volumesAttachedCheck, nil
}

func (attacher *rookAttacher) WaitForAttach(spec *volume.Spec, tprName string, timeout time.Duration) (string, error) {
	ticker := time.NewTicker(checkSleepDuration)
	defer ticker.Stop()
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	volumeSource, _, err := getVolumeSource(spec)
	if err != nil {
		return "", err
	}

	// Saving TPR name for generating the deviceMounthPath
	attacher.volumeAttachTPR = tprName

	for {
		select {
		case <-ticker.C:
			glog.V(5).Infof("Rook: Fetching TPR %s.", tprName)
			volumeAttachTPR := VolumeAttach{}
			err = attacher.host.GetKubeClient().Core().RESTClient().Get().
				Namespace(volumeSource.Cluster).
				Resource(attachmentPluralResources).
				Name(tprName).
				Do().
				Into(&volumeAttachTPR)
			if volumeAttachTPR.Status.State != VolumeAttachStatePending {
				if volumeAttachTPR.Status.State == VolumeAttachStateAttached {
					configmaps, err := attacher.host.GetKubeClient().Core().ConfigMaps(volumeSource.Cluster).Get(volumeAttachConfigMap, metav1.GetOptions{})
					if err != nil {
						return "", fmt.Errorf("Rook: Unable to get configmap %s: %v", volumeAttachConfigMap, err)
					}
					return configmaps.Data[fmt.Sprintf("%s.%s", tprName, devicePathKey)], nil
				} else {
					return "", fmt.Errorf("Rook: Volume Attached TPR %s failed: %s", tprName, volumeAttachTPR.Status.Message)
				}
			}
		case <-timer.C:
			return "", fmt.Errorf("Rook: Could not attach volume %s/%s TPR %s. Timeout waiting for volume attach", volumeSource.VolumeGroup, volumeSource.VolumeID, tprName)
		}
	}
}

func (attacher *rookAttacher) GetDeviceMountPath(spec *volume.Spec) (string, error) {
	volumeSource, _, err := getVolumeSource(spec)
	if err != nil {
		return "", err
	}
	return path.Join(attacher.host.GetPluginDir(rookPluginName), mount.MountsInGlobalPDPath, volumeSource.Cluster, attacher.volumeAttachTPR), nil
}

func (attacher *rookAttacher) MountDevice(spec *volume.Spec, devicePath string, deviceMountPath string) error {
	mounter := attacher.host.GetMounter()
	notMnt, err := mounter.IsLikelyNotMountPoint(deviceMountPath)
	if err != nil {
		if os.IsNotExist(err) {
			if err := os.MkdirAll(deviceMountPath, 0750); err != nil {
				return err
			}
			notMnt = true
		} else {
			return err
		}
	}

	volumeSource, readOnly, err := getVolumeSource(spec)
	if err != nil {
		return err
	}

	options := []string{}
	if readOnly {
		options = append(options, "ro")
	}
	if notMnt {
		diskMounter := &mount.SafeFormatAndMount{Interface: mounter, Runner: exec.New()}
		mountOptions := volume.MountOptionFromSpec(spec, options...)
		mountOptions = volume.JoinMountOptions(mountOptions, options)

		// Get extra mounting option from the rook configmap (if any)
		configmaps, err := attacher.host.GetKubeClient().Core().ConfigMaps(volumeSource.Cluster).Get(volumeAttachConfigMap, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("Rook: Unable to get configmap %s: %v", volumeAttachConfigMap, err)
		}
		extraOptions := configmaps.Data[fmt.Sprintf("%s.%s", attacher.volumeAttachTPR, mountOptionsKey)]
		mountOptions = volume.JoinMountOptions(strings.Split(",", extraOptions), mountOptions)

		err = diskMounter.FormatAndMount(devicePath, deviceMountPath, volumeSource.FSType, mountOptions)
		if err != nil {
			os.Remove(deviceMountPath)
			return err
		}
		glog.V(4).Infof("Rook: Formatting spec %v devicePath %v deviceMountPath %v fs %v with options %+v", spec.Name(), devicePath, deviceMountPath, volumeSource.FSType, options)
	}
	return nil
}

type rookDetacher struct {
	host volume.VolumeHost
}

var _ volume.Detacher = &rookDetacher{}

// Detach unmaps a rook image from the host
func (detacher *rookDetacher) Detach(deviceMountPath string, nodeName types.NodeName) error {
	dir, tprName := path.Split(deviceMountPath)
	namespace := path.Base(dir)
	return detacher.host.GetKubeClient().Core().RESTClient().Delete().
		Namespace(namespace).
		Resource(attachmentPluralResources).
		Name(tprName).
		Do().
		Error()
}

func (detacher *rookDetacher) UnmountDevice(deviceMountPath string) error {
	return volumeutil.UnmountPath(deviceMountPath, detacher.host.GetMounter())
}
