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

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/kubernetes/pkg/api/v1"
	"k8s.io/kubernetes/pkg/util/mount"
	"k8s.io/kubernetes/pkg/volume"
)

// This is the primary entrypoint for volume plugins.
func ProbeVolumePlugins() []volume.VolumePlugin {
	return []volume.VolumePlugin{&rookPlugin{nil}}
}

type rookPlugin struct {
	host volume.VolumeHost
}

var _ volume.PersistentVolumePlugin = &rookPlugin{}
var _ volume.AttachableVolumePlugin = &rookPlugin{}

const (
	rookPluginName = "kubernetes.io/rook"
)

func (plugin *rookPlugin) NewAttacher() (volume.Attacher, error) {
	return &rookAttacher{
		host: plugin.host,
	}, nil
}

func (plugin *rookPlugin) NewDetacher() (volume.Detacher, error) {
	return &rookDetacher{
		host: plugin.host,
	}, nil
}

func (plugin *rookPlugin) GetDeviceMountRefs(deviceMountPath string) ([]string, error) {
	mounter := plugin.host.GetMounter()
	return mount.GetMountRefs(mounter, deviceMountPath)
}

func (plugin *rookPlugin) Init(host volume.VolumeHost) error {
	plugin.host = host
	return nil
}

func (plugin *rookPlugin) GetPluginName() string {
	return rookPluginName
}

func (plugin *rookPlugin) GetVolumeName(spec *volume.Spec) (string, error) {
	volumeSource, _, err := getVolumeSource(spec)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf(
		"%v:%v",
		volumeSource.VolumeGroup,
		volumeSource.VolumeID), nil
}

func (plugin *rookPlugin) CanSupport(spec *volume.Spec) bool {
	if (spec.Volume != nil && spec.Volume.Rook == nil) || (spec.PersistentVolume != nil && spec.PersistentVolume.Spec.Rook == nil) {
		return false
	}
	return true
}

func (plugin *rookPlugin) NewMounter(spec *volume.Spec, pod *v1.Pod, _ volume.VolumeOptions) (volume.Mounter, error) {
	return nil, nil
}

func (plugin *rookPlugin) NewUnmounter(volName string, podUID types.UID) (volume.Unmounter, error) {
	return nil, nil
}

func (plugin *rookPlugin) ConstructVolumeSpec(volumeName, mountPath string) (*volume.Spec, error) {
	return nil, nil
}

func (plugin *rookPlugin) RequiresRemount() bool {
	return false
}

func (plugin *rookPlugin) SupportsMountOption() bool {
	return true
}

func (plugin *rookPlugin) SupportsBulkVolumeVerification() bool {
	return false
}

func (plugin *rookPlugin) GetAccessModes() []v1.PersistentVolumeAccessMode {
	return []v1.PersistentVolumeAccessMode{
		v1.ReadWriteOnce,
		v1.ReadOnlyMany,
		v1.ReadWriteMany,
	}
}

func getVolumeSource(spec *volume.Spec) (*v1.RookVolumeSource, bool, error) {
	if spec.Volume != nil && spec.Volume.Rook != nil {
		return spec.Volume.Rook, spec.Volume.Rook.ReadOnly, nil
	} else if spec.PersistentVolume != nil &&
		spec.PersistentVolume.Spec.Rook != nil {
		return spec.PersistentVolume.Spec.Rook, spec.ReadOnly, nil
	}
	return nil, false, fmt.Errorf("Spec does not reference a Rook volume type")
}
