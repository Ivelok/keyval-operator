// +kubebuilder:object:generate=true
// +groupName=keyval.ivelok.io

package v1alpha1

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

const (
	// Group is the API group for KeyValCluster resources.
	Group = "keyval.ivelok.io"
	// Version is the API version for KeyValCluster resources.
	Version = "v1alpha1"
)

// GroupVersion is group version used to register these objects.
var GroupVersion = schema.GroupVersion{Group: Group, Version: Version}

// SchemeBuilder is used to add go types to the GroupVersionKind scheme
var SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion}

// AddToScheme adds the types in this group-version to the given scheme.
var AddToScheme = SchemeBuilder.AddToScheme
