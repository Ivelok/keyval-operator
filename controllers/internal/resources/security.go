package resources

const (
	// TLSVolumeName is the volume name used to mount TLS certificates into pods.
	TLSVolumeName = "tls-certificates"
	// TLSMountPath is the path where TLS secrets are mounted inside pods.
	TLSMountPath = "/tls"
)
