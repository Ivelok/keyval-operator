package main

import (
	"flag"
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(keyvalv1alpha1.AddToScheme(scheme))
}

func main() {
	cfg := DefaultAppConfig()
	cfg.ApplyEnv()
	cfg.BindFlags(flag.CommandLine)
	flag.Parse()
	if err := cfg.Validate(); err != nil {
		setupLog.Error(err, "validate config")
		os.Exit(1)
	}

	ctx := ctrl.SetupSignalHandler()
	if err := Run(ctx, cfg); err != nil {
		setupLog.Error(err, "run")
		os.Exit(1)
	}
}
