package kubernetes

import (
	"fmt"

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// NewClients builds a kubernetes.Interface from the given kubeconfig path.
// If kubeconfig is empty, it falls back to in-cluster configuration.
func NewClients(kubeconfig string) (kubernetes.Interface, dynamic.Interface, error) {
	var restCfg *rest.Config
	var err error

	if kubeconfig != "" {
		restCfg, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	} else {
		restCfg, err = rest.InClusterConfig()
	}
	if err != nil {
		return nil, nil, fmt.Errorf("building kubernetes config: %w", err)
	}

	client, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return nil, nil, fmt.Errorf("creating kubernetes client: %w", err)
	}

	dynClient, err := dynamic.NewForConfig(restCfg)
	if err != nil {
		return nil, nil, fmt.Errorf("creating dynamic kubernetes client: %w", err)
	}
	return client, dynClient, nil
}
