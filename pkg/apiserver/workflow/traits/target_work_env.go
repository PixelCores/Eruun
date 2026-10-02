package traits

// processTargetWorkEnv renders the user-facing targetWorkEnv trait into a pod nodeSelector.
func processTargetWorkEnv(targetWorkEnv map[string]string) (*TraitResult, error) {
	if len(targetWorkEnv) == 0 {
		return nil, nil
	}

	nodeSelector := make(map[string]string, len(targetWorkEnv))
	for key, value := range targetWorkEnv {
		nodeSelector[key] = value
	}

	return &TraitResult{
		NodeSelector: nodeSelector,
	}, nil
}
