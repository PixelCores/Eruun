package aliyun

import (
	"fmt"
	"strings"

	aliyunnas "github.com/alibabacloud-go/nas-20170626/v2/client"

	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/cloudjob/contracts"
)

func (c *client) ensureMountTarget(params map[string]interface{}) (*contracts.CloudJobResult, error) {
	tenantID, err := requireCloudMapString(params, ParamTenantID)
	if err != nil {
		return nil, err
	}
	fileSystemID, err := requireCloudMapString(params, StateFileSystemIDKey)
	if err != nil {
		return nil, err
	}
	vpcID, err := c.requireConfigValue(c.config.VpcID, ParamVpcID, ActionNasEnsureMountTarget)
	if err != nil {
		return nil, err
	}
	vSwitchID, err := c.requireConfigValue(c.config.VSwitchID, ParamVSwitchID, ActionNasEnsureMountTarget)
	if err != nil {
		return nil, err
	}
	accessGroupName := cloudMapString(params, ParamAccessGroupName)
	if accessGroupName == "" {
		accessGroupName = AliyunNASDefaultAccessGroupName
	}
	securityGroupID := cloudMapString(params, ParamSecurityGroupID)
	mountDomainHint := cloudMapString(params, StateMountDomainKey)

	existingTarget, requestID, err := c.describeSingleMountTarget(fileSystemID, mountDomainHint, c.config.VpcID, c.config.VSwitchID, accessGroupName)
	if err != nil {
		return nil, err
	}
	if existingTarget != nil {
		if err := validateExistingMountTarget(existingTarget, vpcID, vSwitchID, accessGroupName); err != nil {
			return nil, err
		}
		if securityGroupID != "" && mountDomainHint == "" {
			return nil, fmt.Errorf(
				"aliyun nas mount target already exists and params.%s requires explicit params.%s for deterministic reuse",
				ParamSecurityGroupID,
				StateMountDomainKey,
			)
		}
		return &contracts.CloudJobResult{
			RequestID: requestID,
			Message:   "aliyun nas mount target already exists",
			Output: map[string]interface{}{
				StateFileSystemIDKey: fileSystemID,
				StateMountDomainKey:  stringValue(existingTarget.MountTargetDomain),
			},
		}, nil
	}

	createRequest := new(aliyunnas.CreateMountTargetRequest).
		SetFileSystemId(fileSystemID).
		SetVpcId(vpcID).
		SetVSwitchId(vSwitchID).
		SetNetworkType(AliyunNASNetworkTypeVpc).
		SetAccessGroupName(accessGroupName)
	if securityGroupID != "" {
		createRequest.SetSecurityGroupId(securityGroupID)
	}

	createResponse, err := c.nas.CreateMountTarget(createRequest)
	if err != nil {
		return nil, fmt.Errorf("create aliyun nas mount target for tenant %q filesystem %q: %w", tenantID, fileSystemID, err)
	}
	if createResponse == nil || createResponse.Body == nil {
		return nil, fmt.Errorf("aliyun nas create mount target returned nil body")
	}
	mountDomain := stringValue(createResponse.Body.MountTargetDomain)
	if mountDomain == "" {
		return nil, fmt.Errorf("aliyun nas create mount target returned empty %s", StateMountDomainKey)
	}

	return &contracts.CloudJobResult{
		RequestID: stringValue(createResponse.Body.RequestId),
		Message:   "aliyun nas mount target ensured",
		Output: map[string]interface{}{
			StateFileSystemIDKey: fileSystemID,
			StateMountDomainKey:  mountDomain,
		},
	}, nil
}

func (c *client) describeMountTarget(params map[string]interface{}) (*contracts.CloudJobResult, error) {
	tenantID, err := requireCloudMapString(params, ParamTenantID)
	if err != nil {
		return nil, err
	}

	fileSystemID := cloudMapString(params, StateFileSystemIDKey)
	requestID := ""
	if fileSystemID == "" {
		fileSystem, describeRequestID, err := c.describeFileSystem(tenantID, "")
		if err != nil {
			return nil, err
		}
		requestID = describeRequestID
		if fileSystem == nil {
			return &contracts.CloudJobResult{
				RequestID: requestID,
				Message:   "aliyun nas filesystem not found",
			}, nil
		}
		fileSystemID = stringValue(fileSystem.FileSystemId)
		if fileSystemID == "" {
			return nil, fmt.Errorf("aliyun nas describe filesystem returned empty %s", StateFileSystemIDKey)
		}
	}

	accessGroupName := cloudMapString(params, ParamAccessGroupName)
	if accessGroupName == "" {
		accessGroupName = AliyunNASDefaultAccessGroupName
	}
	target, targetRequestID, err := c.describeSingleMountTarget(
		fileSystemID,
		cloudMapString(params, StateMountDomainKey),
		c.config.VpcID,
		c.config.VSwitchID,
		accessGroupName,
	)
	if err != nil {
		return nil, err
	}
	if requestID == "" {
		requestID = targetRequestID
	}

	output := map[string]interface{}{
		StateFileSystemIDKey: fileSystemID,
	}
	if target == nil {
		return &contracts.CloudJobResult{
			RequestID: requestID,
			Message:   "aliyun nas mount target not found",
			Output:    output,
		}, nil
	}

	if mountDomain := stringValue(target.MountTargetDomain); mountDomain != "" {
		output[StateMountDomainKey] = mountDomain
	}
	if mountStatus := stringValue(target.Status); mountStatus != "" {
		output[StateMountStatusKey] = mountStatus
	}
	if confirmInfo := buildMountTargetConfirmInfo(target); confirmInfo != nil {
		output[StateMountConfirmInfoKey] = confirmInfo
	}

	return &contracts.CloudJobResult{
		RequestID: firstNonEmptyString(targetRequestID, requestID),
		Message:   "aliyun nas mount target described",
		Output:    output,
	}, nil
}

func (c *client) describeSingleMountTarget(
	fileSystemID,
	mountDomain,
	vpcID,
	vSwitchID,
	accessGroupName string,
) (*aliyunnas.DescribeMountTargetsResponseBodyMountTargetsMountTarget, string, error) {
	fileSystemID = strings.TrimSpace(fileSystemID)
	mountDomain = strings.TrimSpace(mountDomain)
	vpcID = strings.TrimSpace(vpcID)
	vSwitchID = strings.TrimSpace(vSwitchID)
	accessGroupName = strings.TrimSpace(accessGroupName)

	request := new(aliyunnas.DescribeMountTargetsRequest).
		SetFileSystemId(fileSystemID).
		SetPageNumber(1).
		SetPageSize(10)
	if mountDomain != "" {
		request.SetMountTargetDomain(mountDomain)
	}

	response, err := c.nas.DescribeMountTargets(request)
	if err != nil {
		return nil, "", fmt.Errorf("describe aliyun nas mount targets for filesystem %q: %w", fileSystemID, err)
	}
	if response == nil || response.Body == nil {
		return nil, "", fmt.Errorf("aliyun nas describe mount targets returned nil body")
	}

	mountTargets := response.Body.MountTargets
	if mountTargets == nil || len(mountTargets.MountTarget) == 0 {
		return nil, stringValue(response.Body.RequestId), nil
	}
	if len(mountTargets.MountTarget) == 1 {
		return mountTargets.MountTarget[0], stringValue(response.Body.RequestId), nil
	}

	if mountDomain != "" {
		return nil, "", fmt.Errorf(
			"aliyun nas mount target lookup for filesystem %q and %s=%q is ambiguous: found %d mount targets",
			fileSystemID,
			StateMountDomainKey,
			mountDomain,
			len(mountTargets.MountTarget),
		)
	}

	matchedTargets := filterMountTargetsByTopology(mountTargets.MountTarget, vpcID, vSwitchID, accessGroupName)
	switch len(matchedTargets) {
	case 1:
		return matchedTargets[0], stringValue(response.Body.RequestId), nil
	case 0:
		return nil, "", fmt.Errorf(
			"aliyun nas mount target lookup for filesystem %q has no match for topology (%s=%q, %s=%q, %s=%q) among %d mount targets",
			fileSystemID,
			ParamVpcID,
			vpcID,
			ParamVSwitchID,
			vSwitchID,
			ParamAccessGroupName,
			accessGroupName,
			len(mountTargets.MountTarget),
		)
	default:
		return nil, "", fmt.Errorf(
			"aliyun nas mount target lookup for filesystem %q is ambiguous for topology (%s=%q, %s=%q, %s=%q): matched %d of %d mount targets",
			fileSystemID,
			ParamVpcID,
			vpcID,
			ParamVSwitchID,
			vSwitchID,
			ParamAccessGroupName,
			accessGroupName,
			len(matchedTargets),
			len(mountTargets.MountTarget),
		)
	}
}

func filterMountTargetsByTopology(
	targets []*aliyunnas.DescribeMountTargetsResponseBodyMountTargetsMountTarget,
	vpcID,
	vSwitchID,
	accessGroupName string,
) []*aliyunnas.DescribeMountTargetsResponseBodyMountTargetsMountTarget {
	if len(targets) == 0 {
		return nil
	}
	vpcID = strings.TrimSpace(vpcID)
	vSwitchID = strings.TrimSpace(vSwitchID)
	accessGroupName = strings.TrimSpace(accessGroupName)

	out := make([]*aliyunnas.DescribeMountTargetsResponseBodyMountTargetsMountTarget, 0, len(targets))
	for _, target := range targets {
		if target == nil {
			continue
		}
		if vpcID != "" && stringValue(target.VpcId) != vpcID {
			continue
		}
		if vSwitchID != "" && stringValue(target.VswId) != vSwitchID {
			continue
		}
		if accessGroupName != "" && stringValue(target.AccessGroup) != accessGroupName {
			continue
		}
		out = append(out, target)
	}
	return out
}

func validateExistingMountTarget(target *aliyunnas.DescribeMountTargetsResponseBodyMountTargetsMountTarget, vpcID, vSwitchID, accessGroupName string) error {
	if target == nil {
		return fmt.Errorf("aliyun nas mount target is nil")
	}
	existingVpcID := stringValue(target.VpcId)
	if existingVpcID != "" && existingVpcID != vpcID {
		return fmt.Errorf("aliyun nas mount target already exists with %s=%q, want %q", ParamVpcID, existingVpcID, vpcID)
	}
	existingVSwitchID := stringValue(target.VswId)
	if existingVSwitchID != "" && existingVSwitchID != vSwitchID {
		return fmt.Errorf("aliyun nas mount target already exists with %s=%q, want %q", ParamVSwitchID, existingVSwitchID, vSwitchID)
	}
	existingNetworkType := stringValue(target.NetworkType)
	if existingNetworkType != "" && existingNetworkType != AliyunNASNetworkTypeVpc {
		return fmt.Errorf("aliyun nas mount target already exists with networkType=%q, want %q", existingNetworkType, AliyunNASNetworkTypeVpc)
	}
	existingAccessGroup := stringValue(target.AccessGroup)
	if existingAccessGroup != "" && accessGroupName != "" && existingAccessGroup != accessGroupName {
		return fmt.Errorf("aliyun nas mount target already exists with %s=%q, want %q", ParamAccessGroupName, existingAccessGroup, accessGroupName)
	}
	return nil
}

func buildMountTargetConfirmInfo(target *aliyunnas.DescribeMountTargetsResponseBodyMountTargetsMountTarget) map[string]interface{} {
	if target == nil {
		return nil
	}
	info := map[string]interface{}{}
	if vpcID := stringValue(target.VpcId); vpcID != "" {
		info[ParamVpcID] = vpcID
	}
	if vSwitchID := stringValue(target.VswId); vSwitchID != "" {
		info[ParamVSwitchID] = vSwitchID
	}
	if accessGroupName := stringValue(target.AccessGroup); accessGroupName != "" {
		info[ParamAccessGroupName] = accessGroupName
	}
	if networkType := stringValue(target.NetworkType); networkType != "" {
		info["networkType"] = networkType
	}
	if clientMasterNodes := buildClientMasterNodesConfirmInfo(target.ClientMasterNodes); len(clientMasterNodes) > 0 {
		info["clientMasterNodes"] = clientMasterNodes
	}
	if len(info) == 0 {
		return nil
	}
	return info
}

func buildClientMasterNodesConfirmInfo(nodes *aliyunnas.DescribeMountTargetsResponseBodyMountTargetsMountTargetClientMasterNodes) []map[string]string {
	if nodes == nil || len(nodes.ClientMasterNode) == 0 {
		return nil
	}
	confirmInfo := make([]map[string]string, 0, len(nodes.ClientMasterNode))
	for _, node := range nodes.ClientMasterNode {
		if node == nil {
			continue
		}
		item := map[string]string{}
		if ecsID := stringValue(node.EcsId); ecsID != "" {
			item["ecsId"] = ecsID
		}
		if ecsIP := stringValue(node.EcsIp); ecsIP != "" {
			item["ecsIp"] = ecsIP
		}
		if len(item) == 0 {
			continue
		}
		confirmInfo = append(confirmInfo, item)
	}
	return confirmInfo
}
