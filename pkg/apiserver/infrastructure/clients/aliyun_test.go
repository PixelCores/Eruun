package clients

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	aliyunnas "github.com/alibabacloud-go/nas-20170626/v2/client"
	"github.com/stretchr/testify/require"
	"testing"
)

type connectivityClientFunc func(*aliyunnas.DescribeFileSystemsRequest) (*aliyunnas.DescribeFileSystemsResponse, error)

func (f connectivityClientFunc) DescribeFileSystems(r *aliyunnas.DescribeFileSystemsRequest) (*aliyunnas.DescribeFileSystemsResponse, error) {
	return f(r)
}

func TestAliyunNASConnectivity(t *testing.T) {
	for _, tc := range []struct {
		name     string
		response *aliyunnas.DescribeFileSystemsResponse
		err      error
		message  string
	}{
		{name: "success", response: &aliyunnas.DescribeFileSystemsResponse{Body: &aliyunnas.DescribeFileSystemsResponseBody{}}},
		{name: "SDK failure", err: errors.New("sdk failed"), message: "sdk failed"},
		{name: "nil response", message: "nil body"},
		{name: "nil body", response: &aliyunnas.DescribeFileSystemsResponse{}, message: "nil body"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := checkAliyunNASConnectivity(context.Background(), connectivityClientFunc(func(r *aliyunnas.DescribeFileSystemsRequest) (*aliyunnas.DescribeFileSystemsResponse, error) {
				require.EqualValues(t, 1, *r.PageNumber)
				require.EqualValues(t, 1, *r.PageSize)
				return tc.response, tc.err
			}))
			if tc.message == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.message)
			}
		})
	}
}

func TestAliyunNASConnectivityRejectsInvalidConfigAndCancellation(t *testing.T) {
	require.ErrorContains(t, ValidateAliyunNASConnectivity(context.Background(), json.RawMessage(`{"accessKeyId":"test-ak","accessKeySecret":"******","regionId":"cn-hangzhou"}`)), "accessKeySecret")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, ValidateAliyunNASConnectivity(ctx, nil), context.Canceled)
	require.ErrorIs(t, checkAliyunNASConnectivity(ctx, connectivityClientFunc(func(*aliyunnas.DescribeFileSystemsRequest) (*aliyunnas.DescribeFileSystemsResponse, error) {
		return &aliyunnas.DescribeFileSystemsResponse{Body: &aliyunnas.DescribeFileSystemsResponseBody{}}, nil
	})), context.Canceled)
}

func TestAliyunNASClientNormalizesConfigAndBoundsConnectivityTimeouts(t *testing.T) {
	cfg := spec.AliyunCloudSettingSpec{AccessKeyID: " test-ak ", AccessKeySecret: " test-sk ", RegionID: " cn-hangzhou ", Endpoint: " nas.example.com "}
	client, err := newNASClientWithTimeout(cfg, 3000, 5000)
	require.NoError(t, err)
	require.Equal(t, "nas.example.com", *client.Endpoint)
	require.EqualValues(t, 3000, *client.ConnectTimeout)
	require.EqualValues(t, 5000, *client.ReadTimeout)
	_, err = NewAliyunNASClient(spec.AliyunCloudSettingSpec{})
	require.ErrorContains(t, err, "accessKeyId")
}
