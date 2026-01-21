package trigger_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	commoncap "github.com/smartcontractkit/chainlink-common/pkg/capabilities"
	caperrors "github.com/smartcontractkit/chainlink-common/pkg/capabilities/errors"
	"github.com/smartcontractkit/chainlink-common/pkg/services"
	"github.com/smartcontractkit/chainlink/v2/core/capabilities/remote/trigger"
	"github.com/smartcontractkit/chainlink/v2/core/capabilities/remote/types"
	"github.com/smartcontractkit/chainlink/v2/core/logger"
	p2ptypes "github.com/smartcontractkit/libocr/ragep2p/types"
)

type testTriggerCapability struct {
	err error
}

var _ commoncap.TriggerCapability = (*testTriggerCapability)(nil)

func (t *testTriggerCapability) Info(ctx context.Context) (commoncap.CapabilityInfo, error) {
	return commoncap.CapabilityInfo{
		ID: "test-capability",
	}, nil
}

func (t *testTriggerCapability) RegisterTrigger(ctx context.Context, req commoncap.TriggerRegistrationRequest) (<-chan commoncap.TriggerResponse, error) {
	ch := make(chan commoncap.TriggerResponse, 1)
	return ch, t.err
}

func (t *testTriggerCapability) UnregisterTrigger(ctx context.Context, req commoncap.TriggerRegistrationRequest) error {
	return nil
}

type sendCall struct {
	peerID p2ptypes.PeerID
	msg    *types.MessageBody
}

func TestMultipleAddedRequests_SuccessfulTriggerRegistration(t *testing.T) {
	// Test that when multiple requests are added to a PublisherRegistration,
	// they all receive the same response when the trigger is registered.

	// Setup
	lggr := logger.TestLogger(t)
	triggerID := "test-trigger"
	workflowID := "test-workflow"
	capabilityID := "test-capability"
	capabilityDonID := uint32(1)

	var sendCalls []sendCall
	pr := trigger.NewPublisherRegistration(lggr, make(services.StopChan), triggerID, workflowID, capabilityDonID, capabilityID, func(peerID p2ptypes.PeerID, msgBody *types.MessageBody) error {
		sendCalls = append(sendCalls, sendCall{peerID, msgBody})
		return nil
	})

	peers := make([]p2ptypes.PeerID, 8)
	require.NoError(t, peers[0].UnmarshalText([]byte(peerID1)))
	require.NoError(t, peers[1].UnmarshalText([]byte(peerID2)))
	require.NoError(t, peers[2].UnmarshalText([]byte(peerID3)))

	// Add multiple requests
	peerID1 := peers[0]
	peerID2 := peers[1]
	callerDonID1 := uint32(10)
	callerDonID2 := uint32(20)

	pr.AddRequest(peerID1, callerDonID1)
	pr.AddRequest(peerID2, callerDonID2)

	_, err := pr.RegisterOnUnderlyingTrigger(&testTriggerCapability{err: nil}, commoncap.TriggerRegistrationRequest{
		TriggerID: triggerID,
	})

	require.NoError(t, err)

	// Confirm a trigger response message was sent to each peer
	require.Len(t, sendCalls, 2)

	foundPeer1 := false
	foundPeer2 := false
	for _, call := range sendCalls {
		require.Equal(t, types.RegisterTriggerResponse, call.msg.Method)
		meta := call.msg.GetTriggerRegistrationMetadata()
		require.Nil(t, meta.Error)
		require.Equal(t, triggerID, meta.TriggerId)
		if call.peerID == peerID1 {
			foundPeer1 = true
		} else if call.peerID == peerID2 {
			foundPeer2 = true
		}
	}

	require.True(t, foundPeer1, "did not find message sent to peerID1")
	require.True(t, foundPeer2, "did not find message sent to peerID2")

	// Send an additional registration request after trigger registration has already completed, expect to get immediate response to the new peer
	peerID3 := peers[2]
	callerDonID3 := uint32(30)
	pr.AddRequest(peerID3, callerDonID3)

	// Confirm a trigger response message was sent to the new peer
	require.Len(t, sendCalls, 3)

	foundPeer3 := false
	for _, call := range sendCalls {
		if call.peerID == peerID3 {
			foundPeer3 = true
			require.Equal(t, types.RegisterTriggerResponse, call.msg.Method)
			meta := call.msg.GetTriggerRegistrationMetadata()
			require.Nil(t, meta.Error)
			require.Equal(t, triggerID, meta.TriggerId)
		}
	}

	require.True(t, foundPeer3, "did not find message sent to peerID3")
}

func TestMultipleAddedRequests_TriggerRegistrationError(t *testing.T) {
	// Test that when multiple requests are added to a PublisherRegistration,
	// they all receive the same error response when the trigger registration fails.

	// Setup
	lggr := logger.TestLogger(t)
	triggerID := "test-trigger"
	workflowID := "test-workflow"
	capabilityID := "test-capability"
	capabilityDonID := uint32(1)

	var sendCalls []sendCall
	pr := trigger.NewPublisherRegistration(lggr, make(services.StopChan), triggerID, workflowID, capabilityDonID, capabilityID, func(peerID p2ptypes.PeerID, msgBody *types.MessageBody) error {
		sendCalls = append(sendCalls, sendCall{peerID, msgBody})
		return nil
	})

	peers := make([]p2ptypes.PeerID, 4)
	require.NoError(t, peers[0].UnmarshalText([]byte(peerID1)))
	require.NoError(t, peers[1].UnmarshalText([]byte(peerID2)))

	// Add multiple requests
	peerID1 := peers[0]
	peerID2 := peers[1]
	callerDonID1 := uint32(10)
	callerDonID2 := uint32(20)

	pr.AddRequest(peerID1, callerDonID1)
	pr.AddRequest(peerID2, callerDonID2)

	_, err := pr.RegisterOnUnderlyingTrigger(&testTriggerCapability{err: errors.New("its borken")}, commoncap.TriggerRegistrationRequest{
		TriggerID: triggerID,
	})

	require.Error(t, err)

	// Confirm a trigger response message was sent to each peer
	require.Len(t, sendCalls, 2)

	foundPeer1 := false
	foundPeer2 := false
	for _, call := range sendCalls {
		require.Equal(t, types.RegisterTriggerResponse, call.msg.Method)
		meta := call.msg.GetTriggerRegistrationMetadata()
		require.NotNil(t, meta.Error)
		require.Equal(t, triggerID, meta.TriggerId)
		caperror := caperrors.DeserializeErrorFromString(*meta.Error)
		require.Equal(t, caperrors.Unknown, caperror.Code())
		require.Equal(t, caperrors.OriginSystem, caperror.Origin())
		require.Equal(t, caperrors.VisibilityPublic, caperror.Visibility())
		if call.peerID == peerID1 {
			foundPeer1 = true
		} else if call.peerID == peerID2 {
			foundPeer2 = true
		}
	}

	require.True(t, foundPeer1, "did not find message sent to peerID1")
	require.True(t, foundPeer2, "did not find message sent to peerID2")

	// Send an additional registration request after trigger registration has already completed with error, expect to get immediate error response to the new peer
	peerID3 := peers[2]
	callerDonID3 := uint32(30)
	pr.AddRequest(peerID3, callerDonID3)

	// Confirm a trigger response message was sent to the new peer
	require.Len(t, sendCalls, 3)

	foundPeer3 := false
	for _, call := range sendCalls {
		if call.peerID == peerID3 {
			foundPeer3 = true
			require.Equal(t, types.RegisterTriggerResponse, call.msg.Method)
			meta := call.msg.GetTriggerRegistrationMetadata()
			require.NotNil(t, meta.Error)

			caperror := caperrors.DeserializeErrorFromString(*meta.Error)
			require.Equal(t, caperrors.Unknown, caperror.Code())
			require.Equal(t, caperrors.OriginSystem, caperror.Origin())
			require.Equal(t, caperrors.VisibilityPublic, caperror.Visibility())

			require.Equal(t, triggerID, meta.TriggerId)
		}
	}

	require.True(t, foundPeer3, "did not find message sent to peerID3")
}

func TestMultipleAddedRequests_TriggerRegistrationCapabilityUserError(t *testing.T) {
	// Test a user error from trigger registration is correctly propagated
	lggr := logger.TestLogger(t)
	triggerID := "test-trigger"
	workflowID := "test-workflow"
	capabilityID := "test-capability"
	capabilityDonID := uint32(1)

	var sendCalls []sendCall
	pr := trigger.NewPublisherRegistration(lggr, make(services.StopChan), triggerID, workflowID, capabilityDonID, capabilityID, func(peerID p2ptypes.PeerID, msgBody *types.MessageBody) error {
		sendCalls = append(sendCalls, sendCall{peerID, msgBody})
		return nil
	})

	peers := make([]p2ptypes.PeerID, 4)
	require.NoError(t, peers[0].UnmarshalText([]byte(peerID1)))
	require.NoError(t, peers[1].UnmarshalText([]byte(peerID2)))

	// Add multiple requests
	peerID1 := peers[0]
	peerID2 := peers[1]
	callerDonID1 := uint32(10)
	callerDonID2 := uint32(20)

	pr.AddRequest(peerID1, callerDonID1)
	pr.AddRequest(peerID2, callerDonID2)

	_, err := pr.RegisterOnUnderlyingTrigger(&testTriggerCapability{err: caperrors.NewPublicUserError(errors.New("its borken"), caperrors.InvalidArgument)}, commoncap.TriggerRegistrationRequest{
		TriggerID: triggerID,
	})

	require.Error(t, err)

	// Confirm a trigger response message was sent to each peer
	require.Len(t, sendCalls, 2)

	foundPeer1 := false
	foundPeer2 := false
	for _, call := range sendCalls {
		require.Equal(t, types.RegisterTriggerResponse, call.msg.Method)
		meta := call.msg.GetTriggerRegistrationMetadata()
		require.NotNil(t, meta.Error)
		require.Equal(t, triggerID, meta.TriggerId)
		caperror := caperrors.DeserializeErrorFromString(*meta.Error)
		require.Equal(t, caperrors.InvalidArgument, caperror.Code())
		require.Equal(t, caperrors.OriginUser, caperror.Origin())
		require.Equal(t, caperrors.VisibilityPublic, caperror.Visibility())
		if call.peerID == peerID1 {
			foundPeer1 = true
		} else if call.peerID == peerID2 {
			foundPeer2 = true
		}
	}

	require.True(t, foundPeer1, "did not find message sent to peerID1")
	require.True(t, foundPeer2, "did not find message sent to peerID2")
}
