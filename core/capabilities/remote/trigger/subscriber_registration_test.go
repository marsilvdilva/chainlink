package trigger_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smartcontractkit/chainlink/v2/core/capabilities/remote/messagecache"
	"github.com/smartcontractkit/chainlink/v2/core/capabilities/remote/trigger"
	"github.com/smartcontractkit/chainlink/v2/core/capabilities/remote/types"
	"github.com/smartcontractkit/chainlink/v2/core/logger"
	p2ptypes "github.com/smartcontractkit/libocr/ragep2p/types"
)

const (
	peerID1 = "12D3KooWF3dVeJ6YoT5HFnYhmwQWWMoEwVFzJQ5kKCMX3ZityxMC"
	peerID2 = "12D3KooWQsmok6aD8PZqt3RnJhQRrNzKHLficq7zYFRp7kZ1hHP8"
	peerID3 = "12D3KooWPumsXxg6mJ4hmRizjBD7oFMtN9vm3kTwN8BLEinyDPJS"
	peerID4 = "12D3KooWNuumb38Jpw6DoRbgwejcZYwfsWbbzPU4fWy5imrW3dyD"
)

func TestSubscriberRegistration_SuccessfulRegistration(t *testing.T) {
	lggr := logger.TestLogger(t)
	ctx := t.Context()

	peers := make([]p2ptypes.PeerID, 8)
	require.NoError(t, peers[0].UnmarshalText([]byte(peerID1)))
	require.NoError(t, peers[1].UnmarshalText([]byte(peerID2)))
	require.NoError(t, peers[2].UnmarshalText([]byte(peerID3)))
	require.NoError(t, peers[3].UnmarshalText([]byte(peerID4)))

	registration := trigger.NewSubscriberRegistration(lggr, []byte("rawRequest"), messagecache.NewMessageCache[trigger.TriggerRegistrationKey, p2ptypes.PeerID](),
		1*time.Minute)

	registrationResponseMessage := &types.MessageBody{
		CapabilityId:    "evm1",
		CapabilityDonId: 2,
		CallerDonId:     3,
		Method:          types.RegisterTriggerResponse,
		Metadata: &types.MessageBody_TriggerRegistrationMetadata{
			TriggerRegistrationMetadata: &types.TriggerRegistrationMetadata{
				TriggerId:  "w1t1",
				WorkflowId: "w1",
				Error:      nil,
			},
		},
	}

	go func() {
		capDonF := uint8(1)
		minResponsesToAggregate := uint32(capDonF + 1)
		numberOfCapabilityNodes := int(capDonF*2 + 1)
		messageExpiryMillis := int64(60000)
		registration.HandleTriggerRegistrationResponse(peers[0], registrationResponseMessage, minResponsesToAggregate, messageExpiryMillis, numberOfCapabilityNodes)
		registration.HandleTriggerRegistrationResponse(peers[1], registrationResponseMessage, minResponsesToAggregate, messageExpiryMillis, numberOfCapabilityNodes)
		registration.HandleTriggerRegistrationResponse(peers[2], registrationResponseMessage, minResponsesToAggregate, messageExpiryMillis, numberOfCapabilityNodes)
	}()

	subscriberStopCh := make(chan struct{})
	responseCh, err := registration.AwaitInitialRegistrationResponse(ctx, subscriberStopCh)
	require.NoError(t, err)
	require.NotNil(t, responseCh)

	// Ensure that the second wait for response returns immediately
	responseCh2, err := registration.AwaitInitialRegistrationResponse(ctx, subscriberStopCh)
	require.NoError(t, err)
	require.Equal(t, responseCh, responseCh2)
}

func TestSubscriberRegistration_SuccessfulRegistration_ZeroTimeout(t *testing.T) {
	lggr := logger.TestLogger(t)
	ctx := t.Context()

	// Zero timeout indicates the caller does not want to wait to registration responses, this effectively disables registration response handling
	registration := trigger.NewSubscriberRegistration(lggr, []byte("rawRequest"), messagecache.NewMessageCache[trigger.TriggerRegistrationKey, p2ptypes.PeerID](),
		0)

	subscriberStopCh := make(chan struct{})
	responseCh, err := registration.AwaitInitialRegistrationResponse(ctx, subscriberStopCh)
	require.NoError(t, err)
	require.NotNil(t, responseCh)
}

func TestSubscriberRegistration_UnsuccessfulRegistration_SameError(t *testing.T) {
	lggr := logger.TestLogger(t)
	ctx := t.Context()

	peers := make([]p2ptypes.PeerID, 8)
	require.NoError(t, peers[0].UnmarshalText([]byte(peerID1)))
	require.NoError(t, peers[1].UnmarshalText([]byte(peerID2)))
	require.NoError(t, peers[2].UnmarshalText([]byte(peerID3)))
	require.NoError(t, peers[3].UnmarshalText([]byte(peerID4)))

	registration := trigger.NewSubscriberRegistration(lggr, []byte("rawRequest"), messagecache.NewMessageCache[trigger.TriggerRegistrationKey, p2ptypes.PeerID](),
		1*time.Minute)

	errMsg := "its broken"
	registrationResponseMessage := &types.MessageBody{
		CapabilityId:    "evm1",
		CapabilityDonId: 2,
		CallerDonId:     3,
		Method:          types.RegisterTriggerResponse,
		Metadata: &types.MessageBody_TriggerRegistrationMetadata{
			TriggerRegistrationMetadata: &types.TriggerRegistrationMetadata{
				TriggerId:  "w1t1",
				WorkflowId: "w1",
				Error:      &errMsg,
			},
		},
	}

	go func() {
		capDonF := uint8(1)
		minResponsesToAggregate := uint32(capDonF + 1)
		numberOfCapabilityNodes := int(capDonF*2 + 1)
		messageExpiryMillis := int64(60000)
		registration.HandleTriggerRegistrationResponse(peers[0], registrationResponseMessage, minResponsesToAggregate, messageExpiryMillis, numberOfCapabilityNodes)
		registration.HandleTriggerRegistrationResponse(peers[1], registrationResponseMessage, minResponsesToAggregate, messageExpiryMillis, numberOfCapabilityNodes)
		registration.HandleTriggerRegistrationResponse(peers[2], registrationResponseMessage, minResponsesToAggregate, messageExpiryMillis, numberOfCapabilityNodes)
	}()

	subscriberStopCh := make(chan struct{})
	responseCh, err := registration.AwaitInitialRegistrationResponse(ctx, subscriberStopCh)
	require.Error(t, err)
	require.NotNil(t, responseCh)
	require.Equal(t, err.Error(), errMsg)

	// Ensure that the second wait for response returns immediately, as it not the initial registration it should just return the response channel and no error
	responseCh2, err := registration.AwaitInitialRegistrationResponse(ctx, subscriberStopCh)
	require.NoError(t, err)
	require.Equal(t, responseCh, responseCh2)
}

func TestSubscriberRegistration_UnsuccessfulRegistration_MixedErrors(t *testing.T) {
	lggr := logger.TestLogger(t)
	ctx := t.Context()

	peers := make([]p2ptypes.PeerID, 8)
	require.NoError(t, peers[0].UnmarshalText([]byte(peerID1)))
	require.NoError(t, peers[1].UnmarshalText([]byte(peerID2)))
	require.NoError(t, peers[2].UnmarshalText([]byte(peerID3)))
	require.NoError(t, peers[3].UnmarshalText([]byte(peerID4)))

	registration := trigger.NewSubscriberRegistration(lggr, []byte("rawRequest"), messagecache.NewMessageCache[trigger.TriggerRegistrationKey, p2ptypes.PeerID](),
		1*time.Minute)

	errMsg1 := "its broken1"
	errMsg2 := "its broken2"
	errMsg3 := "its broken3"

	go func() {
		capDonF := uint8(1)
		minResponsesToAggregate := uint32(capDonF + 1)
		numberOfCapabilityNodes := int(capDonF*2 + 1)
		messageExpiryMillis := int64(60000)
		registration.HandleTriggerRegistrationResponse(peers[0], createRegisterResponseMessageWithError(errMsg1), minResponsesToAggregate, messageExpiryMillis, numberOfCapabilityNodes)
		registration.HandleTriggerRegistrationResponse(peers[1], createRegisterResponseMessageWithError(errMsg2), minResponsesToAggregate, messageExpiryMillis, numberOfCapabilityNodes)
		registration.HandleTriggerRegistrationResponse(peers[2], createRegisterResponseMessageWithError(errMsg3), minResponsesToAggregate, messageExpiryMillis, numberOfCapabilityNodes)
	}()

	subscriberStopCh := make(chan struct{})
	responseCh, err := registration.AwaitInitialRegistrationResponse(ctx, subscriberStopCh)
	require.Error(t, err)
	require.NotNil(t, responseCh)
	require.Contains(t, err.Error(), "received 2 errors, last error OK : its broken2")

	// Ensure that the second wait for response returns immediately, as it not the initial registration it should just return the response channel and no error
	responseCh2, err := registration.AwaitInitialRegistrationResponse(ctx, subscriberStopCh)
	require.NoError(t, err)
	require.Equal(t, responseCh, responseCh2)
}

func createRegisterResponseMessageWithError(errMsg1 string) *types.MessageBody {
	return &types.MessageBody{
		CapabilityId:    "evm1",
		CapabilityDonId: 2,
		CallerDonId:     3,
		Method:          types.RegisterTriggerResponse,
		Metadata: &types.MessageBody_TriggerRegistrationMetadata{
			TriggerRegistrationMetadata: &types.TriggerRegistrationMetadata{
				TriggerId:  "w1t1",
				WorkflowId: "w1",
				Error:      &errMsg1,
			},
		},
	}
}

func TestSubscriberRegistration_Timesout(t *testing.T) {
	lggr := logger.TestLogger(t)
	ctx := t.Context()

	peers := make([]p2ptypes.PeerID, 8)
	require.NoError(t, peers[0].UnmarshalText([]byte(peerID1)))
	require.NoError(t, peers[1].UnmarshalText([]byte(peerID2)))
	require.NoError(t, peers[2].UnmarshalText([]byte(peerID3)))
	require.NoError(t, peers[3].UnmarshalText([]byte(peerID4)))

	registration := trigger.NewSubscriberRegistration(lggr, []byte("rawRequest"), messagecache.NewMessageCache[trigger.TriggerRegistrationKey, p2ptypes.PeerID](),
		1*time.Second)

	registrationResponseMessage := &types.MessageBody{
		CapabilityId:    "evm1",
		CapabilityDonId: 2,
		CallerDonId:     3,
		Method:          types.RegisterTriggerResponse,
		Metadata: &types.MessageBody_TriggerRegistrationMetadata{
			TriggerRegistrationMetadata: &types.TriggerRegistrationMetadata{
				TriggerId:  "w1t1",
				WorkflowId: "w1",
				Error:      nil,
			},
		},
	}

	go func() {
		capDonF := uint8(1)
		minResponsesToAggregate := uint32(capDonF + 1)
		numberOfCapabilityNodes := int(capDonF*2 + 1)
		messageExpiryMillis := int64(60000)
		registration.HandleTriggerRegistrationResponse(peers[0], registrationResponseMessage, minResponsesToAggregate, messageExpiryMillis, numberOfCapabilityNodes)
	}()

	subscriberStopCh := make(chan struct{})
	responseCh, err := registration.AwaitInitialRegistrationResponse(ctx, subscriberStopCh)
	require.Error(t, err)
	require.Equal(t, trigger.ErrRegistrationResponseTimeout, err)

	// The channel should still be returned, but the error indicates initial registration failure, it may be that a subsequent registration attempt succeeds
	// hence we do not return nil channel here.  The caller will need to check the error and determine what to do, typically this would be done by checking
	// if it's a user capability error, in which case retrying would not help.
	require.NotNil(t, responseCh)
}

func TestSubscriberRegistration_InsufficientResponses(t *testing.T) {
	lggr := logger.TestLogger(t)
	ctx := t.Context()

	peers := make([]p2ptypes.PeerID, 8)
	require.NoError(t, peers[0].UnmarshalText([]byte(peerID1)))
	require.NoError(t, peers[1].UnmarshalText([]byte(peerID2)))
	require.NoError(t, peers[2].UnmarshalText([]byte(peerID3)))
	require.NoError(t, peers[3].UnmarshalText([]byte(peerID4)))

	registration := trigger.NewSubscriberRegistration(lggr, []byte("rawRequest"), messagecache.NewMessageCache[trigger.TriggerRegistrationKey, p2ptypes.PeerID](),
		1*time.Minute)

	registrationResponseMessage := &types.MessageBody{
		CapabilityId:    "evm1",
		CapabilityDonId: 2,
		CallerDonId:     3,
		Method:          types.RegisterTriggerResponse,
		Metadata: &types.MessageBody_TriggerRegistrationMetadata{
			TriggerRegistrationMetadata: &types.TriggerRegistrationMetadata{
				TriggerId:  "w1t1",
				WorkflowId: "w1",
				Error:      nil,
			},
		},
	}

	ctxWithCancel, cancel := context.WithCancel(ctx)

	go func() {
		capDonF := uint8(1)
		minResponsesToAggregate := uint32(capDonF + 1)
		numberOfCapabilityNodes := int(capDonF*2 + 1)
		messageExpiryMillis := int64(60000)
		registration.HandleTriggerRegistrationResponse(peers[0], registrationResponseMessage, minResponsesToAggregate, messageExpiryMillis, numberOfCapabilityNodes)
		
		// Simulate insufficient responses by not sending the third response
		// Cancel the context to simulate timeout or cancellation
		cancel()
	}()

	subscriberStopCh := make(chan struct{})
	responseCh, err := registration.AwaitInitialRegistrationResponse(ctxWithCancel, subscriberStopCh)
	require.Error(t, err)

	// The channel should still be returned, but the error indicates initial registration failure, it may be that a subsequent registration attempt succeeds
	// hence we do not return nil channel here.  The caller will need to check the error and determine what to do, typically this would be done by checking
	// if it's a user capability error, in which case retrying would not help.
	require.NotNil(t, responseCh)
}
