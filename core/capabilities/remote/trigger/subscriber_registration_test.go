package trigger_test

import (
	"testing"

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

func TestSubscriberRegistration(t *testing.T) {
	lggr := logger.TestLogger(t)
	ctx := t.Context()

	peers := make([]p2ptypes.PeerID, 8)
	require.NoError(t, peers[0].UnmarshalText([]byte(peerID1)))
	require.NoError(t, peers[1].UnmarshalText([]byte(peerID2)))
	require.NoError(t, peers[2].UnmarshalText([]byte(peerID3)))
	require.NoError(t, peers[3].UnmarshalText([]byte(peerID4)))

	registration := trigger.NewSubscriberRegistration(lggr, []byte("rawRequest"), messagecache.NewMessageCache[trigger.TriggerRegistrationKey, p2ptypes.PeerID]())

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
		minResponsesToAggregate := uint32(2*capDonF + 1)
		messageExpiryMillis := int64(60000)
		registration.HandleTriggerRegistrationResponse(peers[0], registrationResponseMessage, minResponsesToAggregate, messageExpiryMillis, capDonF)
		registration.HandleTriggerRegistrationResponse(peers[1], registrationResponseMessage, minResponsesToAggregate, messageExpiryMillis, capDonF)
		registration.HandleTriggerRegistrationResponse(peers[2], registrationResponseMessage, minResponsesToAggregate, messageExpiryMillis, capDonF)
	}()

	subscriberStopCh := make(chan struct{})
	responseCh, err := registration.AwaitInitialRegistrationResponse(ctx, subscriberStopCh)
	require.NoError(t, err)
	require.NotNil(t, responseCh)

	// Ensure that the second wait for response return immediately
	responseCh2, err := registration.AwaitInitialRegistrationResponse(ctx, subscriberStopCh)
	require.NoError(t, err)
	require.Equal(t, responseCh, responseCh2)
}
