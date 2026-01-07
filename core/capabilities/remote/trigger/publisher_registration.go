package trigger

import (
	"context"
	"errors"

	commoncap "github.com/smartcontractkit/chainlink-common/pkg/capabilities"
	caperrors "github.com/smartcontractkit/chainlink-common/pkg/capabilities/errors"
	"github.com/smartcontractkit/chainlink-common/pkg/logger"
	"github.com/smartcontractkit/chainlink/v2/core/capabilities/remote/types"

	p2ptypes "github.com/smartcontractkit/chainlink/v2/core/services/p2p/types"
)

type triggerRegistrationRequest struct {
	peerID      p2ptypes.PeerID
	callerDonID uint32
}

type PublisherRegistration struct {
	lggr       logger.Logger
	dispatcher types.Dispatcher

	triggerID       string
	workflowID      string
	capabilityID    string
	capMethodName   string
	capabilityDonID uint32

	underlyingTrigger commoncap.TriggerCapability

	callback                             <-chan commoncap.TriggerResponse
	underlyingTriggerRegistrationRequest commoncap.TriggerRegistrationRequest
	cancel                               context.CancelFunc

	registrationRequests []triggerRegistrationRequest

	// TODO migrate error from string
	registrationResult *types.Error
	errorMessage       string
}

func NewPublisherRegistration(lggr logger.Logger,
	triggerID string,
	workflowID string,
	capabilityDonId uint32, // TODO verify that this not being dynamic is acceptable, given the initial registration is only active for a short period of time this should be ok?
	capabilityID string,
	capMethodName string,
	dispatcher types.Dispatcher) *PublisherRegistration {
	return &PublisherRegistration{
		lggr:            lggr,
		triggerID:       triggerID,
		workflowID:      workflowID,
		capabilityID:    capabilityID,
		capMethodName:   capMethodName,
		capabilityDonID: capabilityDonId,
		dispatcher:      dispatcher,
	}
}

func (rm *PublisherRegistration) AddRequest(peerID p2ptypes.PeerID, callerDonID uint32) {
	rm.registrationRequests = append(rm.registrationRequests, triggerRegistrationRequest{peerID: peerID, callerDonID: callerDonID})

	if rm.registrationResult != nil {
		// registration already completed, send response immediately
		rm.sendTriggerRegistrationResponse(peerID, callerDonID, rm.errorMessage)
	}

}

func (rm *PublisherRegistration) RegisterOnUnderlyingTrigger(ctx context.Context, cancelCtx context.CancelFunc, underlyingTrigger commoncap.TriggerCapability, request commoncap.TriggerRegistrationRequest) (<-chan commoncap.TriggerResponse, error) {

	if rm.registrationResult != nil {
		// Should only ever attempt registration on the underlying trigger once for a given publisher registration instance
		return nil, errors.New("underlying trigger registration already attempted")
	}

	rm.underlyingTrigger = underlyingTrigger
	rm.underlyingTriggerRegistrationRequest = request

	callbackCh, err := underlyingTrigger.RegisterTrigger(ctx, request)
	result := types.Error_OK
	var errMsg string
	if err == nil {
		rm.callback = callbackCh
		rm.cancel = cancelCtx
	} else {
		result = types.Error_INVALID_REQUEST
		cancelCtx()

		errMsg = "failed to register trigger"
		var capError caperrors.Error
		if errors.As(err, &capError) {
			errMsg = capError.SerializeToRemoteString()
		}
	}

	rm.registrationResult = &result
	rm.errorMessage = errMsg

	for _, registrationRequests := range rm.registrationRequests {
		rm.sendTriggerRegistrationResponse(registrationRequests.peerID, registrationRequests.callerDonID, errMsg)
	}

	return callbackCh, err
}

func (rm *PublisherRegistration) UnregisterFromUnderlyingTrigger(ctx context.Context) error {
	if rm.registrationResult != nil && *rm.registrationResult == types.Error_OK {
		rm.cancel()
		return rm.underlyingTrigger.UnregisterTrigger(ctx, rm.underlyingTriggerRegistrationRequest)
	}
	// If registration on underlying trigger was not successful, nothing to cancel
	return nil
}

// sendTriggerRegistrationResponse sends a trigger registration response back to the caller DON with an optional error message
func (rm *PublisherRegistration) sendTriggerRegistrationResponse(peerID p2ptypes.PeerID, callerDonID uint32, errMsg string) {

	// TODO migrate to using an error on the registration metadata instead of error string
	var errMsgPtr *string
	if errMsg != "" {
		errMsgPtr = &errMsg
	}

	registrationResponseMessage := &types.MessageBody{
		CapabilityId:    rm.capabilityID,
		CapabilityDonId: rm.capabilityDonID,
		CallerDonId:     callerDonID,
		Method:          types.RegisterTriggerResponse,
		Metadata: &types.MessageBody_TriggerRegistrationMetadata{
			TriggerRegistrationMetadata: &types.TriggerRegistrationMetadata{
				TriggerId:  rm.triggerID,
				WorkflowId: rm.workflowID,
				Error:      errMsgPtr,
			},
		},
		CapabilityMethod: rm.capMethodName,
	}
	err := rm.dispatcher.Send(peerID, registrationResponseMessage)
	if err != nil {
		rm.lggr.Errorw("failed to send trigger registration response", "peerID", peerID, "err", err)
	}
}
