package trigger

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	commoncap "github.com/smartcontractkit/chainlink-common/pkg/capabilities"
	"github.com/smartcontractkit/chainlink-common/pkg/logger"
	"github.com/smartcontractkit/chainlink/v2/core/capabilities/remote/log"
	"github.com/smartcontractkit/chainlink/v2/core/capabilities/remote/messagecache"
	"github.com/smartcontractkit/chainlink/v2/core/capabilities/remote/types"
	p2ptypes "github.com/smartcontractkit/libocr/ragep2p/types"
)

const (
	sendChannelBufferSize = 1000
)

var ErrRegistrationResponseTimeout = errors.New("registration response timeout")

type TriggerRegistrationKey struct {
	triggerID string
}

type SubscriberRegistration struct {
	lggr       logger.Logger
	callback   chan commoncap.TriggerResponse
	rawRequest []byte

	registrationResponseCache   *messagecache.MessageCache[TriggerRegistrationKey, p2ptypes.PeerID]
	registrationResponseTimeout time.Duration

	mu sync.Mutex
	// TODO confirm this flag can be removed if we can ensure that AwaitInitialRegistrationResponse is only called once
	isInitialRegistrationResponseAwaited bool

	initialRegistrationResponseChan chan error
}

func NewSubscriberRegistration(lggr logger.Logger, rawRequest []byte,
	registrationResponseCache *messagecache.MessageCache[TriggerRegistrationKey, p2ptypes.PeerID],
	registrationResponseTimeout time.Duration) *SubscriberRegistration {
	return &SubscriberRegistration{
		lggr:                            lggr,
		callback:                        make(chan commoncap.TriggerResponse, sendChannelBufferSize),
		rawRequest:                      rawRequest,
		registrationResponseCache:       registrationResponseCache,
		initialRegistrationResponseChan: make(chan error, 1),
		registrationResponseTimeout:     registrationResponseTimeout,
	}
}

func (sr *SubscriberRegistration) HandleTriggerRegistrationResponse(sender p2ptypes.PeerID, msg *types.MessageBody, minResponseToAggregate uint32,
	messageExpiryMilliseconds int64, publisherNodeCount int) {
	meta := msg.GetTriggerRegistrationMetadata()
	if meta == nil {
		sr.lggr.Errorw("received message with invalid trigger metadata", "sender", sender)
		return
	}

	key := TriggerRegistrationKey{
		triggerID: meta.TriggerId,
	}

	nowMs := time.Now().UnixMilli()
	if meta.Error != nil {
		sr.registrationResponseCache.Insert(key, sender, nowMs, []byte(*meta.Error))
	} else {
		sr.registrationResponseCache.Insert(key, sender, nowMs, nil)
	}

	ready, registrationResponses := sr.registrationResponseCache.Ready(key, minResponseToAggregate, nowMs-messageExpiryMilliseconds, true)

	if ready {
		var successfulRegistrationCount uint32
		var totalErrorCount int

		// aggregate errors by message
		errorToCount := map[string]int{}
		for _, responseError := range registrationResponses {
			if len(responseError) > 0 {
				errorStr := string(responseError)
				errorToCount[errorStr] = errorToCount[errorStr] + 1
				totalErrorCount++
			} else {
				successfulRegistrationCount++
			}
		}

		if successfulRegistrationCount >= minResponseToAggregate {
			// Successful registration
			sr.lggr.Infow("successful trigger registration", "triggerID", meta.TriggerId, "sender", sender)
			sr.sendInitialRegistrationResponse(nil)
		} else {
			// Registration failed - send error response
			// Is there a consensus error?  if so send that
			lastErr := ""
			for errStr, count := range errorToCount {
				if uint32(count) >= minResponseToAggregate {
					sr.sendInitialRegistrationResponse(errors.New(errStr))
					return
				}

				lastErr = errStr
			}

			// This check on when to give up waiting for minResponseToAggregate identical errors assumes that all received
			// errors so far, and any future that will be received are and will be distinct.  It's the same logic
			// used to aggregate errors for remote executable capabilities.  For the purposes of error handling it is
			// sufficient.
			if totalErrorCount >= publisherNodeCount-int(minResponseToAggregate)+1 {
				// There is no consensus error, return a generic error message
				sr.sendInitialRegistrationResponse(fmt.Errorf("received %d errors, last error %s : %s", totalErrorCount, msg.Error, log.SanitizeLogString(lastErr)))
			}
		}
	}
}

func (sr *SubscriberRegistration) sendInitialRegistrationResponse(err error) {
	select {
	case sr.initialRegistrationResponseChan <- err:
	default:
		// channel is closed or initial registration response has been sent already
	}
}

// AwaitInitialRegistrationResponse waits for the initial registration response to be ready or context cancellation.  If the initial registration response
// is not received within a predefined timeout or the response received is an error then the error is returned, in addition the response channel is always returned.
// The caller should check the error and determine what to do.  For example, if the error is a user capability error then it would be reasonable not to
// retry this error and ensure that the error message is propagated to the workflow.
func (sr *SubscriberRegistration) AwaitInitialRegistrationResponse(ctx context.Context, subscriberStopCh chan struct{}) (<-chan commoncap.TriggerResponse, error) {

	// TODO confirm this should only be called once so check below is probably unnecessary
	sr.mu.Lock()
	if !sr.isInitialRegistrationResponseAwaited {
		sr.isInitialRegistrationResponseAwaited = true
	} else {
		sr.mu.Unlock()
		return sr.callback, nil
	}
	sr.mu.Unlock()

	// Channel is only used once to await response, close after use
	defer close(sr.initialRegistrationResponseChan)

	// 0 timeout indicates that the caller does not wish to wait for the initial registration response
	if sr.registrationResponseTimeout == 0 {
		return sr.callback, nil
	}

	ctxWithTimeout, cancel := context.WithTimeout(ctx, sr.registrationResponseTimeout)
	defer cancel()

	select {
	case <-subscriberStopCh:
		return sr.callback, errors.New("trigger subscriber is stopping")
	case <-ctxWithTimeout.Done():
		return sr.callback, ErrRegistrationResponseTimeout
	case err := <-sr.initialRegistrationResponseChan:
		if err != nil {
			return sr.callback, err
		}
		return sr.callback, nil
	}
}

func (sr *SubscriberRegistration) UpdateRequest(rawRequest []byte) {
	sr.rawRequest = rawRequest
}

func (sr *SubscriberRegistration) GetRawRequest() []byte {
	return sr.rawRequest
}

func (sr *SubscriberRegistration) SendAggregatedEvent(aggregatedEvent commoncap.TriggerResponse) {
	sr.callback <- aggregatedEvent
}

func (sr *SubscriberRegistration) Close() {
	close(sr.callback)
}
