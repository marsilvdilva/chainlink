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
	// This is required to ensure registration calls return if the remote node does not support the trigger capability
	// response protocol.  Once all nodes support the trigger capability response protocol, this timeout could be
	// changed to return timeout error instead if we wanted to indicate that registration may have failed.  Current behaviour
	// of the remote triggers is automatically resubscribe so the registration response is only to indicate success/failure of the registration
	// request itself so that user registration errors (i.e. invalid arguments) can be handled appropriately.
	registrationResponseTimeout = 10 * time.Second

	sendChannelBufferSize = 1000
)

type TriggerRegistrationKey struct {
	triggerID string
}

type SubscriberRegistration struct {
	lggr       logger.Logger
	callback   chan commoncap.TriggerResponse
	rawRequest []byte

	registrationResponseCache *messagecache.MessageCache[TriggerRegistrationKey, p2ptypes.PeerID]

	mu                                   sync.Mutex
	isInitialRegistrationResponseAwaited bool

	// TODO need to clean this up after initial registration response is sent and ensure following sent responses are effectively noops and do not block
	initialRegistrationResponseChan chan error
}

func NewSubscriberRegistration(lggr logger.Logger, rawRequest []byte,
	registrationResponseCache *messagecache.MessageCache[TriggerRegistrationKey, p2ptypes.PeerID]) *SubscriberRegistration {
	return &SubscriberRegistration{
		lggr:                            lggr,
		callback:                        make(chan commoncap.TriggerResponse, sendChannelBufferSize),
		rawRequest:                      rawRequest,
		registrationResponseCache:       registrationResponseCache,
		initialRegistrationResponseChan: make(chan error, 1),
	}
}

func (sr *SubscriberRegistration) HandleTriggerRegistrationResponse(sender p2ptypes.PeerID, msg *types.MessageBody, minResponseToAggregate uint32,
	messageExpiryMilliseconds int64, capDonF uint8) {
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

	// TODO check min responses to aggregate, is it 2f+1, will it always be greater that cap don f +1 ?  (why is f number not being used here, or is it effectively used to define min reponses?)
	ready, registrationResponses := sr.registrationResponseCache.Ready(key, minResponseToAggregate, nowMs-messageExpiryMilliseconds, true)

	if ready {
		var successfulRegistrationCount uint8
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

		if successfulRegistrationCount >= (capDonF + 1) {
			// Successful registration
			sr.lggr.Infow("successful trigger registration", "triggerID", meta.TriggerId, "sender", sender)

			select {
			case sr.initialRegistrationResponseChan <- nil:
				// sending nil error to indicate success
			default:
				// channel is closed or response has been sent already
			}
		} else {
			// Registration failed - send error response

			// Is there a consensus error?  if so send that
			for errStr, count := range errorToCount {
				if count >= int(capDonF+1) {
					sr.lggr.Errorw("trigger registration failed with error", "triggerID", meta.TriggerId, "sender", sender, "error", log.SanitizeLogString(errStr), "count", count)
					select {
					case sr.initialRegistrationResponseChan <- errors.New(errStr):
					default:
						// channel is closed or response has been sent already
					}
					return
				}
			}

			// If there is no consensus error, return a generic error
			sr.initialRegistrationResponseChan <- fmt.Errorf("received %d errors, last error %s : %s", totalErrorCount, msg.Error, log.SanitizeLogString(msg.ErrorMsg))
		}
	}
}

func (sr *SubscriberRegistration) AwaitInitialRegistrationResponse(ctx context.Context, subscriberStopCh chan struct{}) (<-chan commoncap.TriggerResponse, error) {

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

	ctxWithTimeout, cancel := context.WithTimeout(ctx, registrationResponseTimeout)
	defer cancel()

	select {
	case <-subscriberStopCh:
		return nil, errors.New("trigger subscriber is stopping")
	case <-ctxWithTimeout.Done():
		return nil, ctx.Err()
	case err := <-sr.initialRegistrationResponseChan:
		if err != nil {
			return nil, err
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
