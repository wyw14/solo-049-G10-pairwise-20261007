package recovery

import (
	"context"
	"fmt"
	"time"

	"orbitlink/internal/antenna"
	passsvc "orbitlink/internal/pass"
	"orbitlink/internal/recorder"
	"orbitlink/internal/scheduler"
)

// drill carries the sandbox environment and the restart-time view of the
// recording segments through the domain replays.
type drill struct {
	ctx             context.Context
	env             *sandboxEnv
	now             time.Time
	initialSegments map[string]recorder.Segment
}

func newDrill(ctx context.Context, env *sandboxEnv) *drill {
	d := &drill{ctx: ctx, env: env, now: env.clock.Now()}
	d.initialSegments = make(map[string]recorder.Segment)
	for _, segment := range env.recorder.List() {
		d.initialSegments[segment.ID] = segment
	}
	return d
}

func (d *drill) passIndex() map[string]passsvc.Pass {
	index := make(map[string]passsvc.Pass)
	for _, value := range d.env.passes.List() {
		index[value.ID] = value
	}
	return index
}

func (d *drill) findReservation(id string) (scheduler.Interval, bool) {
	for _, interval := range d.env.timeline.List("") {
		if interval.ReservationID == id {
			return interval, true
		}
	}
	return scheduler.Interval{}, false
}

func (d *drill) segmentByID(id string) (recorder.Segment, bool) {
	for _, segment := range d.env.recorder.List() {
		if segment.ID == id {
			return segment, true
		}
	}
	return recorder.Segment{}, false
}

// simulate runs a recovery step inside the sandbox and records the outcome.
func simulate(domain *DomainReplay, subject, verb, detail string, step func() error) {
	action := Action{Subject: subject, Verb: verb, Detail: detail}
	if err := step(); err != nil {
		action.Outcome = err.Error()
	} else {
		action.Simulated = true
	}
	domain.Actions = append(domain.Actions, action)
}

func risk(domain *DomainReplay, severity, subject, detail string) {
	domain.Risks = append(domain.Risks, Risk{Subject: subject, Severity: severity, Detail: detail})
}

// replayPasses rebuilds the pass phases and rehearses per-phase recovery.
func (d *drill) replayPasses() DomainReplay {
	domain := DomainReplay{Domain: "passes", Component: "passes"}
	passes := sortedPasses(d.env.passes.List())
	domain.Recovered = len(passes)
	for _, value := range passes {
		domain.Entities = append(domain.Entities, EntityReplay{
			ID: value.ID, State: string(value.Phase),
			Detail: fmt.Sprintf("satellite %s on %s, LOS %s", value.SatelliteID, value.AntennaID, value.Window.LOS.UTC().Format("15:04:05Z")),
		})
		d.recoverPass(&domain, value)
	}
	return domain
}

func (d *drill) recoverPass(domain *DomainReplay, value passsvc.Pass) {
	switch value.Phase {
	case passsvc.Planned:
		d.recoverPlanned(domain, value)
	case passsvc.Acquiring:
		d.recoverAcquiring(domain, value)
	case passsvc.Tracking:
		d.recoverTracking(domain, value)
	case passsvc.Draining:
		d.recoverDraining(domain, value)
	}
}

func (d *drill) recoverPlanned(domain *DomainReplay, value passsvc.Pass) {
	if !value.Window.LOS.After(d.now) {
		risk(domain, RiskWarning, value.ID, "pass window expired while the station was down")
		simulate(domain, value.ID, "mark-failed", "close out the missed window", func() error {
			return d.env.passes.Fail(value.ID, "pass window expired during station outage")
		})
		return
	}
	if _, found := d.env.windows.Get(value.ID); !found {
		risk(domain, RiskWarning, value.ID, "pass window record is missing from the window store")
		simulate(domain, value.ID, "recompute-window", "republish the revision-bound window", func() error {
			return d.env.recompute.Apply(value.ID, value.Window)
		})
	}
	queued := false
	for _, command := range d.env.commands.List(value.ID) {
		if !command.Retired {
			queued = true
			break
		}
	}
	if !queued {
		simulate(domain, value.ID, "requeue-pointing", "rebuild the pointing command queue for the window", func() error {
			return d.env.replanner.QueueInitial(value.ID)
		})
	}
	d.ensureReservation(domain, value)
}

func (d *drill) recoverAcquiring(domain *DomainReplay, value passsvc.Pass) {
	if value.CurrentAttemptID != "" {
		if _, found := d.env.attempts.Current(value.ID); !found {
			risk(domain, RiskWarning, value.ID, "in-flight receiver attempt was lost with the process")
		}
	}
	risk(domain, RiskWarning, value.ID, "acquisition was interrupted; late results for the pre-restart attempt are discarded as stale")
	simulate(domain, value.ID, "restart-acquisition", "dispatch a fresh receiver attempt", func() error {
		_, err := d.env.attempts.Begin(value.ID, firstChain(value))
		return err
	})
	d.ensureReservation(domain, value)
}

func (d *drill) recoverTracking(domain *DomainReplay, value passsvc.Pass) {
	d.ensureReservation(domain, value)
	d.checkAntennaOwnership(domain, value)
	if value.RecordingSegmentID == "" {
		risk(domain, RiskCritical, value.ID, "tracking pass has no recording segment")
		return
	}
	initial, found := d.initialSegments[value.RecordingSegmentID]
	if !found {
		risk(domain, RiskCritical, value.ID, fmt.Sprintf("recording segment %s is missing from the recorder snapshot", value.RecordingSegmentID))
		return
	}
	if initial.FramesBuffered > 0 {
		risk(domain, RiskWarning, value.ID, fmt.Sprintf("%d buffered frames were memory-only and are lost", initial.FramesBuffered))
	}
	if !value.Window.LOS.After(d.now) {
		simulate(domain, value.ID, "finalize-pass", "LOS passed during the outage; unlock, drain and seal", func() error {
			_, err := d.env.finalizer.Finalize(d.ctx, value.ID, firstChain(value))
			return err
		})
		return
	}
	simulate(domain, value.ID, "resume-tracking", "rebuild the decoder session and rearm the deadline from civil LOS", func() error {
		segment, found := d.segmentByID(value.RecordingSegmentID)
		if !found || (segment.Status != recorder.Open && segment.Status != recorder.Draining) {
			return fmt.Errorf("recording segment %s is not recoverable", value.RecordingSegmentID)
		}
		return nil
	})
}

func (d *drill) recoverDraining(domain *DomainReplay, value passsvc.Pass) {
	if value.RecordingSegmentID == "" {
		risk(domain, RiskCritical, value.ID, "draining pass has no recording segment to finish")
		return
	}
	simulate(domain, value.ID, "resume-drain-seal", "finish the interrupted drain and seal the segment", func() error {
		_, err := d.env.finalizer.Finalize(d.ctx, value.ID, firstChain(value))
		return err
	})
}

// ensureReservation rehearses re-reserving a pass whose timeline interval did
// not survive, and flags dangling reservation references.
func (d *drill) ensureReservation(domain *DomainReplay, value passsvc.Pass) {
	interval, found := d.findReservation(value.ReservationID)
	if value.ReservationID != "" && found && interval.Status != scheduler.Released {
		return
	}
	if value.ReservationID != "" {
		risk(domain, RiskWarning, value.ID, fmt.Sprintf("reservation %s is missing from the antenna timeline", value.ReservationID))
	}
	simulate(domain, value.ID, "reserve-antenna", "recreate the antenna reservation from the persisted window", func() error {
		interval, err := d.env.reservations.ReservePass(value.ID, value.AntennaID)
		if err != nil {
			return err
		}
		return d.env.passes.SetReservation(value.ID, interval.ReservationID, interval.Generation)
	})
}

// checkAntennaOwnership flags active passes that no longer own their antenna
// and rehearses reclaiming it through the persisted reservation.
func (d *drill) checkAntennaOwnership(domain *DomainReplay, value passsvc.Pass) {
	state, found := d.env.fleet.State(value.AntennaID)
	if !found {
		risk(domain, RiskWarning, value.ID, fmt.Sprintf("antenna %s has no persisted state", value.AntennaID))
		return
	}
	if state.OwnerPassID == value.ID {
		return
	}
	risk(domain, RiskWarning, value.ID, fmt.Sprintf("pass does not own antenna %s after the restart", value.AntennaID))
	interval, found := d.findReservation(value.ReservationID)
	if !found {
		return
	}
	simulate(domain, value.ID, "claim-antenna", "reclaim the antenna through the persisted reservation", func() error {
		_, err := d.env.fleet.Claim(interval)
		return err
	})
}

// replayReservations rebuilds the antenna timeline and rehearses cleanup of
// intervals whose passes are gone or terminal.
func (d *drill) replayReservations() DomainReplay {
	domain := &DomainReplay{Domain: "reservations", Component: "antenna-timeline"}
	passes := d.passIndex()
	intervals := d.env.timeline.List("")
	domain.Recovered = len(intervals)
	byAntenna := make(map[string][]scheduler.Interval)
	for _, interval := range intervals {
		domain.Entities = append(domain.Entities, EntityReplay{
			ID: interval.ReservationID, State: string(interval.Status),
			Detail: fmt.Sprintf("pass %s on %s until %s", interval.PassID, interval.AntennaID, interval.End.UTC().Format("15:04:05Z")),
		})
		if interval.Status == scheduler.Released {
			continue
		}
		byAntenna[interval.AntennaID] = append(byAntenna[interval.AntennaID], interval)
		value, found := passes[interval.PassID]
		switch {
		case !found:
			risk(domain, RiskWarning, interval.ReservationID, "reservation references an unknown pass")
			simulate(domain, interval.ReservationID, "release-reservation", "free the antenna from the dangling interval", func() error {
				_, err := d.env.timeline.Release(interval.ReservationID)
				return err
			})
		case terminal(value.Phase):
			simulate(domain, interval.ReservationID, "release-reservation", "owning pass is terminal; free the antenna", func() error {
				_, err := d.env.timeline.Release(interval.ReservationID)
				return err
			})
		case !interval.End.After(d.now):
			risk(domain, RiskWarning, interval.ReservationID, "reservation expired while the pass is still active")
		}
	}
	for antennaID, list := range byAntenna {
		for index := 1; index < len(list); index++ {
			if list[index].Start.Before(list[index-1].End) {
				risk(domain, RiskCritical, antennaID, "overlapping active reservations survived the restart")
			}
		}
	}
	return *domain
}

// replayAntennas rebuilds fleet ownership and rehearses releasing ownership
// held by passes that no longer exist or already finished.
func (d *drill) replayAntennas() DomainReplay {
	domain := &DomainReplay{Domain: "antennas", Component: "antennas"}
	passes := d.passIndex()
	states := sortedAntennas(d.env.fleet.List())
	domain.Recovered = len(states)
	for _, state := range states {
		detail := fmt.Sprintf("az %.1f el %.1f", state.Azimuth, state.Elevation)
		if state.OwnerPassID != "" {
			detail = fmt.Sprintf("owned by pass %s, %s", state.OwnerPassID, detail)
		}
		domain.Entities = append(domain.Entities, EntityReplay{ID: state.ID, State: string(state.Mode), Detail: detail})
		if state.OwnerPassID != "" {
			value, found := passes[state.OwnerPassID]
			switch {
			case !found:
				risk(domain, RiskWarning, state.ID, fmt.Sprintf("owner pass %s is unknown", state.OwnerPassID))
				simulate(domain, state.ID, "release-ownership", "drop ownership left by the vanished pass", func() error {
					return d.env.fleet.Release(state.ID, state.OwnerPassID, state.OwnerGeneration)
				})
			case terminal(value.Phase):
				simulate(domain, state.ID, "release-ownership", "owning pass is terminal; return the antenna to idle", func() error {
					return d.env.fleet.Release(state.ID, state.OwnerPassID, state.OwnerGeneration)
				})
			}
		}
		if state.Mode == antenna.Fault {
			risk(domain, RiskWarning, state.ID, fmt.Sprintf("antenna is in fault: %s", state.FaultReason))
		}
		if state.Mode == antenna.Slewing || state.Mode == antenna.Stowing {
			risk(domain, RiskWarning, state.ID, "motion state is uncertain after the restart; re-home before use")
		}
	}
	return *domain
}

// replayRFChains rebuilds chain state and rehearses inhibiting chains that
// come back with transmit enabled.
func (d *drill) replayRFChains() DomainReplay {
	domain := &DomainReplay{Domain: "rf-chains", Component: "rf-chains"}
	passes := d.passIndex()
	chains := sortedChains(d.env.rf.List())
	domain.Recovered = len(chains)
	for _, chain := range chains {
		state := "receive"
		if chain.TransmitEnabled {
			state = "transmit-enabled"
		}
		domain.Entities = append(domain.Entities, EntityReplay{
			ID: chain.ChainID, State: state,
			Detail: fmt.Sprintf("pass %s center %d Hz", noneIfEmpty(chain.PassID), chain.CenterHz),
		})
		if chain.TransmitEnabled {
			risk(domain, RiskCritical, chain.ChainID, "transmit is enabled across the restart; inhibit before any further RF work")
			simulate(domain, chain.ChainID, "inhibit-transmit", "force the chain back to receive-safe", func() error {
				_, err := d.env.rf.Inhibit(d.ctx, chain.ChainID, "recovery-drill")
				return err
			})
		}
		if chain.PassID != "" {
			value, found := passes[chain.PassID]
			switch {
			case !found:
				risk(domain, RiskWarning, chain.ChainID, fmt.Sprintf("frequency plan references unknown pass %s", chain.PassID))
			case terminal(value.Phase):
				risk(domain, RiskWarning, chain.ChainID, fmt.Sprintf("frequency plan is stale; pass %s is terminal", chain.PassID))
			}
		}
	}
	return *domain
}

// replayRecordings rebuilds segment state. Entities and lost-buffer risks
// come from the restart-time snapshot; actions run after pass recovery so a
// segment the pass domain already sealed is not sealed twice.
func (d *drill) replayRecordings() DomainReplay {
	domain := &DomainReplay{Domain: "recordings", Component: "recording-sessions"}
	initial := make([]recorder.Segment, 0, len(d.initialSegments))
	for _, segment := range d.initialSegments {
		initial = append(initial, segment)
	}
	domain.Recovered = len(initial)
	for _, segment := range sortedSegments(initial) {
		domain.Entities = append(domain.Entities, EntityReplay{
			ID: segment.ID, State: string(segment.Status),
			Detail: fmt.Sprintf("pass %s, %d written, %d buffered", segment.PassID, segment.FramesWritten, segment.FramesBuffered),
		})
		if segment.FramesBuffered > 0 {
			risk(domain, RiskWarning, segment.ID, fmt.Sprintf("%d buffered frames were memory-only and are lost", segment.FramesBuffered))
		}
	}
	passes := d.passIndex()
	for _, segment := range sortedSegments(d.env.recorder.List()) {
		if segment.Status == recorder.Sealed || segment.Status == recorder.Aborted {
			continue
		}
		value, found := passes[segment.PassID]
		switch {
		case !found:
			risk(domain, RiskWarning, segment.ID, fmt.Sprintf("recording references unknown pass %s", segment.PassID))
			if segment.Status == recorder.Open {
				simulate(domain, segment.ID, "abort-segment", "abort the orphaned open segment", func() error {
					return d.env.recorder.StopForFailure(segment.PassID, "recovery-drill", "owning pass unknown after restart")
				})
			} else {
				simulate(domain, segment.ID, "drain-seal", "drain and seal the orphaned segment", func() error {
					return d.drainSeal(segment.ID)
				})
			}
		case terminal(value.Phase):
			simulate(domain, segment.ID, "drain-seal", "owning pass is terminal; drain and seal", func() error {
				return d.drainSeal(segment.ID)
			})
		}
	}
	return *domain
}

func (d *drill) drainSeal(segmentID string) error {
	if _, err := d.env.recorder.Drain(d.ctx, segmentID); err != nil {
		return err
	}
	_, err := d.env.recorder.Seal(segmentID, "recovery drill seal")
	return err
}

func firstChain(value passsvc.Pass) string {
	if len(value.RequiredReceiverChains) > 0 {
		return value.RequiredReceiverChains[0]
	}
	if len(value.RFChains) > 0 {
		return value.RFChains[0]
	}
	return "RF-01"
}

func noneIfEmpty(value string) string {
	if value == "" {
		return "-"
	}
	return value
}
