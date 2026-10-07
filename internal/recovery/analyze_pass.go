package recovery

import (
	"fmt"
	"sort"
	"time"

	passsvc "orbitlink/internal/pass"
	"orbitlink/internal/receiver"
	"orbitlink/internal/recorder"
	"orbitlink/internal/scheduler"
)

// replayPasses evaluates one recovered Pass at a time, ordered by AOS so the
// report reads like a timeline.
func (s *replayState) replayPasses(idx *index) []PassRecovery {
	ordered := append([]passsvc.Pass(nil), s.passes...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Window.AOS.Before(ordered[j].Window.AOS) })
	results := make([]PassRecovery, 0, len(ordered))
	for _, value := range ordered {
		results = append(results, s.replayPass(value, idx))
	}
	return results
}

func (s *replayState) replayPass(p passsvc.Pass, idx *index) PassRecovery {
	pr := PassRecovery{PassID: p.ID, SatelliteID: p.SatelliteID, AntennaID: p.AntennaID, Phase: string(p.Phase), Recoverable: true}

	switch p.Phase {
	case passsvc.Planned:
		s.evaluateReservation(&pr, p, idx)
		if s.now.After(p.Window.LOS) {
			pr.Findings = append(pr.Findings, finding(SeverityWarning, "pass", "window-elapsed", p.ID,
				"planned window ended before the pass ever entered acquisition"))
		}
		s.evaluateDueCommands(&pr, p, idx)

	case passsvc.Acquiring:
		s.evaluateReservation(&pr, p, idx)
		s.evaluateDeadline(&pr, p, idx, SeverityInfo)
		s.evaluateReceiver(&pr, p, idx)
		s.evaluateDueCommands(&pr, p, idx)

	case passsvc.Tracking:
		s.evaluateReservation(&pr, p, idx)
		s.evaluateDeadline(&pr, p, idx, SeverityWarning)
		s.evaluateReceiver(&pr, p, idx)
		s.evaluateRF(&pr, p, idx)
		s.evaluateRecording(&pr, p, idx)
		s.evaluateDueCommands(&pr, p, idx)

	case passsvc.Draining, passsvc.Sealed:
		s.evaluateDeadline(&pr, p, idx, SeverityWarning)
		s.evaluateRecording(&pr, p, idx)
		pr.Actions = append(pr.Actions, Action{
			Kind: ActionResumeFinalize, Recoverable: p.RecordingSegmentID != "", Automatic: false,
			Description: "re-drive unlock, drain and seal to advance the stuck pass to completed",
			Requires:    "finalize trigger; the boot path never resumes an in-flight finalize",
		})
		if p.RecordingSegmentID == "" {
			pr.Findings = append(pr.Findings, finding(SeverityCritical, "pass", "finalize-stuck", p.ID,
				"pass is draining/sealing but references no recording segment"))
		}

	case passsvc.Completed, passsvc.Failed, passsvc.Canceled:
		s.evaluateStrandedResources(&pr, p, idx)
		s.evaluateTerminalRecording(&pr, p, idx)
	}

	if hasCritical(pr.Findings) {
		pr.Recoverable = false
	}
	return pr
}

func (s *replayState) evaluateReservation(pr *PassRecovery, p passsvc.Pass, idx *index) {
	antennaState, antennaKnown := idx.antennaByID[p.AntennaID]
	if p.ReservationID != "" {
		interval, known := idx.intervalByID[p.ReservationID]
		switch {
		case !known:
			pr.Findings = append(pr.Findings, finding(SeverityCritical, "scheduler", "reservation-missing", p.ReservationID,
				fmt.Sprintf("pass references reservation %s that is absent from the durable timeline", p.ReservationID)))
			pr.Actions = append(pr.Actions, Action{Kind: ActionRebindReservation, Recoverable: false, Automatic: false,
				Description: "re-reserve the antenna window", Requires: "operator; the timeline interval no longer exists and may now conflict"})
		case interval.Status == scheduler.Released:
			pr.Findings = append(pr.Findings, finding(SeverityWarning, "scheduler", "reservation-released", p.ReservationID,
				"reservation was released while the pass still needs the antenna"))
			pr.Actions = append(pr.Actions, Action{Kind: ActionRebindReservation, Recoverable: false, Automatic: false,
				Description: "re-reserve the antenna window", Requires: "operator; a released interval is not reclaimed at boot"})
		default:
			if !antennaKnown || antennaState.OwnerPassID != p.ID {
				pr.Findings = append(pr.Findings, finding(SeverityWarning, "antenna", "owner-not-reclaimed", p.AntennaID,
					"timeline reservation survives restart but the antenna owner field is not bound to this pass"))
				pr.Actions = append(pr.Actions, Action{Kind: ActionRebindReservation, Recoverable: true, Automatic: false,
					Description: "re-claim antenna ownership against the live reservation", Requires: "reserve/claim call; boot does not re-claim"})
			} else if antennaState.OwnerGeneration != interval.Generation {
				pr.Findings = append(pr.Findings, finding(SeverityWarning, "antenna", "owner-generation-mismatch", p.AntennaID,
					fmt.Sprintf("antenna owner generation %d disagrees with reservation generation %d", antennaState.OwnerGeneration, interval.Generation)))
			}
		}
		return
	}
	if liveIntervalExists(p.ID, idx) {
		pr.Findings = append(pr.Findings, finding(SeverityWarning, "scheduler", "reservation-unlinked", p.ID,
			"a live timeline interval exists for the pass but its reservation id is not recorded on the pass"))
		return
	}
	if s.now.After(p.Window.AOS) {
		pr.Findings = append(pr.Findings, finding(SeverityWarning, "scheduler", "never-reserved", p.ID,
			"pass reached AOS without a durable antenna reservation"))
	}
}

func (s *replayState) evaluateDeadline(pr *PassRecovery, p passsvc.Pass, idx *index, severity Severity) {
	pr.Actions = append(pr.Actions, Action{
		Kind: ActionRebuildDeadline, Recoverable: true, Automatic: false,
		Description: "reconstruct the civil LOS deadline from the pass window and re-arm the watchdog",
		Requires:    "deadline manager re-arm at boot; the deadline map is memory-only",
	})
	pr.Findings = append(pr.Findings, finding(severity, "clock", "deadline-not-durable", p.ID,
		"civil LOS can be rebuilt from the window, but the active monotonic LOS watchdog is lost on restart"))
}

func (s *replayState) evaluateReceiver(pr *PassRecovery, p passsvc.Pass, idx *index) {
	attempt, has := idx.currentAttempt[p.ID]
	if !has {
		pr.Findings = append(pr.Findings, finding(SeverityWarning, "receiver", "attempt-missing", p.ID,
			"no acquisition attempt is recorded; carrier lock cannot be resumed"))
		pr.Actions = append(pr.Actions, Action{Kind: ActionReacquireLock, Recoverable: false, Automatic: false,
			Description: "begin a fresh acquisition attempt", Requires: "operator acquire call and hardware carrier lock"})
		return
	}
	switch attempt.Status {
	case receiver.AttemptPending:
		pr.Findings = append(pr.Findings, finding(SeverityWarning, "receiver", "lock-unresolved", attempt.ID,
			"acquisition attempt was still pending when the process stopped and is not concluded on disk"))
		pr.Actions = append(pr.Actions, Action{Kind: ActionReacquireLock, Recoverable: false, Automatic: false,
			Description: "re-drive acquisition for attempt " + attempt.ID, Requires: "fresh acquire; the in-memory receiver session is gone"})
	case receiver.AttemptLocked:
		if p.Phase == passsvc.Acquiring {
			pr.Findings = append(pr.Findings, finding(SeverityInfo, "receiver", "awaiting-diversity", attempt.ID,
				"one chain locked but diversity never converged; remaining chain results are not durable"))
		}
		pr.Findings = append(pr.Findings, finding(SeverityWarning, "receiver", "lock-memory-only", attempt.ID,
			"carrier lock and decoder epoch live only in memory; the durable attempt cannot restore the live session"))
		pr.Actions = append(pr.Actions,
			Action{Kind: ActionReacquireLock, Recoverable: false, Automatic: false, Description: "re-confirm carrier lock on the receiver", Requires: "hardware re-lock; receiver.Session is not persisted"},
			Action{Kind: ActionReopenDecoder, Recoverable: true, Automatic: false, Description: "rebind a decoder session to segment " + p.RecordingSegmentID, Requires: "decoder rebuild; symbol assembler and lost-symbol count are memory-only"})
	default:
		pr.Findings = append(pr.Findings, finding(SeverityWarning, "receiver", "attempt-failed", attempt.ID,
			"latest acquisition attempt is failed; the pass needs a new attempt before tracking"))
		pr.Actions = append(pr.Actions, Action{Kind: ActionReacquireLock, Recoverable: false, Automatic: false, Description: "begin a replacement acquisition attempt", Requires: "operator acquire call"})
	}
}

func (s *replayState) evaluateRF(pr *PassRecovery, p passsvc.Pass, idx *index) {
	for _, chainID := range p.RFChains {
		chain, known := idx.chainByID[chainID]
		if !known {
			pr.Findings = append(pr.Findings, finding(SeverityCritical, "rf", "chain-missing", chainID,
				fmt.Sprintf("pass requires RF chain %s but it is absent from the durable snapshot", chainID)))
			continue
		}
		if chain.PassID != p.ID {
			pr.Findings = append(pr.Findings, finding(SeverityWarning, "rf", "plan-not-bound", chainID,
				fmt.Sprintf("RF chain %s is not tuned for the pass on reload (bound to %q); center/doppler must be re-applied", chainID, chain.PassID)))
			pr.Actions = append(pr.Actions, Action{Kind: ActionReassertRFPlan, Recoverable: false, Automatic: false,
				Description: "re-apply the frequency plan to chain " + chainID, Requires: "operator; the original center/doppler plan is not stored on the pass"})
		}
	}
}

func (s *replayState) evaluateRecording(pr *PassRecovery, p passsvc.Pass, idx *index) {
	if p.RecordingSegmentID == "" {
		pr.Findings = append(pr.Findings, finding(SeverityCritical, "recorder", "segment-unbound", p.ID,
			"active pass has no recording segment recorded on disk"))
		return
	}
	segment, known := idx.segmentByID[p.RecordingSegmentID]
	if !known {
		pr.Findings = append(pr.Findings, finding(SeverityCritical, "recorder", "segment-missing", p.RecordingSegmentID,
			"pass references a recording segment that is absent from the durable snapshot"))
		return
	}
	switch segment.Status {
	case recorder.Open, recorder.Draining:
		pr.Actions = append(pr.Actions, Action{Kind: ActionResumeDrain, Recoverable: true, Automatic: false,
			Description: fmt.Sprintf("re-open segment %s and continue draining buffered frames to disk", segment.ID),
			Requires:    "drain trigger; the in-memory frame buffer and drain progress are not persisted"})
		pr.Findings = append(pr.Findings, finding(SeverityWarning, "recorder", "buffered-frames-lost", segment.ID,
			"frames buffered in memory but never drained are not durable and are lost on restart; the snapshot cannot report their count"))
	case recorder.Sealed, recorder.Aborted:
		pr.Findings = append(pr.Findings, finding(SeverityCritical, "recorder", "segment-closed-while-active", segment.ID,
			fmt.Sprintf("pass is %s but its recording segment is already %s", p.Phase, segment.Status)))
	}
}

func (s *replayState) evaluateDueCommands(pr *PassRecovery, p passsvc.Pass, idx *index) {
	due := s.dueCommands(p.ID, idx)
	if len(due) == 0 {
		return
	}
	pr.Actions = append(pr.Actions, Action{Kind: ActionReplayDueCommands, Recoverable: true, Automatic: false,
		Description: fmt.Sprintf("execute %d due pointing command(s)", len(due)), Requires: "execute-due trigger; nothing runs the queue at boot"})
	if s.now.After(p.Window.AOS) {
		pr.Findings = append(pr.Findings, finding(SeverityWarning, "antenna", "due-commands-bursted", p.AntennaID,
			fmt.Sprintf("%d pointing command(s) became due during downtime; RunDue fires them back-to-back and the antenna jumps to the last position", len(due))))
	} else {
		pr.Findings = append(pr.Findings, finding(SeverityInfo, "scheduler", "commands-armed", p.ID,
			fmt.Sprintf("%d pointing command(s) are queued and await their execute time", len(due))))
	}
}

func (s *replayState) evaluateStrandedResources(pr *PassRecovery, p passsvc.Pass, idx *index) {
	if antennaState, ok := idx.antennaByID[p.AntennaID]; ok && antennaState.OwnerPassID == p.ID {
		pr.Findings = append(pr.Findings, finding(SeverityWarning, "antenna", "owner-stranded", p.AntennaID,
			fmt.Sprintf("terminal pass %s still owns the antenna; finalize/cancel/fail never releases it", p.ID)))
		pr.Actions = append(pr.Actions, Action{Kind: ActionReleaseResources, Recoverable: true, Automatic: false,
			Description: "release the antenna owner and reservation held by the terminal pass",
			Requires:    "operator or a reaper; no terminal path calls timeline.Release or fleet.Release"})
	}
	for _, interval := range idx.intervalsByPass[p.ID] {
		if interval.Status != scheduler.Released {
			pr.Findings = append(pr.Findings, finding(SeverityWarning, "scheduler", "reservation-stranded", interval.ReservationID,
				fmt.Sprintf("terminal pass %s still holds a live reservation until %s", p.ID, interval.End.UTC().Format(time.RFC3339))))
		}
	}
}

func (s *replayState) evaluateTerminalRecording(pr *PassRecovery, p passsvc.Pass, idx *index) {
	if p.RecordingSegmentID == "" {
		return
	}
	segment, known := idx.segmentByID[p.RecordingSegmentID]
	if !known {
		pr.Findings = append(pr.Findings, finding(SeverityWarning, "recorder", "segment-missing", p.RecordingSegmentID,
			"terminal pass references a segment absent from the durable snapshot"))
		return
	}
	if p.Phase == passsvc.Completed && segment.Status != recorder.Sealed {
		pr.Findings = append(pr.Findings, finding(SeverityWarning, "recorder", "completed-not-sealed", segment.ID,
			fmt.Sprintf("pass is completed but its segment is %s rather than sealed", segment.Status)))
	}
	if segment.Status == recorder.Open || segment.Status == recorder.Draining {
		pr.Findings = append(pr.Findings, finding(SeverityWarning, "recorder", "segment-left-open", segment.ID,
			fmt.Sprintf("terminal pass leaves an %s recording segment that still accepts frames", segment.Status)))
		pr.Actions = append(pr.Actions, Action{Kind: ActionResumeDrain, Recoverable: true, Automatic: false,
			Description: "drain and seal the abandoned segment " + segment.ID, Requires: "operator drain/seal"})
	}
}
