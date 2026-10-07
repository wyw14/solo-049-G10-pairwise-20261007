package recovery

import (
	"fmt"

	passsvc "orbitlink/internal/pass"
	"orbitlink/internal/receiver"
	"orbitlink/internal/recorder"
	"orbitlink/internal/scheduler"
)

// replayGlobals checks cross-document integrity that no single pass owns.
func (s *replayState) replayGlobals(idx *index) []Finding {
	var findings []Finding
	passByID := make(map[string]passsvc.Pass, len(s.passes))
	terminal := make(map[string]bool, len(s.passes))
	for _, value := range s.passes {
		passByID[value.ID] = value
		switch value.Phase {
		case passsvc.Completed, passsvc.Failed, passsvc.Canceled:
			terminal[value.ID] = true
		}
	}

	for _, state := range s.antennas {
		if state.OwnerPassID == "" {
			continue
		}
		owner, known := passByID[state.OwnerPassID]
		if !known {
			findings = append(findings, finding(SeverityCritical, "antenna", "owner-pass-missing", state.ID,
				fmt.Sprintf("antenna %s is owned by pass %s which no longer exists in the durable pass snapshot", state.ID, state.OwnerPassID)))
			continue
		}
		if terminal[owner.ID] {
			findings = append(findings, finding(SeverityWarning, "antenna", "owner-terminal", state.ID,
				fmt.Sprintf("antenna %s remains claimed by terminal pass %s (%s)", state.ID, owner.ID, owner.Phase)))
			continue
		}
		if !liveIntervalExists(owner.ID, idx) {
			findings = append(findings, finding(SeverityWarning, "antenna", "claim-without-reservation", state.ID,
				fmt.Sprintf("antenna %s is claimed by active pass %s but the timeline holds no live reservation", state.ID, owner.ID)))
		}
	}

	for _, chain := range s.chains {
		if chain.PassID == "" {
			continue
		}
		if _, known := passByID[chain.PassID]; !known {
			findings = append(findings, finding(SeverityWarning, "rf", "plan-pass-missing", chain.ChainID,
				fmt.Sprintf("RF chain %s is tuned for pass %s which is absent from the durable snapshot", chain.ChainID, chain.PassID)))
		}
	}

	for _, segment := range s.segments {
		if segment.Status == recorder.Open || segment.Status == recorder.Draining {
			if _, known := passByID[segment.PassID]; !known {
				findings = append(findings, finding(SeverityWarning, "recorder", "segment-pass-missing", segment.ID,
					fmt.Sprintf("active segment %s belongs to pass %s which is absent from the durable snapshot", segment.ID, segment.PassID)))
			}
		}
	}

	rev := idx.activeRev
	for _, command := range s.commands {
		if command.Retired || command.Revision != rev[command.PassID] {
			continue
		}
		if _, known := passByID[command.PassID]; !known && !command.ExecuteAt.After(s.now) {
			findings = append(findings, finding(SeverityWarning, "scheduler", "command-pass-missing", command.ID,
				fmt.Sprintf("due pointing command belongs to pass %s which is absent from the durable snapshot", command.PassID)))
		}
	}
	return findings
}

// replayMissing enumerates running-process state that is never persisted. These
// are the components a restarted process cannot rebuild no matter how complete
// the snapshots are.
func (s *replayState) replayMissing(idx *index) []MissingComponent {
	var trackingPasses, lockedPasses []string
	for _, value := range s.passes {
		switch value.Phase {
		case passsvc.Acquiring, passsvc.Tracking, passsvc.Draining, passsvc.Sealed:
			trackingPasses = append(trackingPasses, value.ID)
		}
		if attempt, ok := idx.currentAttempt[value.ID]; ok && attempt.Status == receiver.AttemptLocked {
			lockedPasses = append(lockedPasses, value.ID)
		}
	}
	var bufferedSegments []string
	for _, segment := range s.segments {
		if segment.Status == recorder.Open || segment.Status == recorder.Draining {
			bufferedSegments = append(bufferedSegments, segment.ID)
		}
	}

	missing := []MissingComponent{
		{Component: "deadline-manager",
			Reason:   "DeadlineManager keeps civil and monotonic LOS deadlines in an in-memory map",
			Impact:   "the LOS watchdog is unarmed after restart; civil LOS is rebuildable but the active monotonic deadline is gone",
			Affected: trackingPasses},
		{Component: "receiver-session",
			Reason:   "Runtime.receiverSessions and decoder.Session (carrier lock, current attempt, decoder epoch and symbol assembler) are memory-only",
			Impact:   "carrier lock must be re-established and the decoder epoch rebuilt before frames flow again",
			Affected: lockedPasses},
		{Component: "recorder-frame-buffer",
			Reason:   "recorder.session.buffer holds frames in memory; the snapshot stores segment metadata only",
			Impact:   "frames buffered but not drained before the stop are lost and cannot be counted after restart",
			Affected: bufferedSegments},
		{Component: "handover-coordinator",
			Reason: "HandoverCoordinator sessions live in a map and are never written to a snapshot",
			Impact: "an ownership transfer interrupted mid-flight vanishes; neither side can resume or clean it up by id"},
		{Component: "receiver-unlock-delays",
			Reason: "Unlocker delay configuration is an in-memory map",
			Impact: "configured receiver unlock delays revert to zero on restart, changing finalize timing"},
		{Component: "ephemeris-subscribers",
			Reason: "ephemeris revision listeners (the replanner subscription) are wired in NewRuntime only",
			Impact: "the subscription itself is recreated at boot, but a revision delivered while stopped is not replayed onto already-loaded passes"},
	}
	return missing
}

func (s *replayState) summarize(idx *index, passes []PassRecovery, globals []Finding, missing []MissingComponent) Summary {
	summary := Summary{PassesTotal: len(s.passes), MissingComponents: len(missing)}
	for _, pr := range passes {
		if len(pr.Findings) > 0 {
			summary.PassesAtRisk++
		}
		if pr.Recoverable {
			summary.PassesRecoverable++
		}
	}
	for _, interval := range s.intervals {
		if interval.Status != scheduler.Released {
			summary.ReservationsLive++
		}
	}
	for _, state := range s.antennas {
		if state.OwnerPassID != "" {
			summary.AntennasOwned++
		}
	}
	for _, chain := range s.chains {
		if chain.PassID != "" {
			summary.RFChainsTuned++
		}
	}
	for _, segment := range s.segments {
		if segment.Status == recorder.Open || segment.Status == recorder.Draining {
			summary.RecordingsActive++
		}
	}
	for _, command := range s.commands {
		if command.Retired || command.Revision != idx.activeRev[command.PassID] {
			continue
		}
		if !command.ExecuteAt.After(s.now) {
			summary.CommandsDue++
			if command.ExecuteAt.Before(s.now) {
				summary.CommandsOverdue++
			}
		}
	}
	for _, state := range s.antennas {
		if state.OwnerPassID == "" {
			continue
		}
		for _, value := range s.passes {
			if value.ID == state.OwnerPassID {
				switch value.Phase {
				case passsvc.Completed, passsvc.Failed, passsvc.Canceled:
					summary.StrandedOwners++
				}
			}
		}
	}
	return summary
}
