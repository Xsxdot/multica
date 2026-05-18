# Progress: Channel agent-turn-only refactor

## Milestone 1 / 5

### Implemented
- Natural-language channel messages no longer fall back to rule intent resolution when the channel agent turn client or workspace context is unavailable.
- Slash/source-command input remains deterministic and continues through the rule command resolver path.
- Added regression coverage proving natural language does not use old rules without channel turn support.

### Approach
- Kept the routing boundary in `inbound.Runtime.resolveIntent`, because this is the single ingress point that decides whether a message is deterministic command input or an agent turn.
- Reused the existing failure-notice path so missing channel-agent support terminates the event without retry spam.

### Plan Delta
- No delta from the plan for this milestone.

### Follow-Up
- The old planner/task types and `intent` package naming still exist. They are scheduled for later milestones.
- Pending clarification/action state is not implemented yet. The screenshot flow still needs that milestone to remember that "STA-82" answers a prior cancellation clarification.

### Next
- Milestone 2: split command/turn/action boundaries and start removing old chat intent planner influence from package/API naming.
