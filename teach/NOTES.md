# Teaching Notes

## User preferences

- Wants state committed to git so study can resume on any machine after `git pull`
- Wants a TODO/progress tracker with statuses: not started → started → complete
- Wants to be able to restart (replay) any lesson that has already been covered
- Wants `mise run` tasks to automate opening lessons and checking progress
- Emphasis on **narrating out loud** — this is the single most important behavior
- Interview is on Data Engineering spike track (not Full Stack)

## Teaching approach

- One lesson = one tightly-scoped skill, completable in one sitting
- Each lesson is an HTML file — open with a single `mise run teach:lesson N` command
- Lessons include in-browser quizzes so feedback is immediate and self-contained
- Always tie the concept back to the interview scoring rubric:
  Computational Thinking · Code Stewardship · AI Stewardship · Resilience
- After each lesson, ask 1–2 verbal "narrate-it-back" prompts — simulate the interview voice

## Session resume protocol

1. `git pull` on the new machine
2. `mise run teach:status` — prints TODO list with statuses
3. `mise run teach:next` — opens the next not-started or started lesson in browser
4. Pick up where you left off; mark lessons complete with `mise run teach:done N`
