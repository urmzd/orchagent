package runtime

// Record names a run writes to anchor's ledger. A stored run depends on them,
// so they never change. internal/legacy pins them.
const (
	RecordStop           = stopRecordName
	RecordQuestionPrefix = questionStepPrefix
	RecordAnswerPrefix   = answerRecordPrefix
	RecordChildPrefix    = childStepPrefix
	RecordUsagePrefix    = usageRecordName
)
