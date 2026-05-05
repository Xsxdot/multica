package intent

import "regexp"

// issueKey matches workspace-scoped keys like STA-2 (≥2 letter prefix).
const issueKeyPattern = `[A-Z]{2,}-\d+`

type rule struct {
	kind       IntentKind
	confidence float64
	re         *regexp.Regexp
	params     func(sub []string) map[string]string
}

func defaultRules() []rule {
	key := issueKeyPattern
	return []rule{
		// Unsupported — checked before actionable intents so destructive/media
		// verbs cannot be mistaken for softer commands.
		{
			kind: IntentUnsupported, confidence: 1,
			re: regexp.MustCompile(`^删除\s*(?:\[)?(` + key + `)(?:\])?$`),
			params: func(sub []string) map[string]string {
				return map[string]string{"issue_key": sub[1]}
			},
		},
		{
			kind: IntentUnsupported, confidence: 1,
			re: regexp.MustCompile(`^上传.+图(?:给\s*)?(?:\[)?(` + key + `)(?:\])?`),
			params: func(sub []string) map[string]string {
				return map[string]string{"issue_key": sub[1]}
			},
		},
		{
			kind: IntentAddComment, confidence: 1,
			re: regexp.MustCompile(`^在\s*(?:\[)?(` + key + `)(?:\])?\s*上(?:加一条)?评论\s*[:：]\s*(.+)$`),
			params: func(sub []string) map[string]string {
				return map[string]string{"issue_key": sub[1], "comment": sub[2]}
			},
		},
		{
			kind: IntentAddComment, confidence: 1,
			re: regexp.MustCompile(`^(` + key + `)\s*评论\s*[:：]\s*(.+)$`),
			params: func(sub []string) map[string]string {
				return map[string]string{"issue_key": sub[1], "comment": sub[2]}
			},
		},
		{
			kind: IntentSetStatus, confidence: 1,
			re: regexp.MustCompile(`^把\s*(?:\[)?(` + key + `)(?:\])?\s*标成\s*([\w-]+)$`),
			params: func(sub []string) map[string]string {
				return map[string]string{"issue_key": sub[1], "status": sub[2]}
			},
		},
		{
			kind: IntentSetStatus, confidence: 1,
			re: regexp.MustCompile(`^(` + key + `)\s*完成了$`),
			params: func(sub []string) map[string]string {
				return map[string]string{"issue_key": sub[1], "status": "done"}
			},
		},
		{
			kind: IntentSetStatus, confidence: 1,
			re: regexp.MustCompile(`^(` + key + `)\s*改成\s*([\w_]+)$`),
			params: func(sub []string) map[string]string {
				return map[string]string{"issue_key": sub[1], "status": sub[2]}
			},
		},
		// SetAssignee
		{
			kind: IntentSetAssignee, confidence: 1,
			re: regexp.MustCompile(`^把\s*(?:\[)?(` + key + `)(?:\])?\s*指派给\s*@?(.+)$`),
			params: func(sub []string) map[string]string {
				return map[string]string{"issue_key": sub[1], "assignee": sub[2]}
			},
		},
		{
			kind: IntentSetAssignee, confidence: 1,
			re: regexp.MustCompile(`^(` + key + `)\s*指派给\s*@?(.+)$`),
			params: func(sub []string) map[string]string {
				return map[string]string{"issue_key": sub[1], "assignee": sub[2]}
			},
		},
		// SetPriority
		{
			kind: IntentSetPriority, confidence: 1,
			re: regexp.MustCompile(`^把\s*(?:\[)?(` + key + `)(?:\])?\s*改优先级\s*([\w_]+)$`),
			params: func(sub []string) map[string]string {
				return map[string]string{"issue_key": sub[1], "priority": sub[2]}
			},
		},
		{
			kind: IntentSetPriority, confidence: 1,
			re: regexp.MustCompile(`^(` + key + `)\s*改优先级\s*([\w_]+)$`),
			params: func(sub []string) map[string]string {
				return map[string]string{"issue_key": sub[1], "priority": sub[2]}
			},
		},
		// SetLabel (add/remove)
		{
			kind: IntentSetLabel, confidence: 1,
			re: regexp.MustCompile(`^把\s*(?:\[)?(` + key + `)(?:\])?\s*加标签\s*([\w-]+)$`),
			params: func(sub []string) map[string]string {
				return map[string]string{"issue_key": sub[1], "label": sub[2], "op": "add"}
			},
		},
		{
			kind: IntentSetLabel, confidence: 1,
			re: regexp.MustCompile(`^(` + key + `)\s*加标签\s*([\w-]+)$`),
			params: func(sub []string) map[string]string {
				return map[string]string{"issue_key": sub[1], "label": sub[2], "op": "add"}
			},
		},
		{
			kind: IntentSetLabel, confidence: 1,
			re: regexp.MustCompile(`^把\s*(?:\[)?(` + key + `)(?:\])?\s*去掉标签\s*([\w-]+)$`),
			params: func(sub []string) map[string]string {
				return map[string]string{"issue_key": sub[1], "label": sub[2], "op": "remove"}
			},
		},
		{
			kind: IntentSetLabel, confidence: 1,
			re: regexp.MustCompile(`^(` + key + `)\s*去掉标签\s*([\w-]+)$`),
			params: func(sub []string) map[string]string {
				return map[string]string{"issue_key": sub[1], "label": sub[2], "op": "remove"}
			},
		},
		{
			kind: IntentQueryIssue, confidence: 1,
			re: regexp.MustCompile(`^(?:\[)?(` + key + `)(?:\])?\s*到哪了[？?]?$`),
			params: func(sub []string) map[string]string {
				return map[string]string{"issue_key": sub[1]}
			},
		},
		{
			kind: IntentQueryIssue, confidence: 1,
			re: regexp.MustCompile(`^(` + key + `)\s*现在状态$`),
			params: func(sub []string) map[string]string {
				return map[string]string{"issue_key": sub[1]}
			},
		},
		{
			kind: IntentQueryIssue, confidence: 1,
			re: regexp.MustCompile(`^我的待办$`),
			params: func(_ []string) map[string]string {
				return map[string]string{}
			},
		},
		{
			kind: IntentCreateIssue, confidence: 1,
			re: regexp.MustCompile(`^帮我记一个\s+(.+)$`),
			params: func(sub []string) map[string]string {
				return map[string]string{"title": sub[1]}
			},
		},
		{
			kind: IntentCreateIssue, confidence: 1,
			re: regexp.MustCompile(`(?i)^创建一个\s*Issue\s*[:：]\s*(.+)$`),
			params: func(sub []string) map[string]string {
				return map[string]string{"title": sub[1]}
			},
		},
		{
			kind: IntentUnknown, confidence: 1,
			re: regexp.MustCompile(`(?i)^(?:在么|在吗|你好|您好|hi|hello)(?:啊)?[？?!！.]?$`),
			params: func(_ []string) map[string]string {
				return map[string]string{}
			},
		},
	}
}
