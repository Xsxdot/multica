package intent

import (
	"regexp"
	"strings"
)

// issueKey matches workspace-scoped keys like STA-2 (≥2 letter prefix).
const issueKeyPattern = `[A-Za-z]{2,}-\d+`

func keyParam(raw string) string {
	return strings.ToUpper(strings.TrimSpace(raw))
}

type rule struct {
	kind       IntentKind
	confidence float64
	re         *regexp.Regexp
	params     func(sub []string) map[string]string
}

func defaultRules() []rule {
	key := issueKeyPattern
	return []rule{
		{
			kind: IntentConfirmAction, confidence: 1,
			re: regexp.MustCompile(`^确认操作\s*([A-Za-z0-9]{4,16})$`),
			params: func(sub []string) map[string]string {
				return map[string]string{"code": strings.ToUpper(sub[1])}
			},
		},
		{
			kind: IntentCancelAction, confidence: 1,
			re: regexp.MustCompile(`^取消操作\s*([A-Za-z0-9]{4,16})$`),
			params: func(sub []string) map[string]string {
				return map[string]string{"code": strings.ToUpper(sub[1])}
			},
		},
		{
			kind: IntentIssueDetail, confidence: 1,
			re: regexp.MustCompile(`^查看详情\s*(?:\[)?(` + key + `)(?:\])?$`),
			params: func(sub []string) map[string]string {
				return map[string]string{"issue_key": keyParam(sub[1])}
			},
		},
		{
			kind: IntentIssueTimeline, confidence: 1,
			re: regexp.MustCompile(`^查看动态\s*(?:\[)?(` + key + `)(?:\])?(?:\s+([1-9][0-9]*))?$`),
			params: func(sub []string) map[string]string {
				page := "1"
				if len(sub) > 2 && sub[2] != "" {
					page = sub[2]
				}
				return map[string]string{"issue_key": keyParam(sub[1]), "page": page}
			},
		},
		{
			kind: IntentIssueLogs, confidence: 1,
			re: regexp.MustCompile(`^查看日志\s*(?:\[)?(` + key + `)(?:\])?(?:\s+([1-9][0-9]*))?$`),
			params: func(sub []string) map[string]string {
				page := "1"
				if len(sub) > 2 && sub[2] != "" {
					page = sub[2]
				}
				return map[string]string{"issue_key": keyParam(sub[1]), "page": page}
			},
		},
		// Unsupported — checked before actionable intents so destructive/media
		// verbs cannot be mistaken for softer commands.
		{
			kind: IntentUnsupported, confidence: 1,
			re: regexp.MustCompile(`^删除\s*(?:\[)?(` + key + `)(?:\])?$`),
			params: func(sub []string) map[string]string {
				return map[string]string{"issue_key": keyParam(sub[1])}
			},
		},
		{
			kind: IntentUnsupported, confidence: 1,
			re: regexp.MustCompile(`^上传.+图(?:给\s*)?(?:\[)?(` + key + `)(?:\])?`),
			params: func(sub []string) map[string]string {
				return map[string]string{"issue_key": keyParam(sub[1])}
			},
		},
		{
			kind: IntentAddComment, confidence: 1,
			re: regexp.MustCompile(`^在\s*(?:\[)?(` + key + `)(?:\])?\s*上(?:加一条)?评论\s*[:：]\s*(.+)$`),
			params: func(sub []string) map[string]string {
				return map[string]string{"issue_key": keyParam(sub[1]), "comment": sub[2]}
			},
		},
		{
			kind: IntentAddComment, confidence: 1,
			re: regexp.MustCompile(`^(` + key + `)\s*评论\s*[:：]\s*(.+)$`),
			params: func(sub []string) map[string]string {
				return map[string]string{"issue_key": keyParam(sub[1]), "comment": sub[2]}
			},
		},
		{
			kind: IntentSetStatus, confidence: 1,
			re: regexp.MustCompile(`^把\s*(?:\[)?(` + key + `)(?:\])?\s*标成\s*([\w-]+)$`),
			params: func(sub []string) map[string]string {
				return map[string]string{"issue_key": keyParam(sub[1]), "status": sub[2]}
			},
		},
		{
			kind: IntentSetStatus, confidence: 1,
			re: regexp.MustCompile(`^(` + key + `)\s*完成了$`),
			params: func(sub []string) map[string]string {
				return map[string]string{"issue_key": keyParam(sub[1]), "status": "done"}
			},
		},
		{
			kind: IntentSetStatus, confidence: 1,
			re: regexp.MustCompile(`^(` + key + `)\s*改成\s*([\w_]+)$`),
			params: func(sub []string) map[string]string {
				return map[string]string{"issue_key": keyParam(sub[1]), "status": sub[2]}
			},
		},
		// SetAssignee
		{
			kind: IntentSetAssignee, confidence: 1,
			re: regexp.MustCompile(`^把\s*(?:\[)?(` + key + `)(?:\])?\s*指派给\s*@?(.+)$`),
			params: func(sub []string) map[string]string {
				return map[string]string{"issue_key": keyParam(sub[1]), "assignee": sub[2]}
			},
		},
		{
			kind: IntentSetAssignee, confidence: 1,
			re: regexp.MustCompile(`^(` + key + `)\s*指派给\s*@?(.+)$`),
			params: func(sub []string) map[string]string {
				return map[string]string{"issue_key": keyParam(sub[1]), "assignee": sub[2]}
			},
		},
		// SetPriority
		{
			kind: IntentSetPriority, confidence: 1,
			re: regexp.MustCompile(`^把\s*(?:\[)?(` + key + `)(?:\])?\s*改优先级\s*([\w_]+)$`),
			params: func(sub []string) map[string]string {
				return map[string]string{"issue_key": keyParam(sub[1]), "priority": sub[2]}
			},
		},
		{
			kind: IntentSetPriority, confidence: 1,
			re: regexp.MustCompile(`^(` + key + `)\s*改优先级\s*([\w_]+)$`),
			params: func(sub []string) map[string]string {
				return map[string]string{"issue_key": keyParam(sub[1]), "priority": sub[2]}
			},
		},
		// SetLabel (add/remove)
		{
			kind: IntentSetLabel, confidence: 1,
			re: regexp.MustCompile(`^把\s*(?:\[)?(` + key + `)(?:\])?\s*加标签\s*([\w-]+)$`),
			params: func(sub []string) map[string]string {
				return map[string]string{"issue_key": keyParam(sub[1]), "label": sub[2], "op": "add"}
			},
		},
		{
			kind: IntentSetLabel, confidence: 1,
			re: regexp.MustCompile(`^(` + key + `)\s*加标签\s*([\w-]+)$`),
			params: func(sub []string) map[string]string {
				return map[string]string{"issue_key": keyParam(sub[1]), "label": sub[2], "op": "add"}
			},
		},
		{
			kind: IntentSetLabel, confidence: 1,
			re: regexp.MustCompile(`^把\s*(?:\[)?(` + key + `)(?:\])?\s*去掉标签\s*([\w-]+)$`),
			params: func(sub []string) map[string]string {
				return map[string]string{"issue_key": keyParam(sub[1]), "label": sub[2], "op": "remove"}
			},
		},
		{
			kind: IntentSetLabel, confidence: 1,
			re: regexp.MustCompile(`^(` + key + `)\s*去掉标签\s*([\w-]+)$`),
			params: func(sub []string) map[string]string {
				return map[string]string{"issue_key": keyParam(sub[1]), "label": sub[2], "op": "remove"}
			},
		},
		{
			kind: IntentQueryProgress, confidence: 1,
			re: regexp.MustCompile(`^(?:各|所有|全部)?项目(?:的)?(?:进展|情况|状态)(?:怎么样|如何)?[？?]?$`),
			params: func(_ []string) map[string]string {
				return map[string]string{"scope": "projects"}
			},
		},
		{
			kind: IntentQueryProgress, confidence: 1,
			re: regexp.MustCompile(`^(?:\[)?(` + key + `)(?:\])?\s*到哪了[？?]?$`),
			params: func(sub []string) map[string]string {
				return map[string]string{"scope": "issue", "issue_key": keyParam(sub[1])}
			},
		},
		{
			kind: IntentQueryProgress, confidence: 1,
			re: regexp.MustCompile(`^(?:\[)?(` + key + `)(?:\])?\s*(?:这个\s*(?i:issue)\s*)?(?:怎么样了?|什么情况|进展(?:怎么样|如何)?|状态(?:怎么样|如何)?|现在状态)[？?]?$`),
			params: func(sub []string) map[string]string {
				return map[string]string{"scope": "issue", "issue_key": keyParam(sub[1])}
			},
		},
		{
			kind: IntentQueryIssue, confidence: 1,
			re: regexp.MustCompile(`^(?:\[)?(` + key + `)(?:\])?\s*到哪了[？?]?$`),
			params: func(sub []string) map[string]string {
				return map[string]string{"issue_key": keyParam(sub[1])}
			},
		},
		{
			kind: IntentQueryIssue, confidence: 1,
			re: regexp.MustCompile(`^(?:\[)?(` + key + `)(?:\])?\s*(?:这个\s*(?i:issue)\s*)?(?:怎么样了?|什么情况|进展(?:怎么样|如何)?|状态(?:怎么样|如何)?|现在状态)[？?]?$`),
			params: func(sub []string) map[string]string {
				return map[string]string{"issue_key": keyParam(sub[1])}
			},
		},
		{
			kind: IntentQueryIssue, confidence: 1,
			re: regexp.MustCompile(`^(?:我的待办|待办列表|看一下待办|我有哪些待办)$`),
			params: func(_ []string) map[string]string {
				return map[string]string{}
			},
		},
		{
			kind: IntentCreateIssue, confidence: 1,
			re: regexp.MustCompile(`(?i)^创建一个\s*Issue\s*[:：]?\s*(.+?)\s*(?:，|,|\s)+(?:指派给|分配给)\s*@?(.+)$`),
			params: func(sub []string) map[string]string {
				return map[string]string{"title": strings.TrimSpace(sub[1]), "assignee": strings.TrimSpace(sub[2])}
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
