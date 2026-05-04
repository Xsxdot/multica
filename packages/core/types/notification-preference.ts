export type NotificationGroupKey =
  | "assignments"
  | "status_changes"
  | "comments"
  | "updates"
  | "agent_activity";

export type NotificationGroupValue = "all" | "muted";

/**
 * Feishu channel preferences. Each key represents one event family the
 * Feishu push integration can mute.
 *
 * Default semantics: a key absent from this object is treated as
 * **enabled** (default-on). Only an explicit `false` mutes the family.
 * The backend (`server/internal/handler/notification_preference.go`)
 * holds the same contract — see `IsFeishuEventEnabled` for the
 * canonical predicate. Use {@link isFeishuEventEnabled} below from
 * any frontend code rather than re-implementing the rule.
 */
export interface FeishuChannelPreferences {
  issues?: boolean;
  comments?: boolean;
  mentions?: boolean;
}

export type FeishuEventKey = keyof FeishuChannelPreferences;

export interface ChannelPreferences {
  feishu?: FeishuChannelPreferences;
}

export interface NotificationPreferences {
  assignments?: NotificationGroupValue;
  status_changes?: NotificationGroupValue;
  comments?: NotificationGroupValue;
  updates?: NotificationGroupValue;
  agent_activity?: NotificationGroupValue;
  channel?: ChannelPreferences;
}

export interface NotificationPreferenceResponse {
  workspace_id: string;
  preferences: NotificationPreferences;
}

/**
 * Returns true when the Feishu integration should deliver an event of
 * the given key for the given preferences. Missing keys mean
 * "enabled" (default-on); explicit false means muted.
 *
 * Centralising this rule keeps the UI and any future frontend consumer
 * aligned with the backend default semantics.
 */
export function isFeishuEventEnabled(
  prefs: NotificationPreferences | undefined | null,
  key: FeishuEventKey,
): boolean {
  const value = prefs?.channel?.feishu?.[key];
  return value !== false;
}
