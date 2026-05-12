export type NotificationGroupKey =
  | "assignments"
  | "status_changes"
  | "comments"
  | "updates"
  | "agent_activity";

export type NotificationGroupValue = "all" | "muted";

/**
 * Provider channel preferences. Each key represents one event family a
 * channel provider integration can mute.
 *
 * Default semantics: a key absent from this object is treated as
 * **enabled** (default-on). Only an explicit `false` mutes the family.
 * The backend (`server/internal/handler/notification_preference.go`)
 * holds the same contract — see `IsChannelEventEnabled` for the
 * canonical predicate. Use {@link isChannelEventEnabled} below from
 * frontend code rather than re-implementing the rule.
 */
export interface ChannelNotificationPreferences {
  issues?: boolean;
  comments?: boolean;
  mentions?: boolean;
  slash_aliases?: Record<string, string>;
}

export type ChannelEventKey = "issues" | "comments" | "mentions";

export type ChannelPreferences = Record<string, ChannelNotificationPreferences | undefined>;

export type FeishuChannelPreferences = ChannelNotificationPreferences;
export type FeishuEventKey = ChannelEventKey;

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
 * Returns true when a provider integration should deliver an event of
 * the given key for the given preferences. Missing keys mean
 * "enabled" (default-on); explicit false means muted.
 *
 * Centralising this rule keeps the UI and any future frontend consumer
 * aligned with the backend default semantics.
 */
export function isChannelEventEnabled(
  prefs: NotificationPreferences | undefined | null,
  provider: string,
  key: ChannelEventKey,
): boolean {
  const value = prefs?.channel?.[provider]?.[key];
  return value !== false;
}

export function isFeishuEventEnabled(
  prefs: NotificationPreferences | undefined | null,
  key: FeishuEventKey,
): boolean {
  return isChannelEventEnabled(prefs, "feishu", key);
}
