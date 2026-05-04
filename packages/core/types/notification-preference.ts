export type NotificationGroupKey =
  | "assignments"
  | "status_changes"
  | "comments"
  | "updates"
  | "agent_activity";

export type NotificationGroupValue = "all" | "muted";

export interface FeishuChannelPreferences {
  issues?: boolean;
  comments?: boolean;
  mentions?: boolean;
}

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
