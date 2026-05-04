"use client";

import { useQuery } from "@tanstack/react-query";
import { useWorkspaceId } from "@multica/core/hooks";
import { notificationPreferenceOptions } from "@multica/core/notification-preferences/queries";
import { useUpdateNotificationPreferences } from "@multica/core/notification-preferences/mutations";
import type { ChannelPreferences, NotificationGroupKey, NotificationPreferences } from "@multica/core/types";
import { Card, CardContent } from "@multica/ui/components/ui/card";
import { Switch } from "@multica/ui/components/ui/switch";
import { toast } from "sonner";

const notificationGroups: {
  key: NotificationGroupKey;
  label: string;
  description: string;
}[] = [
  {
    key: "assignments",
    label: "Assignments",
    description: "When you are assigned or unassigned from an issue",
  },
  {
    key: "status_changes",
    label: "Status changes",
    description: "When an issue you follow changes status (e.g. todo, in progress, done)",
  },
  {
    key: "comments",
    label: "Comments & Mentions",
    description: "New comments on issues you follow, or when someone @mentions you",
  },
  {
    key: "updates",
    label: "Priority & Due date",
    description: "When priority or due date changes on issues you follow",
  },
  {
    key: "agent_activity",
    label: "Agent activity",
    description: "When an agent task completes or fails",
  },
];

const feishuNotificationTypes: {
  key: "issues" | "comments" | "mentions";
  label: string;
  description: string;
}[] = [
  {
    key: "issues",
    label: "Issue events",
    description: "New issues, status changes, and assignments in this workspace",
  },
  {
    key: "comments",
    label: "Comments",
    description: "New comments on issues you follow",
  },
  {
    key: "mentions",
    label: "Mentions",
    description: "When someone @mentions you in a Feishu group",
  },
];

export function NotificationsTab() {
  const wsId = useWorkspaceId();
  const { data } = useQuery(notificationPreferenceOptions(wsId));
  const mutation = useUpdateNotificationPreferences();

  const preferences = data?.preferences ?? {};

  const handleToggle = (key: NotificationGroupKey, enabled: boolean) => {
    const updated: NotificationPreferences = {
      ...preferences,
      [key]: enabled ? "all" : "muted",
    };
    // Remove keys set to "all" (default) to keep the object clean
    if (enabled) {
      delete updated[key];
    }
    mutation.mutate(updated, {
      onError: () => toast.error("Failed to update notification settings"),
    });
  };

  const handleFeishuToggle = (key: "issues" | "comments" | "mentions", enabled: boolean) => {
    const currentFeishu = preferences.channel?.feishu ?? {};
    const updatedFeishu = { ...currentFeishu, [key]: enabled };

    // Remove keys set to true (default) to keep the object clean
    if (enabled) {
      delete updatedFeishu[key];
    }

    const updatedChannel: ChannelPreferences = {
      ...preferences.channel,
      ...(Object.keys(updatedFeishu).length > 0 ? { feishu: updatedFeishu } : {}),
    };

    const updated: NotificationPreferences = { ...preferences };
    if (Object.keys(updatedChannel).length === 0) {
      delete updated.channel;
    } else {
      updated.channel = updatedChannel;
    }

    mutation.mutate(updated, {
      onError: () => toast.error("Failed to update Feishu notification settings"),
    });
  };

  const feishuPrefs = preferences.channel?.feishu ?? {};

  return (
    <div className="space-y-6">
      <section className="space-y-4">
        <div>
          <h2 className="text-sm font-semibold">Inbox Notifications</h2>
          <p className="text-sm text-muted-foreground mt-1">
            Control which events generate inbox notifications. Muted event types
            are silently filtered — you can still see them by visiting the issue
            directly.
          </p>
        </div>

        <Card>
          <CardContent className="divide-y">
            {notificationGroups.map((group) => {
              const enabled = preferences[group.key] !== "muted";
              return (
                <div
                  key={group.key}
                  className="flex items-center justify-between py-3 first:pt-0 last:pb-0"
                >
                  <div className="space-y-0.5 pr-4">
                    <p className="text-sm font-medium">{group.label}</p>
                    <p className="text-xs text-muted-foreground">
                      {group.description}
                    </p>
                  </div>
                  <Switch
                    checked={enabled}
                    onCheckedChange={(checked) =>
                      handleToggle(group.key, checked)
                    }
                  />
                </div>
              );
            })}
          </CardContent>
        </Card>
      </section>

      <section className="space-y-4">
        <div>
          <h2 className="text-sm font-semibold">Feishu Notifications</h2>
          <p className="text-sm text-muted-foreground mt-1">
            Control which events are forwarded to Feishu. Disabled types will
            not send messages to your connected Feishu groups.
          </p>
        </div>

        <Card>
          <CardContent className="divide-y">
            {feishuNotificationTypes.map((type) => {
              const enabled = feishuPrefs[type.key] !== false;
              return (
                <div
                  key={type.key}
                  className="flex items-center justify-between py-3 first:pt-0 last:pb-0"
                >
                  <div className="space-y-0.5 pr-4">
                    <p className="text-sm font-medium">{type.label}</p>
                    <p className="text-xs text-muted-foreground">
                      {type.description}
                    </p>
                  </div>
                  <Switch
                    checked={enabled}
                    onCheckedChange={(checked) =>
                      handleFeishuToggle(type.key, checked)
                    }
                  />
                </div>
              );
            })}
          </CardContent>
        </Card>
      </section>
    </div>
  );
}
