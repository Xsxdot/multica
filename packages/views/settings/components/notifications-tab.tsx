"use client";

import { useQuery } from "@tanstack/react-query";
import { useWorkspaceId } from "@multica/core/hooks";
import { notificationPreferenceOptions } from "@multica/core/notification-preferences/queries";
import { useUpdateNotificationPreferences } from "@multica/core/notification-preferences/mutations";
import { channelConnectionListOptions } from "@multica/core/workspace/queries";
import type {
  ChannelEventKey,
  ChannelPreferences,
  NotificationGroupKey,
  NotificationPreferences,
} from "@multica/core/types";
import { isChannelEventEnabled } from "@multica/core/types";
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

const channelNotificationTypes: {
  key: ChannelEventKey;
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
    description: "When someone @mentions you in a connected conversation",
  },
];

function providerLabel(value: string) {
  if (!value) return "Channel";
  return value
    .split(/[-_]/)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

export function NotificationsTab() {
  const wsId = useWorkspaceId();
  const { data } = useQuery(notificationPreferenceOptions(wsId));
  const { data: connectionsData } = useQuery(channelConnectionListOptions());
  const mutation = useUpdateNotificationPreferences();

  const preferences = data?.preferences ?? {};
  const configuredProviders = Array.from(
    new Map(
      (connectionsData?.connections ?? [])
        .filter((connection) => connection.enabled)
        .map((connection) => [
          connection.provider,
          {
            provider: connection.provider,
            label: connection.display_name || providerLabel(connection.provider),
          },
        ]),
    ).values(),
  );
  const preferenceProviders = Object.keys(preferences.channel ?? {}).map((provider) => ({
    provider,
    label: providerLabel(provider),
  }));
  const channelProviders = configuredProviders.length > 0 ? configuredProviders : preferenceProviders;

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

  const handleChannelToggle = (provider: string, key: ChannelEventKey, enabled: boolean) => {
    const currentProvider = preferences.channel?.[provider] ?? {};
    const updatedProvider = { ...currentProvider, [key]: enabled };

    // Remove keys set to true (default) to keep the object clean
    if (enabled) {
      delete updatedProvider[key];
    }

    const updatedChannel: ChannelPreferences = {
      ...preferences.channel,
    };
    if (Object.keys(updatedProvider).length > 0) {
      updatedChannel[provider] = updatedProvider;
    } else {
      delete updatedChannel[provider];
    }

    const updated: NotificationPreferences = { ...preferences };
    if (Object.keys(updatedChannel).length === 0) {
      delete updated.channel;
    } else {
      updated.channel = updatedChannel;
    }

    mutation.mutate(updated, {
      onError: () => toast.error("Failed to update channel notification settings"),
    });
  };

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

      {channelProviders.length > 0 ? (
        <section className="space-y-4">
          <div>
            <h2 className="text-sm font-semibold">Channel Notifications</h2>
            <p className="text-sm text-muted-foreground mt-1">
              Control which events are forwarded to connected channel providers.
              Disabled types will not send outbound channel messages.
            </p>
          </div>

          <div className="space-y-3">
            {channelProviders.map((channelProvider) => (
              <Card key={channelProvider.provider}>
                <CardContent className="divide-y">
                  <div className="pb-3">
                    <p className="text-sm font-medium">{channelProvider.label}</p>
                    <p className="text-xs text-muted-foreground">
                      {providerLabel(channelProvider.provider)}
                    </p>
                  </div>
                  {channelNotificationTypes.map((type) => {
                    // R4: route through the shared helper rather than inlining
                    // the "missing === enabled" rule, so the UI cannot drift
                    // from the backend default contract.
                    const enabled = isChannelEventEnabled(
                      preferences,
                      channelProvider.provider,
                      type.key,
                    );
                    return (
                      <div
                        key={type.key}
                        className="flex items-center justify-between py-3 last:pb-0"
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
                            handleChannelToggle(channelProvider.provider, type.key, checked)
                          }
                        />
                      </div>
                    );
                  })}
                </CardContent>
              </Card>
            ))}
          </div>
        </section>
      ) : null}
    </div>
  );
}
