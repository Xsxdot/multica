"use client";

import { useEffect, useState } from "react";
import { Link2, Star, Trash2, MessageCircle, Plug, Plus, Settings, FlaskConical } from "lucide-react";
import { Button } from "@multica/ui/components/ui/button";
import { Badge } from "@multica/ui/components/ui/badge";
import { Input } from "@multica/ui/components/ui/input";
import { Label } from "@multica/ui/components/ui/label";
import { Switch } from "@multica/ui/components/ui/switch";
import { NativeSelect } from "@multica/ui/components/ui/native-select";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter,
} from "@multica/ui/components/ui/dialog";
import {
  AlertDialog,
  AlertDialogContent,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogCancel,
  AlertDialogAction,
} from "@multica/ui/components/ui/alert-dialog";
import { toast } from "sonner";
import { useQuery, useQueryClient, useMutation } from "@tanstack/react-query";
import { useWorkspaceId } from "@multica/core/hooks";
import { useCurrentWorkspace } from "@multica/core/paths";
import { useCurrentMember } from "@multica/core/permissions";
import {
  workspaceKeys,
  channelBindingListOptions,
  channelConnectionListOptions,
  channelProviderListOptions,
  agentListOptions,
} from "@multica/core/workspace/queries";
import { api } from "@multica/core/api";
import type {
  Agent,
  ChannelBinding,
  ChannelConnection,
  ChannelListenMode,
  ChannelProvider,
  PatchChannelBindingRequest,
  Project,
} from "@multica/core/types";

function providerLabel(value: string) {
  if (!value) return "Channel";
  return value
    .split(/[-_]/)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function connectionLabel(binding: ChannelBinding, connections: Map<string, ChannelConnection>) {
  const connection = connections.get(binding.connection_id);
  return connection?.display_name || providerLabel(binding.provider);
}

function listenModeLabel(mode: string | undefined) {
  return mode === "all" ? "所有消息" : "仅 @ 机器人";
}

type ConnectionDraft = {
  id?: string;
  provider: string;
  display_name: string;
  enabled: boolean;
  is_default: boolean;
  config: Record<string, string>;
  secret_config: Record<string, string>;
};

function draftFromConnection(connection: ChannelConnection): ConnectionDraft {
  return {
    id: connection.id,
    provider: connection.provider,
    display_name: connection.display_name,
    enabled: connection.enabled,
    is_default: connection.is_default,
    config: { ...(connection.config ?? {}) },
    secret_config: {},
  };
}

function BindingCard({
  binding,
  canManage,
  busy,
  onSetPrimary,
  onUnbind,
  connectionName,
  listenSummary,
  agentSummary,
  onEditSettings,
}: {
  binding: ChannelBinding;
  canManage: boolean;
  busy: boolean;
  onSetPrimary: () => void;
  onUnbind: () => void;
  connectionName: string;
  listenSummary: string;
  agentSummary: string;
  onEditSettings: () => void;
}) {
  return (
    <div className="flex items-center gap-3 px-4 py-3">
      <div className="flex h-8 w-8 items-center justify-center rounded-full bg-muted">
        <MessageCircle className="h-4 w-4 text-muted-foreground" />
      </div>
      <div className="min-w-0 flex-1">
        <div className="text-sm font-medium truncate">
          {binding.external_chat_name ?? binding.external_chat_id}
        </div>
        <div className="flex flex-wrap items-center gap-x-2 gap-y-1 text-xs text-muted-foreground">
          <span>{connectionName}</span>
          <span>·</span>
          <span className="capitalize">{binding.chat_type}</span>
          <span>·</span>
          <span>{listenSummary}</span>
          <span>·</span>
          <span className="truncate">Agent: {agentSummary}</span>
        </div>
      </div>
      <div className="flex flex-wrap items-center justify-end gap-2">
        {canManage && (
          <Button variant="outline" size="sm" disabled={busy} onClick={onEditSettings} title="Edit binding settings">
            <Settings className="h-3.5 w-3.5 mr-1" />
            Edit
          </Button>
        )}
        {binding.is_primary ? (
          <Badge variant="default">
            <Star className="h-3 w-3 mr-1" />
            Primary
          </Badge>
        ) : (
          canManage && (
            <Button variant="outline" size="sm" disabled={busy} onClick={onSetPrimary}>
              Set as Primary
            </Button>
          )
        )}
        {canManage && (
          <Button variant="ghost" size="icon-sm" disabled={busy} onClick={onUnbind} title="Unbind">
            <Trash2 className="h-4 w-4 text-muted-foreground" />
          </Button>
        )}
      </div>
    </div>
  );
}

export function IntegrationsTab() {
  const workspace = useCurrentWorkspace();
  const qc = useQueryClient();
  const wsId = useWorkspaceId();
  const { data: providersData } = useQuery(channelProviderListOptions());
  const { data: connectionsData } = useQuery(channelConnectionListOptions());
  const { data: bindingsData, isLoading } = useQuery(channelBindingListOptions(wsId));
  const { data: bindProjectsData } = useQuery({
    queryKey: ["settings", "integrations", wsId, "projects"],
    queryFn: () => api.listProjects({ workspace_id: wsId }),
    enabled: !!wsId,
  });
  const { data: bindAgents = [] } = useQuery({
    ...agentListOptions(wsId),
    enabled: !!wsId,
  });
  const connections = connectionsData?.connections ?? [];
  const canManageConnections = connectionsData?.can_manage ?? false;
  const connectionByID = new Map(connections.map((connection) => [connection.id, connection]));
  const bindings = bindingsData?.bindings ?? [];
  const bindProjects = bindProjectsData?.projects ?? [];

  const [actionBindingId, setActionBindingId] = useState<string | null>(null);
  const [editBinding, setEditBinding] = useState<ChannelBinding | null>(null);
  const [draft, setDraft] = useState<ConnectionDraft | null>(null);
  const providers = providersData?.providers ?? [];
  const providerByID = new Map(providers.map((provider) => [provider.provider, provider]));
  const [confirmAction, setConfirmAction] = useState<{
    title: string;
    description: string;
    variant?: "destructive";
    onConfirm: () => Promise<void>;
  } | null>(null);

  const { userId, role } = useCurrentMember(wsId);
  const canManageBinding = (binding: ChannelBinding) =>
    role === "owner" || role === "admin" || binding.bound_by_user_id === userId;

  const setPrimaryMutation = useMutation({
    mutationFn: ({ bindingId }: { bindingId: string }) =>
      api.setPrimaryChannelBinding(wsId, bindingId, { is_primary: true }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: workspaceKeys.channelBindings(wsId) });
      toast.success("Primary binding updated");
    },
    onError: (e: Error) => {
      toast.error(e.message || "Failed to update primary binding");
    },
  });

  const updateBindingMutation = useMutation({
    mutationFn: ({ bindingId, patch }: { bindingId: string; patch: PatchChannelBindingRequest }) =>
      api.updateChannelBinding(wsId, bindingId, patch),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: workspaceKeys.channelBindings(wsId) });
      toast.success("Binding settings saved");
      setEditBinding(null);
    },
    onError: (e: Error) => {
      toast.error(e.message || "Failed to update binding settings");
    },
  });

  const saveConnectionMutation = useMutation({
    mutationFn: async (input: ConnectionDraft) => {
      const provider = providerByID.get(input.provider);
      const config: Record<string, string | null> = {};
      const secret_config: Record<string, string | null> = {};
      for (const field of provider?.config_schema ?? []) {
        if (field.secret) {
          const value = input.secret_config[field.key];
          if (value !== undefined && value !== "") secret_config[field.key] = value;
        } else {
          config[field.key] = input.config[field.key] ?? "";
        }
      }
      const payload = {
        provider: input.provider,
        display_name: input.display_name,
        enabled: input.enabled,
        is_default: input.is_default,
        config,
        secret_config,
      };
      if (input.id) return api.updateChannelConnection(input.id, payload);
      return api.createChannelConnection(payload);
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: workspaceKeys.channelConnections() });
      setDraft(null);
      toast.success("Channel connection saved");
    },
    onError: (e: Error) => toast.error(e.message || "Failed to save channel connection"),
  });

  const testConnectionMutation = useMutation({
    mutationFn: (connectionId: string) => api.testChannelConnection(connectionId),
    onSuccess: () => toast.success("Connection test succeeded"),
    onError: (e: Error) => toast.error(e.message || "Connection test failed"),
  });

  const toggleConnection = async (connection: ChannelConnection, enabled: boolean) => {
    try {
      await api.updateChannelConnection(connection.id, {
        display_name: connection.display_name,
        enabled,
        is_default: connection.is_default,
        config: connection.config ?? {},
      });
      qc.invalidateQueries({ queryKey: workspaceKeys.channelConnections() });
    } catch (e) {
      toast.error(e instanceof Error ? e.message : "Failed to update connection");
    }
  };

  const handleSetPrimary = (binding: ChannelBinding) => {
    setActionBindingId(binding.id);
    setPrimaryMutation.mutate(
      { bindingId: binding.id },
      { onSettled: () => setActionBindingId(null) }
    );
  };

  const handleUnbind = (binding: ChannelBinding) => {
    setConfirmAction({
      title: `Unbind ${binding.external_chat_name ?? binding.external_chat_id}?`,
      description: `This will remove the binding to this ${connectionLabel(binding, connectionByID)} conversation. The conversation will no longer receive notifications or be able to interact with this workspace.`,
      variant: "destructive",
      onConfirm: async () => {
        setActionBindingId(binding.id);
        try {
          await api.deleteChannelBinding(wsId, binding.id);
          qc.invalidateQueries({ queryKey: workspaceKeys.channelBindings(wsId) });
          toast.success("Binding removed");
        } catch (e) {
          toast.error(e instanceof Error ? e.message : "Failed to remove binding");
        } finally {
          setActionBindingId(null);
          setConfirmAction(null);
        }
      },
    });
  };

  if (!workspace) return null;

  return (
    <div className="space-y-8">
      <section className="space-y-4">
        <div className="flex items-center justify-between gap-3">
          <div className="flex items-center gap-2">
            <Plug className="h-4 w-4 text-muted-foreground" />
            <h2 className="text-sm font-semibold">Channel Connections ({connections.length})</h2>
          </div>
          {canManageConnections ? (
            <Button
              size="sm"
              disabled={providers.length === 0}
              onClick={() => {
                const provider = providers[0];
                if (!provider) return;
                setDraft({
                  provider: provider.provider,
                  display_name: provider.display_name,
                  enabled: false,
                  is_default: false,
                  config: {},
                  secret_config: {},
                });
              }}
            >
              <Plus className="h-4 w-4" />
              Add
            </Button>
          ) : null}
        </div>
        {!canManageConnections ? (
          <p className="text-sm text-muted-foreground">Only workspace owners can manage channel connections.</p>
        ) : null}

        {connections.length > 0 ? (
          <div className="overflow-hidden rounded-xl ring-1 ring-foreground/10">
            {connections.map((connection, i) => (
              <ConnectionRow
                key={connection.id}
                connection={connection}
                separated={i > 0}
                canManage={canManageConnections}
                onEdit={() => setDraft(draftFromConnection(connection))}
                onToggle={(enabled) => toggleConnection(connection, enabled)}
                onTest={() => testConnectionMutation.mutate(connection.id)}
                onDelete={() => {
                  setConfirmAction({
                    title: `Delete ${connection.display_name}?`,
                    description: "This removes the connection and its channel bindings.",
                    variant: "destructive",
                    onConfirm: async () => {
                      try {
                        await api.deleteChannelConnection(connection.id);
                        qc.invalidateQueries({ queryKey: workspaceKeys.channelConnections() });
                        qc.invalidateQueries({ queryKey: workspaceKeys.channelBindings(wsId) });
                        toast.success("Channel connection deleted");
                      } catch (e) {
                        toast.error(e instanceof Error ? e.message : "Failed to delete channel connection");
                      } finally {
                        setConfirmAction(null);
                      }
                    },
                  });
                }}
              />
            ))}
          </div>
        ) : (
          <p className="text-sm text-muted-foreground">No channel connections configured.</p>
        )}
      </section>

      <section className="space-y-4">
        <div className="flex items-center gap-2">
          <Link2 className="h-4 w-4 text-muted-foreground" />
          <h2 className="text-sm font-semibold">Channel Bindings ({bindings.length})</h2>
        </div>

        {isLoading ? (
          <p className="text-sm text-muted-foreground">Loading...</p>
        ) : bindings.length > 0 ? (
          <div className="overflow-hidden rounded-xl ring-1 ring-foreground/10">
            {bindings.map((b, i) => {
              const agentSummary = b.agent_id
                ? bindAgents.find((a) => a.id === b.agent_id)?.name ?? b.agent_id
                : "自动选择";
              return (
                <div key={b.id} className={i > 0 ? "border-t border-border/50" : ""}>
                  <BindingCard
                    binding={b}
                    canManage={canManageBinding(b)}
                    busy={actionBindingId === b.id || updateBindingMutation.isPending}
                    onSetPrimary={() => handleSetPrimary(b)}
                    onUnbind={() => handleUnbind(b)}
                    connectionName={connectionLabel(b, connectionByID)}
                    listenSummary={listenModeLabel(b.listen_mode)}
                    agentSummary={agentSummary}
                    onEditSettings={() => setEditBinding(b)}
                  />
                </div>
              );
            })}
          </div>
        ) : (
          <p className="text-sm text-muted-foreground">No integrations yet.</p>
        )}
      </section>

      <BindingSettingsDialog
        binding={editBinding}
        open={!!editBinding}
        onOpenChange={(open) => {
          if (!open) setEditBinding(null);
        }}
        projects={bindProjects}
        agents={bindAgents.filter((a) => !a.archived_at)}
        busy={updateBindingMutation.isPending}
        onSave={(patch) => {
          if (!editBinding) return;
          updateBindingMutation.mutate({ bindingId: editBinding.id, patch });
        }}
      />

      {canManageConnections ? (
        <ConnectionDialog
          draft={draft}
          providers={providers}
          busy={saveConnectionMutation.isPending}
          onChange={setDraft}
          onClose={() => setDraft(null)}
          onSave={() => {
            if (draft) saveConnectionMutation.mutate(draft);
          }}
        />
      ) : null}

      <AlertDialog open={!!confirmAction} onOpenChange={(v) => { if (!v) setConfirmAction(null); }}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>{confirmAction?.title}</AlertDialogTitle>
            <AlertDialogDescription>{confirmAction?.description}</AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              variant={confirmAction?.variant === "destructive" ? "destructive" : "default"}
              onClick={async () => {
                await confirmAction?.onConfirm();
              }}
            >
              Confirm
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}

function BindingSettingsDialog({
  binding,
  open,
  onOpenChange,
  projects,
  agents,
  busy,
  onSave,
}: {
  binding: ChannelBinding | null;
  open: boolean;
  onOpenChange: (open: boolean) => void;
  projects: Project[];
  agents: Agent[];
  busy: boolean;
  onSave: (patch: PatchChannelBindingRequest) => void;
}) {
  const [defaultProjectId, setDefaultProjectId] = useState("");
  const [listenMode, setListenMode] = useState<ChannelListenMode>("mentions");
  const [agentId, setAgentId] = useState("");

  useEffect(() => {
    if (!binding) return;
    setDefaultProjectId(binding.default_project_id ?? "");
    setListenMode((binding.listen_mode as ChannelListenMode) || "mentions");
    setAgentId(binding.agent_id ?? "");
  }, [binding]);

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle>Channel binding settings</DialogTitle>
          <DialogDescription>
            Update listen scope, default project, and optional fixed agent for this chat.
          </DialogDescription>
        </DialogHeader>
        {binding ? (
          <div className="space-y-4">
            <div className="grid gap-2">
              <Label htmlFor="edit-binding-project">Default project</Label>
              <NativeSelect
                id="edit-binding-project"
                value={defaultProjectId}
                disabled={busy}
                onChange={(e) => setDefaultProjectId(e.target.value)}
              >
                <option value="">None</option>
                {projects.map((p) => (
                  <option key={p.id} value={p.id}>
                    {p.title}
                  </option>
                ))}
              </NativeSelect>
            </div>
            <div className="grid gap-2">
              <Label htmlFor="edit-binding-listen">Listen scope</Label>
              <NativeSelect
                id="edit-binding-listen"
                value={listenMode}
                disabled={busy}
                onChange={(e) => setListenMode(e.target.value as ChannelListenMode)}
              >
                <option value="mentions">Mentions only (@ bot)</option>
                <option value="all">All messages</option>
              </NativeSelect>
            </div>
            <div className="grid gap-2">
              <Label htmlFor="edit-binding-agent">Agent (optional)</Label>
              <NativeSelect
                id="edit-binding-agent"
                value={agentId}
                disabled={busy}
                onChange={(e) => setAgentId(e.target.value)}
              >
                <option value="">Auto-select</option>
                {agents.map((a) => (
                  <option key={a.id} value={a.id}>
                    {a.name}
                  </option>
                ))}
              </NativeSelect>
            </div>
            <DialogFooter>
              <Button variant="secondary" type="button" disabled={busy} onClick={() => onOpenChange(false)}>
                Cancel
              </Button>
              <Button
                type="button"
                disabled={busy}
                onClick={() =>
                  onSave({
                    default_project_id: defaultProjectId === "" ? null : defaultProjectId,
                    listen_mode: listenMode,
                    agent_id: agentId === "" ? "" : agentId,
                  })
                }
              >
                Save
              </Button>
            </DialogFooter>
          </div>
        ) : null}
      </DialogContent>
    </Dialog>
  );
}

function ConnectionDialog({
  draft,
  providers,
  busy,
  onChange,
  onClose,
  onSave,
}: {
  draft: ConnectionDraft | null;
  providers: ChannelProvider[];
  busy: boolean;
  onChange: (draft: ConnectionDraft | null) => void;
  onClose: () => void;
  onSave: () => void;
}) {
  const provider = providers.find((item) => item.provider === draft?.provider);
  return (
    <Dialog open={!!draft} onOpenChange={(open) => { if (!open) onClose(); }}>
      <DialogContent className="sm:max-w-lg">
        <DialogHeader>
          <DialogTitle>{draft?.id ? "Edit Channel Connection" : "Add Channel Connection"}</DialogTitle>
          <DialogDescription>Configure a channel connection for this Multica instance.</DialogDescription>
        </DialogHeader>
        {draft ? (
          <div className="space-y-4">
            <div className="grid gap-2">
              <Label>Provider</Label>
              <NativeSelect
                value={draft.provider}
                disabled={!!draft.id}
                onChange={(event) => {
                  const nextProvider = providers.find((item) => item.provider === event.target.value);
                  if (!nextProvider) return;
                  onChange({
                    provider: nextProvider.provider,
                    display_name: nextProvider.display_name,
                    enabled: draft.enabled,
                    is_default: draft.is_default,
                    config: {},
                    secret_config: {},
                  });
                }}
              >
                {providers.map((item) => (
                  <option key={item.provider} value={item.provider}>{item.display_name}</option>
                ))}
              </NativeSelect>
            </div>
            <div className="grid gap-2">
              <Label>Display name</Label>
              <Input
                value={draft.display_name}
                onChange={(event) => onChange({ ...draft, display_name: event.target.value })}
              />
            </div>
            <div className="flex items-center justify-between gap-3 rounded-lg border border-border px-3 py-2">
              <Label>Enabled</Label>
              <Switch checked={draft.enabled} onCheckedChange={(enabled) => onChange({ ...draft, enabled })} />
            </div>
            {(provider?.config_schema ?? []).map((field) => (
              <div className="grid gap-2" key={field.key}>
                <Label>{field.label || field.key}{field.required ? " *" : ""}</Label>
                <Input
                  type={field.secret ? "password" : "text"}
                  placeholder={field.secret && field.configured ? "Configured; leave blank to keep" : undefined}
                  value={field.secret ? draft.secret_config[field.key] ?? "" : draft.config[field.key] ?? ""}
                  onChange={(event) => {
                    const value = event.target.value;
                    if (field.secret) {
                      onChange({ ...draft, secret_config: { ...draft.secret_config, [field.key]: value } });
                    } else {
                      onChange({ ...draft, config: { ...draft.config, [field.key]: value } });
                    }
                  }}
                />
              </div>
            ))}
          </div>
        ) : null}
        <DialogFooter>
          <Button variant="outline" onClick={onClose}>Cancel</Button>
          <Button disabled={!draft || busy} onClick={onSave}>Save</Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function ConnectionRow({
  connection,
  separated,
  canManage,
  onEdit,
  onToggle,
  onTest,
  onDelete,
}: {
  connection: ChannelConnection;
  separated: boolean;
  canManage: boolean;
  onEdit: () => void;
  onToggle: (enabled: boolean) => void;
  onTest: () => void;
  onDelete: () => void;
}) {
  const requiredFields = (connection.config_schema ?? [])
    .filter((field) => field.required)
    .map((field) => field.label || field.key);

  return (
    <div className={`flex items-center gap-3 px-4 py-3 ${separated ? "border-t border-border/50" : ""}`}>
      <div className="flex h-8 w-8 items-center justify-center rounded-full bg-muted">
        <MessageCircle className="h-4 w-4 text-muted-foreground" />
      </div>
      <div className="min-w-0 flex-1 space-y-0.5">
        <div className="text-sm font-medium truncate">{connection.display_name}</div>
        <div className="text-xs text-muted-foreground">{providerLabel(connection.provider)}</div>
        {requiredFields.length > 0 ? (
          <div className="text-xs text-muted-foreground">
            Required config: {requiredFields.join(", ")}
          </div>
        ) : null}
      </div>
      <div className="flex items-center gap-2">
        <Badge variant={connection.enabled ? "default" : "secondary"}>
          {connection.status || (connection.enabled ? "Enabled" : "Disabled")}
        </Badge>
        {canManage ? (
          <>
            <Switch checked={connection.enabled} onCheckedChange={onToggle} />
            <Button variant="ghost" size="icon-sm" title="Test connection" onClick={onTest}>
              <FlaskConical className="h-4 w-4 text-muted-foreground" />
            </Button>
            <Button variant="ghost" size="icon-sm" title="Edit connection" onClick={onEdit}>
              <Settings className="h-4 w-4 text-muted-foreground" />
            </Button>
            <Button variant="ghost" size="icon-sm" title="Delete connection" onClick={onDelete}>
              <Trash2 className="h-4 w-4 text-muted-foreground" />
            </Button>
          </>
        ) : null}
      </div>
    </div>
  );
}
