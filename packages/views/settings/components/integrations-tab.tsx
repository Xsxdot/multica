"use client";

import { useState } from "react";
import { Link2, Plus, Star, Trash2, MessageCircle } from "lucide-react";
import { Button } from "@multica/ui/components/ui/button";
import { Card, CardContent } from "@multica/ui/components/ui/card";
import { Badge } from "@multica/ui/components/ui/badge";
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
import { useAuthStore } from "@multica/core/auth";
import { useWorkspaceId } from "@multica/core/hooks";
import { useCurrentWorkspace } from "@multica/core/paths";
import { useCurrentMember } from "@multica/core/permissions";
import { workspaceKeys, channelBindingListOptions } from "@multica/core/workspace/queries";
import { api } from "@multica/core/api";
import type { ChannelBinding } from "@multica/core/types";

function BindingCard({
  binding,
  canManage,
  busy,
  onSetPrimary,
  onUnbind,
}: {
  binding: ChannelBinding;
  canManage: boolean;
  busy: boolean;
  onSetPrimary: () => void;
  onUnbind: () => void;
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
        <div className="flex items-center gap-2 text-xs text-muted-foreground">
          <span className="capitalize">{binding.provider}</span>
          <span>·</span>
          <span className="capitalize">{binding.chat_type}</span>
        </div>
      </div>
      <div className="flex items-center gap-2">
        {binding.is_primary ? (
          <Badge variant="default">
            <Star className="h-3 w-3 mr-1" />
            Primary
          </Badge>
        ) : (
          canManage && (
            <Button
              variant="outline"
              size="sm"
              disabled={busy}
              onClick={onSetPrimary}
            >
              Set as Primary
            </Button>
          )
        )}
        {canManage && (
          <Button
            variant="ghost"
            size="icon-sm"
            disabled={busy}
            onClick={onUnbind}
            title="Unbind"
          >
            <Trash2 className="h-4 w-4 text-muted-foreground" />
          </Button>
        )}
      </div>
    </div>
  );
}

export function IntegrationsTab() {
  const user = useAuthStore((s) => s.user);
  const workspace = useCurrentWorkspace();
  const qc = useQueryClient();
  const wsId = useWorkspaceId();
  const { data: bindingsData, isLoading } = useQuery(channelBindingListOptions(wsId));
  const bindings = bindingsData?.bindings ?? [];

  const [actionBindingId, setActionBindingId] = useState<string | null>(null);
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

  const deleteMutation = useMutation({
    mutationFn: ({ bindingId }: { bindingId: string }) =>
      api.deleteChannelBinding(wsId, bindingId),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: workspaceKeys.channelBindings(wsId) });
      toast.success("Binding removed");
    },
    onError: (e: Error) => {
      toast.error(e.message || "Failed to remove binding");
    },
  });

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
      description: `This will remove the binding to this ${binding.provider} chat. The chat will no longer receive notifications or be able to interact with this workspace.`,
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
        <div className="flex items-center gap-2">
          <Link2 className="h-4 w-4 text-muted-foreground" />
          <h2 className="text-sm font-semibold">Channel Bindings ({bindings.length})</h2>
        </div>

        {isLoading ? (
          <p className="text-sm text-muted-foreground">Loading...</p>
        ) : bindings.length > 0 ? (
          <div className="overflow-hidden rounded-xl ring-1 ring-foreground/10">
            {bindings.map((b, i) => (
              <div key={b.id} className={i > 0 ? "border-t border-border/50" : ""}>
                <BindingCard
                  binding={b}
                  canManage={canManageBinding(b)}
                  busy={actionBindingId === b.id}
                  onSetPrimary={() => handleSetPrimary(b)}
                  onUnbind={() => handleUnbind(b)}
                />
              </div>
            ))}
          </div>
        ) : (
          <p className="text-sm text-muted-foreground">No integrations yet.</p>
        )}
      </section>

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
