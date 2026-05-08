"use client";

import { Suspense, useEffect, useMemo, useRef, useState } from "react";
import type { ReactNode } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import { Check, Loader2, LogIn, MessageCircle, RotateCcw } from "lucide-react";
import { useQuery } from "@tanstack/react-query";
import { ApiError, api } from "@multica/core/api";
import { useAuthStore } from "@multica/core/auth";
import { paths } from "@multica/core/paths";
import { workspaceListOptions } from "@multica/core/workspace/queries";
import { Button } from "@multica/ui/components/ui/button";

type BindState = "idle" | "binding" | "success" | "error";

function BindPageContent() {
  const router = useRouter();
  const params = useSearchParams();
  const token = params.get("token") ?? "";
  const provider = params.get("provider") ?? "feishu";
  const kind = params.get("kind") ?? "user";
  const user = useAuthStore((s) => s.user);
  const isLoading = useAuthStore((s) => s.isLoading);
  const { data: workspaces = [], isLoading: workspacesLoading } = useQuery({
    ...workspaceListOptions(),
    enabled: !!user && kind === "chat",
  });
  const [state, setState] = useState<BindState>("idle");
  const [message, setMessage] = useState("");
  const [retryNonce, setRetryNonce] = useState(0);
  const bindingKeyRef = useRef<string | null>(null);

  const loginHref = useMemo(() => {
    const next = `/bind?kind=${encodeURIComponent(kind)}&token=${encodeURIComponent(token)}&provider=${encodeURIComponent(provider)}`;
    return `${paths.login()}?next=${encodeURIComponent(next)}`;
  }, [kind, provider, token]);

  useEffect(() => {
    if (!isLoading && !user) router.replace(loginHref);
  }, [isLoading, loginHref, router, user]);

  useEffect(() => {
    if (isLoading || !user || !token || kind !== "user") return;

    const bindingKey = `${user.id}:${provider}:${token}:${retryNonce}`;
    if (bindingKeyRef.current === bindingKey) return;
    bindingKeyRef.current = bindingKey;

    let cancelled = false;
    setState("binding");
    api
      .createChannelUserBinding({ token, provider })
      .then(() => {
        if (cancelled) return;
        setState("success");
        setMessage("飞书账号已绑定到当前 Multica 账号。回到飞书后再发送一次消息即可继续。");
      })
      .catch((err: unknown) => {
        if (cancelled) return;
        setState("error");
        if (err instanceof ApiError && err.status === 409) {
          setMessage("这个绑定链接已经被使用过。请回到飞书重新发送消息，机器人会生成新的链接。");
          return;
        }
        setMessage(err instanceof Error ? err.message : "绑定失败，请回到飞书重新发送消息。");
      });

    return () => {
      cancelled = true;
    };
  }, [isLoading, kind, provider, retryNonce, token, user?.id]);

  if (isLoading || !user) {
    return (
      <BindShell
        icon={<LogIn className="size-5" />}
        title="正在确认登录状态"
        description="如果还没有登录，会先跳转到登录页。"
      />
    );
  }

  if (!token) {
    return (
      <BindShell
        icon={<MessageCircle className="size-5" />}
        title="绑定链接无效"
        description="缺少绑定 token。请回到飞书重新发送消息，让机器人生成新的绑定链接。"
      />
    );
  }

  if (state === "success") {
    return (
      <BindShell
        icon={<Check className="size-5" />}
        title="绑定完成"
        description={message}
        action={
          <Button onClick={() => router.push(paths.root())}>
            <Check className="size-4" />
            打开 Multica
          </Button>
        }
      />
    );
  }

  if (state === "error") {
    return (
      <BindShell
        icon={<MessageCircle className="size-5" />}
        title="绑定失败"
        description={message}
        action={
          <Button
            variant="secondary"
            onClick={() => {
              bindingKeyRef.current = null;
              setState("idle");
              setRetryNonce((value) => value + 1);
            }}
          >
            <RotateCcw className="size-4" />
            重试
          </Button>
        }
      />
    );
  }

  if (kind === "chat") {
    return (
      <BindShell
        icon={workspacesLoading ? <Loader2 className="size-5 animate-spin" /> : <MessageCircle className="size-5" />}
        title="绑定飞书群到工作区"
        description="选择这个飞书群要连接的 Multica 工作区。绑定后，群里可以使用 Bot 指令。"
        action={
          <div className="space-y-2">
            {workspaces.map((workspace) => (
              <Button
                key={workspace.id}
                className="w-full justify-start"
                variant="secondary"
                disabled={state === "binding"}
                onClick={() => {
                  const bindingKey = `${user.id}:${provider}:${token}:${workspace.id}:${retryNonce}`;
                  if (bindingKeyRef.current === bindingKey) return;
                  bindingKeyRef.current = bindingKey;
                  setState("binding");
                  api
                    .createChannelBinding(workspace.id, { token, provider })
                    .then(() => {
                      setState("success");
                      setMessage(`飞书群已绑定到 ${workspace.name}。回到群里发送指令即可使用。`);
                      router.push(paths.workspace(workspace.slug).settings());
                    })
                    .catch((err: unknown) => {
                      setState("error");
                      setMessage(err instanceof Error ? err.message : "群绑定失败，请回到飞书重新发起绑定。");
                    });
                }}
              >
                {workspace.name}
              </Button>
            ))}
            {!workspacesLoading && workspaces.length === 0 ? (
              <Button onClick={() => router.push(paths.newWorkspace())}>
                创建工作区
              </Button>
            ) : null}
          </div>
        }
      />
    );
  }

  return (
    <BindShell
      icon={<Loader2 className="size-5 animate-spin" />}
      title="正在绑定飞书账号"
      description="完成后，这个飞书身份发来的消息会映射到当前 Multica 账号。"
    />
  );
}

function BindShell({
  icon,
  title,
  description,
  action,
}: {
  icon: ReactNode;
  title: string;
  description: string;
  action?: ReactNode;
}) {
  return (
    <main className="flex min-h-svh items-center justify-center bg-background px-6">
      <section className="w-full max-w-md space-y-5">
        <div className="flex size-10 items-center justify-center rounded-lg border border-border bg-muted text-muted-foreground">
          {icon}
        </div>
        <div className="space-y-2">
          <h1 className="text-xl font-semibold tracking-normal text-foreground">{title}</h1>
          <p className="text-sm leading-6 text-muted-foreground">{description}</p>
        </div>
        {action}
      </section>
    </main>
  );
}

export default function BindPage() {
  return (
    <Suspense
      fallback={
        <BindShell
          icon={<Loader2 className="size-5 animate-spin" />}
          title="正在打开绑定链接"
          description="请稍等。"
        />
      }
    >
      <BindPageContent />
    </Suspense>
  );
}
