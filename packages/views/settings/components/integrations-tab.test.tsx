import type { ReactNode } from "react";
import { describe, expect, it, beforeEach, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

vi.mock("@multica/ui/components/ui/dialog", () => ({
  Dialog: ({ children, open }: { children: ReactNode; open: boolean }) =>
    open ? <div>{children}</div> : null,
  DialogContent: ({ children }: { children: ReactNode }) => <div>{children}</div>,
  DialogHeader: ({ children }: { children: ReactNode }) => <div>{children}</div>,
  DialogTitle: ({ children }: { children: ReactNode }) => <h1>{children}</h1>,
  DialogDescription: ({ children }: { children: ReactNode }) => <p>{children}</p>,
  DialogFooter: ({ children }: { children: ReactNode }) => <div>{children}</div>,
}));

vi.mock("@multica/ui/components/ui/alert-dialog", () => ({
  AlertDialog: ({ children, open }: { children: ReactNode; open: boolean }) =>
    open ? <div>{children}</div> : null,
  AlertDialogContent: ({ children }: { children: ReactNode }) => <div>{children}</div>,
  AlertDialogHeader: ({ children }: { children: ReactNode }) => <div>{children}</div>,
  AlertDialogTitle: ({ children }: { children: ReactNode }) => <h1>{children}</h1>,
  AlertDialogDescription: ({ children }: { children: ReactNode }) => <p>{children}</p>,
  AlertDialogFooter: ({ children }: { children: ReactNode }) => <div>{children}</div>,
  AlertDialogCancel: ({ children }: { children: ReactNode }) => <button>{children}</button>,
  AlertDialogAction: ({ children, onClick }: { children: ReactNode; onClick?: () => void }) => (
    <button onClick={onClick}>{children}</button>
  ),
}));

vi.mock("@tanstack/react-query", () => ({
  useQuery: vi.fn(),
  useQueryClient: vi.fn(() => ({ invalidateQueries: vi.fn() })),
  useMutation: vi.fn(),
}));

vi.mock("@multica/core/api", () => ({
  api: {
    listChannelBindings: vi.fn(),
    createChannelBinding: vi.fn(),
    deleteChannelBinding: vi.fn(),
    setPrimaryChannelBinding: vi.fn(),
  },
}));

vi.mock("@multica/core/auth", () => ({
  useAuthStore: vi.fn(() => ({ user: { id: "user-1", name: "Test User" } })),
}));

vi.mock("@multica/core/hooks", () => ({
  useWorkspaceId: vi.fn(() => "ws-1"),
}));

vi.mock("@multica/core/paths", () => ({
  useCurrentWorkspace: vi.fn(() => ({ id: "ws-1", name: "Test Workspace" })),
}));

vi.mock("sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

import { useQuery, useMutation } from "@tanstack/react-query";
import { api } from "@multica/core/api";
import { IntegrationsTab } from "./integrations-tab";

describe("IntegrationsTab", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("renders empty state when no bindings", () => {
    (useQuery as ReturnType<typeof vi.fn>).mockReturnValue({
      data: [],
      isLoading: false,
    });
    (useMutation as ReturnType<typeof vi.fn>).mockReturnValue({
      mutateAsync: vi.fn(),
      isPending: false,
    });

    render(<IntegrationsTab />);
    expect(screen.getByText("No integrations yet.")).toBeInTheDocument();
  });

  it("renders binding list with primary badge", () => {
    (useQuery as ReturnType<typeof vi.fn>).mockReturnValue({
      data: [
        {
          id: "bind-1",
          provider: "feishu",
          external_chat_id: "oc_xxx",
          external_chat_name: "Test Group",
          chat_type: "group",
          is_primary: true,
          bound_by_user_id: "user-1",
          created_at: "2026-05-06T00:00:00Z",
        },
      ],
      isLoading: false,
    });
    (useMutation as ReturnType<typeof vi.fn>).mockReturnValue({
      mutateAsync: vi.fn(),
      isPending: false,
    });

    render(<IntegrationsTab />);
    expect(screen.getByText("Test Group")).toBeInTheDocument();
    expect(screen.getByText("Primary")).toBeInTheDocument();
  });

  it("shows 'Set as Primary' button for non-primary binding", () => {
    (useQuery as ReturnType<typeof vi.fn>).mockReturnValue({
      data: [
        {
          id: "bind-1",
          provider: "feishu",
          external_chat_id: "oc_xxx",
          external_chat_name: "Test Group",
          chat_type: "group",
          is_primary: true,
          bound_by_user_id: "user-1",
          created_at: "2026-05-06T00:00:00Z",
        },
        {
          id: "bind-2",
          provider: "feishu",
          external_chat_id: "oc_yyy",
          external_chat_name: "Second Group",
          chat_type: "group",
          is_primary: false,
          bound_by_user_id: "user-1",
          created_at: "2026-05-06T00:00:00Z",
        },
      ],
      isLoading: false,
    });
    (useMutation as ReturnType<typeof vi.fn>).mockReturnValue({
      mutateAsync: vi.fn(),
      isPending: false,
    });

    render(<IntegrationsTab />);
    expect(screen.getByText("Second Group")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Set as Primary" })).toBeInTheDocument();
  });

  it("calls setPrimary when 'Set as Primary' is clicked", async () => {
    const setPrimaryMock = vi.fn().mockResolvedValue({});
    (useQuery as ReturnType<typeof vi.fn>).mockReturnValue({
      data: [
        {
          id: "bind-1",
          provider: "feishu",
          external_chat_id: "oc_xxx",
          external_chat_name: "Primary Group",
          chat_type: "group",
          is_primary: true,
          bound_by_user_id: "user-1",
          created_at: "2026-05-06T00:00:00Z",
        },
        {
          id: "bind-2",
          provider: "feishu",
          external_chat_id: "oc_yyy",
          external_chat_name: "Second Group",
          chat_type: "group",
          is_primary: false,
          bound_by_user_id: "user-1",
          created_at: "2026-05-06T00:00:00Z",
        },
      ],
      isLoading: false,
    });
    (useMutation as ReturnType<typeof vi.fn>).mockImplementation((opts: { mutationFn?: (vars: unknown) => Promise<unknown> }) => ({
      mutateAsync: opts?.mutationFn ?? vi.fn(),
      isPending: false,
    }));
    (api.setPrimaryChannelBinding as ReturnType<typeof vi.fn>).mockImplementation(setPrimaryMock);

    const user = userEvent.setup();
    render(<IntegrationsTab />);

    const btn = screen.getByRole("button", { name: "Set as Primary" });
    await user.click(btn);

    await waitFor(() => {
      expect(setPrimaryMock).toHaveBeenCalledWith({ workspaceId: "ws-1", bindingId: "bind-2" });
    });
  });

  it("shows unbind confirmation dialog when unbind is clicked", async () => {
    const deleteMock = vi.fn().mockResolvedValue({});
    (useQuery as ReturnType<typeof vi.fn>).mockReturnValue({
      data: [
        {
          id: "bind-1",
          provider: "feishu",
          external_chat_id: "oc_xxx",
          external_chat_name: "Test Group",
          chat_type: "group",
          is_primary: true,
          bound_by_user_id: "user-1",
          created_at: "2026-05-06T00:00:00Z",
        },
      ],
      isLoading: false,
    });
    (useMutation as ReturnType<typeof vi.fn>).mockImplementation((opts: { mutationFn?: (vars: unknown) => Promise<unknown> }) => ({
      mutateAsync: opts?.mutationFn ?? vi.fn(),
      isPending: false,
    }));
    (api.deleteChannelBinding as ReturnType<typeof vi.fn>).mockImplementation(deleteMock);

    const user = userEvent.setup();
    render(<IntegrationsTab />);

    const unbindBtn = screen.getByRole("button", { name: "Unbind" });
    await user.click(unbindBtn);

    expect(screen.getByText("Unbind Test Group?")).toBeInTheDocument();

    const confirmBtn = screen.getByRole("button", { name: "Confirm" });
    await user.click(confirmBtn);

    await waitFor(() => {
      expect(deleteMock).toHaveBeenCalledWith({ workspaceId: "ws-1", bindingId: "bind-1" });
    });
  });
});
