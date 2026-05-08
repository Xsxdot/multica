import { render, screen, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

const { authStateRef, mockCreateChannelBinding, mockCreateChannelUserBinding, routerReplace, searchParamsState } =
  vi.hoisted(() => ({
    authStateRef: {
      state: {
        user: { id: "user-1", email: "test@multica.ai" },
        isLoading: false,
      },
    },
    mockCreateChannelBinding: vi.fn(),
    mockCreateChannelUserBinding: vi.fn(),
    routerReplace: vi.fn(),
    searchParamsState: { params: new URLSearchParams({ token: "bind-token", provider: "feishu" }) },
  }));

vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), replace: routerReplace }),
  useSearchParams: () => searchParamsState.params,
}));

vi.mock("@multica/core/auth", () => ({
  useAuthStore: (selector: (s: typeof authStateRef.state) => unknown) => selector(authStateRef.state),
}));

vi.mock("@tanstack/react-query", () => ({
  queryOptions: (options: unknown) => options,
  useQuery: () => ({ data: [], isLoading: false }),
}));

vi.mock("@multica/core/api", () => {
  class ApiError extends Error {
    status: number;
    statusText: string;
    body: unknown;

    constructor(message: string, status: number, statusText = "", body?: unknown) {
      super(message);
      this.status = status;
      this.statusText = statusText;
      this.body = body;
    }
  }

  return {
    ApiError,
    api: {
      createChannelBinding: mockCreateChannelBinding,
      createChannelUserBinding: mockCreateChannelUserBinding,
    },
  };
});

import BindPage from "./page";

describe("BindPage", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    authStateRef.state.user = { id: "user-1", email: "test@multica.ai" };
    authStateRef.state.isLoading = false;
    searchParamsState.params = new URLSearchParams({ token: "bind-token", provider: "feishu" });
    mockCreateChannelUserBinding.mockResolvedValue({
      provider: "feishu",
      external_user_id: "ou_1",
      user_id: "user-1",
    });
  });

  it("shows success after the binding request resolves", async () => {
    render(<BindPage />);

    expect(screen.getByText("正在绑定飞书账号")).toBeInTheDocument();

    await waitFor(() => {
      expect(screen.getByText("绑定完成")).toBeInTheDocument();
    });
    expect(mockCreateChannelUserBinding).toHaveBeenCalledWith({
      token: "bind-token",
      provider: "feishu",
    });
  });
});
