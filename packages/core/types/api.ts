import type { Issue, IssueStatus, IssuePriority, IssueAssigneeType } from "./issue";
import type { MemberRole } from "./workspace";
import type { Project } from "./project";

// Issue API
export interface CreateIssueRequest {
  title: string;
  description?: string;
  status?: IssueStatus;
  priority?: IssuePriority;
  assignee_type?: IssueAssigneeType;
  assignee_id?: string;
  parent_issue_id?: string;
  project_id?: string;
  due_date?: string;
  attachment_ids?: string[];
}

export interface UpdateIssueRequest {
  title?: string;
  description?: string;
  status?: IssueStatus;
  priority?: IssuePriority;
  assignee_type?: IssueAssigneeType | null;
  assignee_id?: string | null;
  position?: number;
  due_date?: string | null;
  parent_issue_id?: string | null;
  project_id?: string | null;
}

export interface ListIssuesParams {
  limit?: number;
  offset?: number;
  workspace_id?: string;
  status?: IssueStatus;
  priority?: IssuePriority;
  assignee_id?: string;
  assignee_ids?: string[];
  creator_id?: string;
  project_id?: string;
  open_only?: boolean;
}

/** Raw backend response shape for `GET /api/issues`. */
export interface ListIssuesResponse {
  issues: Issue[];
  total: number;
}

/** Per-status bucket in the paginated issue cache. `total` is the server count (all pages), not the length of `issues`. */
export interface IssueStatusBucket {
  issues: Issue[];
  total: number;
}

/**
 * Frontend cache shape for the issue list. Data is bucketed by status so
 * each column can paginate independently. Assembled from per-status
 * `api.listIssues` responses by the query functions in `issues/queries.ts`.
 */
export interface ListIssuesCache {
  byStatus: Partial<Record<IssueStatus, IssueStatusBucket>>;
}

export interface SearchIssueResult extends Issue {
  match_source: "title" | "description" | "comment";
  matched_snippet?: string;
}

export interface SearchIssuesResponse {
  issues: SearchIssueResult[];
  total: number;
}

export interface SearchProjectResult extends Project {
  match_source: "title" | "description";
  matched_snippet?: string;
}

export interface SearchProjectsResponse {
  projects: SearchProjectResult[];
  total: number;
}

export interface UpdateMeRequest {
  name?: string;
  avatar_url?: string;
}

export interface CreateMemberRequest {
  email: string;
  role?: MemberRole;
}

export interface UpdateMemberRequest {
  role: MemberRole;
}

// Personal Access Tokens
export interface PersonalAccessToken {
  id: string;
  name: string;
  token_prefix: string;
  expires_at: string | null;
  last_used_at: string | null;
  created_at: string;
}

export interface CreatePersonalAccessTokenRequest {
  name: string;
  expires_in_days?: number;
}

export interface CreatePersonalAccessTokenResponse extends PersonalAccessToken {
  token: string;
}

// Pagination
export interface PaginationParams {
  limit?: number;
  offset?: number;
}

// Channel Bindings
export type ChannelListenMode = "mentions" | "all";

export interface ChannelBinding {
  id: string;
  provider: string;
  connection_id: string;
  external_chat_id: string;
  chat_type: string;
  external_chat_name: string | null;
  default_project_id?: string | null;
  listen_mode: ChannelListenMode;
  agent_id?: string | null;
  is_primary: boolean;
  bound_by_user_id: string;
  created_at: string;
}

export interface ChannelConnection {
  id: string;
  provider: string;
  display_name: string;
  enabled: boolean;
  is_default: boolean;
  status: string;
  last_error: string | null;
  config: Record<string, string>;
  created_at: string;
  updated_at: string;
  config_schema: ChannelConfigField[];
}

export interface ChannelConfigField {
  key: string;
  label: string;
  required: boolean;
  secret: boolean;
  configured?: boolean;
}

export interface ListChannelConnectionsResponse {
  connections: ChannelConnection[];
  can_manage: boolean;
}

export interface ChannelProvider {
  provider: string;
  display_name: string;
  config_schema: ChannelConfigField[];
}

export interface ListChannelProvidersResponse {
  providers: ChannelProvider[];
}

export interface ChannelConnectionWriteRequest {
  provider?: string;
  display_name?: string;
  enabled?: boolean;
  is_default?: boolean;
  config?: Record<string, string | null>;
  secret_config?: Record<string, string | null>;
}

export interface ChannelBindTokenPreview {
  kind: "user" | "chat";
  provider: string;
  connection_id: string;
  connection_display_name: string;
  external_chat_id: string | null;
  external_chat_name: string | null;
  expires_at: string;
}

export interface ListChannelBindingsResponse {
  bindings: ChannelBinding[];
}

export interface CreateChannelBindingRequest {
  token: string;
  provider: string;
  connection_id?: string;
  default_project_id?: string | null;
  listen_mode?: ChannelListenMode;
  agent_id?: string | null;
}

export interface CreateChannelUserBindingRequest {
  token: string;
  provider: string;
  connection_id?: string;
}

export interface ChannelUserBindingResponse {
  provider: string;
  connection_id: string;
  external_user_id: string;
  user_id: string;
}

export interface PatchChannelBindingRequest {
  is_primary?: boolean;
  default_project_id?: string | null;
  listen_mode?: ChannelListenMode;
  /** Send empty string to clear a previously configured agent. */
  agent_id?: string | null;
}
