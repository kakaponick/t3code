import {
  AuthOrchestrationOperateScope,
  AuthOrchestrationReadScope,
  CommandId,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  DEFAULT_SERVER_SETTINGS,
  EnvironmentHttpApi,
  MessageId,
  type ModelSelection,
  type OrchestrationProjectShell,
  ProjectId,
  ProviderInstanceId,
  type ServerProvider,
  ServerSettings,
  ThreadId,
} from "@t3tools/contracts";
import { normalizeProjectPathForComparison } from "@t3tools/shared/path";
import { resolveProjectSettings } from "@t3tools/shared/projectSettings";
import { fromLenientJson } from "@t3tools/shared/schemaJson";
import { isModelSelectionProviderEnabled } from "@t3tools/shared/serverSettings";
import { truncate } from "@t3tools/shared/String";
import * as Console from "effect/Console";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as References from "effect/References";
import * as Schema from "effect/Schema";
import * as Stdio from "effect/Stdio";
import * as Stream from "effect/Stream";
import { Argument, Command, Flag, GlobalFlag } from "effect/unstable/cli";
import { FetchHttpClient } from "effect/unstable/http";
import * as HttpApiClient from "effect/unstable/httpapi/HttpApiClient";

import * as EnvironmentAuth from "../auth/EnvironmentAuth.ts";
import * as ServerConfig from "../config.ts";
import { readProviderStatusCache } from "../provider/providerStatusCache.ts";
import { isProcessAlive, readPersistedServerRuntimeState } from "../serverRuntimeState.ts";
import * as WorkspacePaths from "../workspace/WorkspacePaths.ts";
import { projectLocationFlags, resolveCliAuthConfig } from "./config.ts";
import { projectCommandErrorFromLiveServerRequest } from "./project.ts";

const THREAD_CLI_LIVE_SERVER_TIMEOUT = Duration.seconds(10);
const STDIN_PROMPT = "-";

const encodeThreadStartOutput = Schema.encodeEffect(
  Schema.fromJsonString(
    Schema.Struct({
      threadId: ThreadId,
      projectId: ProjectId,
      instanceId: ProviderInstanceId,
      model: Schema.String,
    }),
  ),
);

const decodeServerSettings = Schema.decodeUnknownEffect(fromLenientJson(ServerSettings));

export class ThreadServerNotRunningError extends Schema.TaggedError<ThreadServerNotRunningError>()(
  "ThreadServerNotRunningError",
  {},
) {
  override get message(): string {
    return "T3 Code not running. Open desktop app or run `t3`.";
  }
}

export class ThreadPromptEmptyError extends Schema.TaggedError<ThreadPromptEmptyError>()(
  "ThreadPromptEmptyError",
  {},
) {
  override get message(): string {
    return "Prompt empty.";
  }
}

export class ThreadProjectNotFoundError extends Schema.TaggedError<ThreadProjectNotFoundError>()(
  "ThreadProjectNotFoundError",
  { identifier: Schema.String },
) {
  override get message(): string {
    return `Project '${this.identifier}' not found. Add with \`t3 project add\`.`;
  }
}

export class ThreadModelRequiredError extends Schema.TaggedError<ThreadModelRequiredError>()(
  "ThreadModelRequiredError",
  {},
) {
  override get message(): string {
    return "No default model. Pass --model.";
  }
}

export class ThreadProviderNotFoundError extends Schema.TaggedError<ThreadProviderNotFoundError>()(
  "ThreadProviderNotFoundError",
  { provider: Schema.String, available: Schema.Array(Schema.String) },
) {
  override get message(): string {
    return `Provider '${this.provider}' not found. Available: ${this.available.join(", ") || "none"}.`;
  }
}

export class ThreadModelNotFoundError extends Schema.TaggedError<ThreadModelNotFoundError>()(
  "ThreadModelNotFoundError",
  { model: Schema.String, available: Schema.Array(Schema.String) },
) {
  override get message(): string {
    return `Model '${this.model}' not found. Available: ${this.available.join(", ") || "none"}.`;
  }
}

export class ThreadModelAmbiguousError extends Schema.TaggedError<ThreadModelAmbiguousError>()(
  "ThreadModelAmbiguousError",
  { model: Schema.String, providers: Schema.Array(Schema.String) },
) {
  override get message(): string {
    return `Model '${this.model}' on ${this.providers.join(", ")}. Pass --provider.`;
  }
}

const threadCommandUuid = Crypto.Crypto.pipe(
  Effect.flatMap((crypto) => crypto.randomUUIDv4),
  Effect.orDie,
);

const readPrompt = Effect.fn("readThreadPrompt")(function* (prompt: string) {
  const text =
    prompt === STDIN_PROMPT
      ? yield* Stdio.Stdio.pipe(
          Effect.flatMap((stdio) => stdio.stdin.pipe(Stream.decodeText(), Stream.mkString)),
        )
      : prompt;
  const trimmed = text.trim();
  if (trimmed.length === 0) {
    return yield* new ThreadPromptEmptyError();
  }
  return trimmed;
});

const findProject = Effect.fn("findThreadProject")(function* (
  projects: ReadonlyArray<OrchestrationProjectShell>,
  identifier: string,
) {
  const trimmed = identifier.trim();
  const byId = projects.find((project) => project.id === trimmed);
  if (byId) {
    return byId;
  }
  const workspacePaths = yield* WorkspacePaths.WorkspacePaths;
  const workspaceRoot = normalizeProjectPathForComparison(
    yield* workspacePaths.normalizeWorkspaceRoot(trimmed).pipe(Effect.orElseSucceed(() => trimmed)),
  );
  const byWorkspaceRoot = projects.find(
    (project) => normalizeProjectPathForComparison(project.workspaceRoot) === workspaceRoot,
  );
  if (byWorkspaceRoot) {
    return byWorkspaceRoot;
  }
  return yield* new ThreadProjectNotFoundError({ identifier: trimmed });
});

const readServerSettings = (settingsPath: string) =>
  FileSystem.FileSystem.pipe(
    Effect.flatMap((fs) => fs.readFileString(settingsPath)),
    Effect.flatMap(decodeServerSettings),
    Effect.orElseSucceed(() => DEFAULT_SERVER_SETTINGS),
  );

const readCachedProviders = Effect.fn("readCachedProviders")(function* (cacheDir: string) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const entries = yield* fs.readDirectory(cacheDir).pipe(Effect.orElseSucceed(() => []));
  const providers = yield* Effect.forEach(
    entries.filter((entry) => entry.endsWith(".json")),
    (entry) => readProviderStatusCache(path.join(cacheDir, entry)),
  );
  return providers.filter((provider): provider is ServerProvider => provider !== undefined);
});

const resolveModelSelection = Effect.fn("resolveThreadModelSelection")(function* (input: {
  readonly model: string | undefined;
  readonly provider: string | undefined;
  readonly settings: ServerSettings;
  readonly projectDefault: ModelSelection | null;
  readonly cacheDir: string;
}) {
  const { model, provider, settings } = input;

  if (model === undefined && provider === undefined) {
    if (
      input.projectDefault === null ||
      !isModelSelectionProviderEnabled(settings, input.projectDefault)
    ) {
      return yield* new ThreadModelRequiredError();
    }
    return input.projectDefault;
  }

  const offered = (yield* readCachedProviders(input.cacheDir)).flatMap((candidate) =>
    candidate.enabled && candidate.installed
      ? candidate.models
          .map((entry) => ({
            entry,
            selection: { instanceId: candidate.instanceId, model: entry.slug },
          }))
          .filter(({ selection }) => isModelSelectionProviderEnabled(settings, selection))
      : [],
  );
  const scoped =
    provider === undefined
      ? offered
      : offered.filter(({ selection }) => selection.instanceId === provider);
  if (provider !== undefined && scoped.length === 0) {
    return yield* new ThreadProviderNotFoundError({
      provider,
      available: [...new Set(offered.map(({ selection }) => selection.instanceId))],
    });
  }

  if (model === undefined) {
    const fallback =
      scoped.find(({ entry }) => entry.isDefault && !entry.isCustom) ??
      scoped.find(({ entry }) => !entry.isCustom) ??
      scoped[0]!;
    return fallback.selection satisfies ModelSelection;
  }

  const matches = scoped.filter(
    ({ entry }) => entry.slug === model || entry.aliases?.includes(model) === true,
  );
  const [match, ...rest] = matches;
  if (match === undefined) {
    return yield* new ThreadModelNotFoundError({
      model,
      available: scoped.map(({ selection }) => `${selection.instanceId}/${selection.model}`),
    });
  }
  if (rest.length > 0) {
    return yield* new ThreadModelAmbiguousError({
      model,
      providers: matches.map(({ selection }) => selection.instanceId),
    });
  }
  return match.selection satisfies ModelSelection;
});

const runThreadStart = Effect.fn("runThreadStart")(function* (flags: {
  readonly baseDir: Option.Option<string>;
  readonly project: string;
  readonly prompt: string;
  readonly model: Option.Option<string>;
  readonly provider: Option.Option<string>;
  readonly title: Option.Option<string>;
  readonly json: boolean;
}) {
  const logLevel = yield* GlobalFlag.LogLevel;
  const config = yield* resolveCliAuthConfig(flags, logLevel);
  const prompt = yield* readPrompt(flags.prompt);

  return yield* Effect.gen(function* () {
    const runtimeState = yield* readPersistedServerRuntimeState(config.serverRuntimeStatePath);
    if (Option.isNone(runtimeState) || !isProcessAlive(runtimeState.value.pid)) {
      return yield* new ThreadServerNotRunningError();
    }
    const environmentAuth = yield* EnvironmentAuth.EnvironmentAuth;
    const client = yield* HttpApiClient.make(EnvironmentHttpApi, {
      baseUrl: runtimeState.value.origin,
    });

    const result = yield* Effect.acquireUseRelease(
      environmentAuth.issueSession({
        scopes: [AuthOrchestrationReadScope, AuthOrchestrationOperateScope],
        label: "t3 thread cli",
        ttl: Duration.minutes(5),
      }),
      (issued) =>
        Effect.gen(function* () {
          const headers = { authorization: `Bearer ${issued.token}` };
          const callServer = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
            effect.pipe(
              Effect.timeout(THREAD_CLI_LIVE_SERVER_TIMEOUT),
              Effect.mapError(projectCommandErrorFromLiveServerRequest),
            );
          const shell = yield* callServer(client.orchestration.shellSnapshot({ headers }));
          const project = yield* findProject(shell.projects, flags.project);
          const settings = yield* readServerSettings(config.settingsPath);
          const projectSettings = resolveProjectSettings(settings, project.id, project).settings;
          const modelSelection = yield* resolveModelSelection({
            model: Option.getOrUndefined(flags.model)?.trim(),
            provider: Option.getOrUndefined(flags.provider)?.trim(),
            settings,
            projectDefault: projectSettings.defaultModelSelection,
            cacheDir: config.providerStatusCacheDir,
          });
          const runtimeMode = projectSettings.defaultRuntimeMode;

          const threadId = ThreadId.make(yield* threadCommandUuid);
          const createdAt = DateTime.formatIso(yield* DateTime.now);
          const explicitTitle = Option.getOrUndefined(flags.title)?.trim() || undefined;
          const title = explicitTitle ?? truncate(prompt);
          yield* callServer(
            client.orchestration.dispatch({
              headers,
              payload: {
                type: "thread.create",
                commandId: CommandId.make(yield* threadCommandUuid),
                threadId,
                projectId: project.id,
                title,
                modelSelection,
                runtimeMode,
                interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
                branch: null,
                worktreePath: null,
                createdAt,
              },
            }),
          );
          yield* callServer(
            client.orchestration.dispatch({
              headers,
              payload: {
                type: "thread.turn.start",
                commandId: CommandId.make(yield* threadCommandUuid),
                threadId,
                message: {
                  messageId: MessageId.make(yield* threadCommandUuid),
                  role: "user",
                  text: prompt,
                  attachments: [],
                },
                modelSelection,
                ...(explicitTitle === undefined ? { titleSeed: title } : {}),
                runtimeMode,
                interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
                createdAt,
              },
            }),
          ).pipe(
            Effect.tapError(() =>
              Effect.gen(function* () {
                yield* callServer(
                  client.orchestration.dispatch({
                    headers,
                    payload: {
                      type: "thread.delete",
                      commandId: CommandId.make(yield* threadCommandUuid),
                      threadId,
                    },
                  }),
                );
              }).pipe(Effect.ignore({ log: true })),
            ),
          );

          return { threadId, project, modelSelection };
        }),
      (issued) =>
        environmentAuth.revokeSession(issued.sessionId).pipe(Effect.ignore({ log: true })),
    );

    yield* Console.log(
      flags.json
        ? yield* encodeThreadStartOutput({
            threadId: result.threadId,
            projectId: result.project.id,
            instanceId: result.modelSelection.instanceId,
            model: result.modelSelection.model,
          })
        : `Started thread ${result.threadId} in ${result.project.title} (${result.modelSelection.instanceId}/${result.modelSelection.model}).`,
    );
  }).pipe(
    Effect.provide(
      Layer.mergeAll(EnvironmentAuth.runtimeLayer, WorkspacePaths.layer).pipe(
        Layer.provideMerge(FetchHttpClient.layer),
        Layer.provide(ServerConfig.layer(config)),
        Layer.provide(
          Layer.succeed(References.MinimumLogLevel, flags.json ? "Error" : config.logLevel),
        ),
      ),
    ),
  );
});

const threadStartCommand = Command.make("start", {
  ...projectLocationFlags,
  project: Argument.String("project").pipe(Argument.withDescription("Project id or path.")),
  prompt: Argument.String("prompt").pipe(Argument.withDescription("Prompt, or `-` to read stdin.")),
  model: Flag.String("model").pipe(
    Flag.withDescription("Model slug, e.g. `claude-opus-5`. Default: project default."),
    Flag.optional,
  ),
  provider: Flag.String("provider").pipe(
    Flag.withDescription("Provider instance, e.g. `claudeAgent`."),
    Flag.optional,
  ),
  title: Flag.String("title").pipe(
    Flag.withDescription("Title. Default: prompt start."),
    Flag.optional,
  ),
  json: Flag.Boolean("json").pipe(Flag.withDescription("Print JSON."), Flag.withDefault(false)),
}).pipe(
  Command.withDescription("Start thread with prompt on running server."),
  Command.withHandler((flags) =>
    flags.json
      ? runThreadStart(flags).pipe(Effect.provideService(References.MinimumLogLevel, "Error"))
      : runThreadStart(flags),
  ),
);

export const threadCommand = Command.make("thread").pipe(
  Command.withDescription("Manage threads."),
  Command.withSubcommands([threadStartCommand]),
);
