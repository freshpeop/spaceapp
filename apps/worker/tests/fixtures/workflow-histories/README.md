# Temporal replay histories

These are synthetic executions recorded by the real time-skipping Temporal
server, not hand-written or edited histories. Each JSON document contains the
original `workflowId` and its `history`. Replay uses that original ID, because
workflow code may derive commands from `workflowInfo().workflowId`.

`npm test -w @space/worker` runs production workflow code with stub activities,
checks signals, cancellation, retry policies and continuations, then replays
**all committed versions**. Recording additionally replays all newly observed
histories before writing them. A test-only
extra timer must produce `DeterminismViolationError`. Normal tests never write
history files and fail if the committed corpus is absent.

The legacy bundle removes only the two named `patched()` checks before
bundling. Its histories therefore have no corresponding patch marker. Replay
always uses the unmodified current production bundle. The real SDK decides
which old branch is compatible; we do not mock `patched()` during replay.

## Adding histories

After the initial `v1` corpus, recording is allowed **only together with a
reviewed, replay-safe `patched()` workflow change**. Never re-record to make a
failing replay test pass, and never delete the histories of an older branch.

1. Run `npm run check` to build the workspace dependencies.
2. Run `npm run temporal:record-histories -w @space/worker -- <new-version>`.
3. Review the patch and newly added synthetic histories together. Existing
   version files must remain byte-for-byte unchanged.
4. Run `npm test -w @space/worker` normally, without the recording variable.

Recording first replays every existing history. It then runs the same scenario
assertions and negative control; any failure prevents recording. It requires
a new version directory and uses exclusive file creation. A partially created
version from a failed disk write is not silently reused.

Node 22 and the locked Temporal SDK 1.24.0 are required. The first test run may
download Temporal's free test-server executable; subsequent runs reuse its
cache. Tests use localhost gRPC, not external services, databases, Docker or AI
accounts. Each scenario uses its own server because time skipping is global.
Endless supervisors advance in bounded one-second steps: awaiting their final
result would fast-forward an idle server to its default ten-year timeout.
CI asserts that the sandbox/capture/replay suite finishes in under 180 seconds.

Activity failures and timeout causes are synthetic inputs. Temporal performs
their serialization, activity retries and propagation through actual child
workflows. Long-lived supervisors are explicitly terminated by the test after
their state has been asserted; termination is not claimed as normal completion.
