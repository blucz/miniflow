import { parse } from "https://deno.land/std@0.175.0/flags/mod.ts";
import { exists, ensureDir } from "https://deno.land/std/fs/mod.ts";
import { dirname, join } from "https://deno.land/std/path/mod.ts";
import { formatDistance } from "https://cdn.skypack.dev/date-fns";
import * as toml from "https://deno.land/std@0.180.0/encoding/toml.ts";
import * as colors from "https://deno.land/std@0.179.0/fmt/colors.ts";

import { FlowToml, StateJson, Flow, Step, StepState, StepRun, LogMode, parseStepState } from "./model.ts";
import { formatDuration, truncateOneLine, mergeStreams } from "./utils.ts";

import { info, trace, error, debug, setDebug } from "./log.ts";

const APP_NAME              = "miniflow";
const STATE_JSON_FILENAME   = "state.json";
const DEFAULT_FLOW_FILENAME = `${APP_NAME}.toml`;
const STATE_DIR_NAME        = `_${APP_NAME}`;
const DEFAULT_STATE         = { version: 1, steps: { } };
const MAX_RUNS_TO_RETAIN    = 10;

function printStates() {
  console.error("   - none");
  console.error("   - waiting-for-run");
  console.error("   - waiting-for-dependency");
  console.error("   - dependency-failed");
  console.error("   - succeeded");
  console.error("   - running");
  console.error("   - failed");
  console.error("   - disabled");
}

function usage() {
  console.error(`Usage: ${APP_NAME} <command> [args...]`);
  console.error("");
  console.error("COMMANDS");
  console.error("");
  console.error("   run                       Run the flow, attempting to make progress based on the current state.");
  console.error("   reset [--only] [step]...  Reset a step + its dependents. If no step is provided, reset all steps");
  console.error("   set [state] [step]....    Manually set steps to a certain state")
  console.error("   disable [step...]         Disable steps. ")
  console.error("   enable [step...]          Re-enable steps. They will be restored to the last prior non-disabled state")
  console.error("   purge                     Clear all state associated with this flow and start from scratch");
  console.error("   status                    Show short-form flow status");
  console.error("   inspect                   Show detailed information about the flow for debugging");
  console.error("   logs [step]               Print logs from the last run of a step");
  console.error("");
  console.error("STATES");  
  console.error("");
  printStates();
  console.error("");
  console.error("OPTIONS"); 
  console.error("");
  console.error(`    -f --flow <${DEFAULT_FLOW_FILENAME}>  Specify the flow file to use. Defaults to ${DEFAULT_FLOW_FILENAME}`);
  console.error("    -e --env 'KEY=value'       Set an environment variable");
  console.error("    -h --help                  Show this help message");
  console.error("");
  console.error("FORMAT of miniflow.toml");
  console.error("");
  console.error("    [env]");
  console.error("    ENV_VAR=value                # Environment variables that apply to the whole flow");
  console.error("");
  console.error("    [steps.print-hello]");
  console.error("    cmd = 'echo hello'           # Command to run");
  console.error("    cwd = '.'                    # Working directory (optional, defaults to flow file directory)");
  console.error("    env = { 'ENV_VAR': 'value' } # Environment variables (optional)");
  console.error("    tags = [ 'tag1', ... ]       # Tags (optional)");
  console.error("");
  console.error("    [steps.print-world]");
  console.error("    desc = 'Prints \"world\"'     # Short human-readbale description of what the step does");
  console.error("    cmd  = 'echo world'           # Command to run");
  console.error("    deps = ['print-hello']        # List of steps that this step depends on");
  console.error("    cwd  = '.'                    # Working directory (optional, defaults to flow file directory)");
  console.error("    env  = { 'ENV_VAR': 'value' } # Environment variables (optional)");
  console.error("    tags = [ 'tag1', ... ]       # Tags (optional)");
  console.error("");
  console.error("NOTES");
  console.error("");
  console.error(`    ${APP_NAME} stores state information and logs in the ${STATE_DIR_NAME} folder. Add ${STATE_DIR_NAME}`);
  console.error(`    to your .gitignore.`);
  console.error("");
  Deno.exit(1);
}

type Colorizer = (input: string) => string;

function getColorizer(state: StepState): Colorizer {
  switch (state) {
    case StepState.none:          return colors.gray;        break;
    case StepState.waitingForRun: return colors.yellow;      break;
    case StepState.waitingForDep: return colors.yellow;      break;
    case StepState.depFailed:     return colors.red;         break;
    case StepState.succeeded:     return colors.green;       break;
    case StepState.running:       return colors.brightGreen; break;
    case StepState.failed:        return colors.brightRed;   break;
    default: return (s) => s;
  }
}

class Context {
  constructor(flow: Flow, state: StateJson, flowFile: string, stateDir: string) {
    this.flow = flow;
    this.state = state;
    this.flowFile = flowFile;
    this.stateDir = stateDir;
    this.pending = [];
  }

  public flow: Flow;
  public state: StateJson;
  public flowFile: string;
  public stateDir: string;
  public pending: Promise<void>[];

  public async save(): Promise<void> {
    this.flow.update(this.state);
    await Deno.writeTextFile(join(this.stateDir, STATE_JSON_FILENAME), JSON.stringify(this.state));
  }

  public async waitAll() : Promise<void> {
    if (this.pending.length > 0) {
      info("Waiting for all steps to complete...");
      let pending = this.pending;
      this.pending = [];
      await Promise.all(pending);
    }
  }

  public logStatus(): void {
    info(`---[ Flow Status ]----------------------------`);
    for (let step of this.flow.steps) {
      let colorstate = step.state.toString();
      let stateNameMaxLen = Object.values(StepState).reduce((max, state) => Math.max(max, state.length), 0);
      colorstate = getColorizer(step.state)(step.state.padEnd(stateNameMaxLen));
      let stepNameMaxLen = this.flow.steps.reduce((max, step) => Math.max(max, step.name.length), 0);

      let runinfo = "";
      if (step.runs.length > 0 && (step.state == StepState.succeeded || step.state == StepState.failed)) {
        let run = step.runs[0];
        if (step.state == StepState.succeeded || step.state == StepState.failed) {
          runinfo = colors.gray(`exit(${run.exitCode}) after ${formatDuration(run.durationMs!)} (${formatDistance(run.endTimestamp, new Date(), { addSuffix: true })})`);
        } else {
          runinfo = colors.gray(`Running as of ${formatDistance(run.startTimestamp, new Date(), { addSuffix: true })}`);
        }
      }

      info(`${step.name.padEnd(stepNameMaxLen)}  ${colorstate}  ${runinfo}`);
    }
    info(`----------------------------------------------`);
  }
};

async function ensureStateDir(path: string) : Promise<string> {
  let dir = dirname(path);
  let stateDir = join(dirname(path), STATE_DIR_NAME);

  await ensureDir(stateDir);

  let stateFileExists = await exists(join(stateDir, STATE_JSON_FILENAME));
  if (!stateFileExists) {
    await Deno.writeTextFile(join(stateDir, STATE_JSON_FILENAME), JSON.stringify(DEFAULT_STATE));
  }

  let logsDir = join(stateDir, "logs");
  let logsDirExists = await exists(logsDir);
  if (!logsDirExists) {
    await ensureDir(logsDir);
  }

  return stateDir;
}

async function loadState(stateDir: string) : Promise<StateJson> {
  let stateFile = join(stateDir, STATE_JSON_FILENAME);
  let stateJson;
  try {
    stateJson = await Deno.readTextFile(stateFile);
  } catch (e) {
    console.error(`Error reading file ${stateFile}: ${e}`);
    Deno.exit(1);
  }
  try {
    return JSON.parse(stateJson);
  } catch (e) {
    console.error(`Couldn't parse ${stateFile}: ${e}`);
    Deno.exit(1);
  }
}

async function loadFlow(flowFile: string, state: StateJson) : Promise<Flow> {
  let flowText;
  try {
    flowText = await Deno.readTextFile(flowFile);
  } catch (e) {
    console.error(`Error reading file ${flowFile}: ${e}`);
    Deno.exit(1);
  }

    let flowToml = toml.parse(flowText) as unknown as FlowToml;
    let flow = new Flow(flowToml, state);
    return flow;
  try {
  } catch (e) {
    console.error(`Couldn't parse ${flowFile}: ${e}`);
    Deno.exit(1);
  }
}

async function runStep(activeSteps: Step[], ctx: Context, step: Step) : Promise<void> {
  activeSteps.push(step);

  let startTime = new Date();
  const filenameSafeDate = startTime.toISOString().replace(/:/g, "-").replace(/\./g, "-");
  let logFile = join(ctx.stateDir, "logs", step.name,  `run-${filenameSafeDate}.txt`);
  let latestLogFile = join(ctx.stateDir, "logs", step.name,  `run.txt`);

  await ensureDir(dirname(logFile));
  step.transition(StepState.running);
  let run = new StepRun(startTime);
  run.logFile = logFile;
  step.runs.unshift(run);

  while (step.runs.length > MAX_RUNS_TO_RETAIN) {
    const runToDelete = step.runs.pop()!;
    if (runToDelete.logFile) {
      try { await Deno.remove(runToDelete.logFile); } catch { }
    }
  }

  await ctx.save();

  let cwd = step.cwd ?? dirname(ctx.flowFile);

  info(`[${step.name}] Starting step ${step.name} at ${startTime.toISOString()}`);
  trace(`[${step.name}]     cmd: ${step.cmd}`);
  trace(`[${step.name}]     cwd: ${cwd}`);

  let globalenv = Array.from(ctx.flow.env.entries()).map(([env,val]) => `export ${env}=${val}`).join("\n");
  let localenv  = Array.from(step.env.entries()).map(([env,val]) => `export ${env}=${val}`).join("\n");

  let scriptFile = join(ctx.stateDir, "scripts", `${step.name}.bash`);
  await ensureDir(dirname(scriptFile));

  let logStuff = step.logMode == LogMode.file ? `exec 2>&1 >${logFile}
rm -f ${latestLogFile}
ln -s \`realpath ${logFile}\` ${latestLogFile}` : "";

  info(`Logs for step ${step.name} will be written to ${logFile}`);

  let scriptContents =`
set -e -o pipefail
${logStuff}
${globalenv}
${localenv}
cd ${cwd}
${step.cmd}`;

  await Deno.writeTextFile(scriptFile, scriptContents);
  
  let p = Deno.run({
    cmd: [ "bash", scriptFile ],
    stdin: "null",
  });

  try {
    let exitCode = await p.status();

    run.exitCode = exitCode.code;
    run.endTimestamp = new Date();
    if (exitCode.success) {
      info(`[${step.name}] Succeeded with code ${exitCode.code} after ${formatDuration(run.durationMs!)}`);
      step.transition(StepState.succeeded);
      await ctx.save();
    } else {
      error(`[${step.name}] Failed with code ${exitCode.code} after ${formatDuration(run.durationMs!)}`);
      step.transition(StepState.failed);
      await ctx.save();
    }

    for (let desc of step.descendants) {
      desc.dirty();
    }

    ctx.flow.clean();
    await ctx.save();
    runLoop(activeSteps, ctx);

  } finally {
    try { p.kill("SIGKILL"); } catch { }
  }
}

function runLoop(activeSteps: Step[], ctx: Context) {
  let waitingForRunSteps = ctx.flow.steps.filter(step => step.state == StepState.waitingForRun);

  for (let step of waitingForRunSteps) {
    ctx.pending.push(runStep(activeSteps, ctx, step));
  }
}

async function cmdRun(ctx: Context, args: string[]) : Promise<void> {
  let failedSteps = ctx.flow.steps.filter(step => step.state == StepState.failed);
  if (failedSteps.length > 0) {
    info(`Resetting failed steps: ${failedSteps.map(step => step.name).join(", ")}`);
    for (let failedStep of failedSteps) {
      failedStep.transition(StepState.none);
      for (let desc of failedStep.descendants) {
        desc.transition(StepState.none);
      }
    }
    ctx.flow.clean();
    ctx.save();
  }

  let activeSteps : Step[] = [];

  if (args.length > 0) {
    console.error("run doesn't take any arguments");
    Deno.exit(1);
  }

  let waitingForRunSteps = ctx.flow.steps.filter(step => step.state == StepState.waitingForRun);
  if (waitingForRunSteps.length == 0) {
    info("Nothing to do");
    ctx.logStatus();
    Deno.exit(0);
  }

  runLoop(activeSteps, ctx);

  let interval = setInterval(() => { 
    let runningStepStatuses: string[] = [];
    let notRunningStepStatuses: string[]  = [];

    activeSteps.sort((a,b) => a.runs[0].startTimestamp.getTime() - b.runs[0].startTimestamp.getTime());
    let runningSteps    = activeSteps.filter(x => x.state == StepState.running);
    let notRunningSteps = activeSteps.filter(x => x.state != StepState.running);

    let now = new Date();
    for (let step of runningSteps) {
      let run = step.runs[0];
        let elapsed = now.getTime() - run.startTimestamp.getTime();
        runningStepStatuses.push(`${getColorizer(step.state)(step.name)} (${formatDuration(elapsed)})`);
    }

    for (let step of notRunningSteps) {
      //stepStatuses.push(`${getColorizer(step.state)(step.name)} (${formatDistance(run.endTimestamp!, now, { addSuffix: true })})`);
      notRunningStepStatuses.push(`${getColorizer(step.state)(step.name)}`);
    }

    let sections = [ notRunningStepStatuses, runningStepStatuses ].filter(x => x.length > 0);
    let joined   = sections.map(x => x.join(" ")).join(" <= ");
    let statusLine = `[ ${joined} ]\r`

    Deno.writeAllSync(Deno.stdout, new TextEncoder().encode(statusLine));
  }, 100);
  while (ctx.pending.length > 0) {
    await ctx.waitAll();
  }
  clearInterval(interval);

  ctx.logStatus();

  if (ctx.flow.steps.filter(step => step.state == StepState.failed).length > 0) {
    Deno.exit(1);
  } else {
    Deno.exit(0);
  }
}

async function cmdLogs(ctx: Context, args: string[]) : Promise<void> {
}

async function cmdInspect(ctx: Context, args: string[]) : Promise<void> {
  function prepareRunForDisplay(run: StepRun): unknown {
    let now = new Date();
    if (run.endTimestamp) {
      return {
        end:      formatDistance(run.endTimestamp, now, { addSuffix: true }),
        duration: `${formatDuration(run.durationMs!)}`,
        exitCode: run.exitCode
      };
    } else {
      return {
        start: formatDistance(run.startTimestamp, now, { addSuffix: true }),
      };
    }
  }
  console.log({
    env: Object.fromEntries(ctx.flow.env),
    initialSteps: ctx.flow.initialSteps.map(step => step.name),
    finalSteps: ctx.flow.finalSteps.map(step => step.name),
    steps: ctx.flow.steps.map(step => ({
      name: step.name,
      tags: step.tags,
      state: step.state,
      deps: step.deps.map(dep => dep.name),
      cmd: truncateOneLine(step.cmd),
      cwd: step.cwd,
      desc: step.desc,
      lastRun: step.runs.length == 0 ? "(not run yet)" : prepareRunForDisplay(step.runs[0]),
      env: Object.fromEntries(step.env),
    })),
  });
}

async function cmdReset(ctx: Context, args: string[]) : Promise<void> {
  let only = false;

  if (args[0] == "--only") {
    args.shift();
    only=true;
  }

  let stepNames = args;
  let steps: Array<Step>;

  if (stepNames.length > 0) {
    steps = ctx.flow.resolveSteps(stepNames);
  } else {
    steps = ctx.flow.steps;
  }

  for (let step of steps) {
    step.transition(StepState.none);
    if (!only) {
      for (let desc of step.descendants) {
        desc.transition(StepState.none);
      }
    }
  }

  ctx.flow.clean();
  await ctx.save();

  ctx.logStatus();
}

async function cmdDisable(ctx: Context, args: string[]) : Promise<void> {
  let only = false;

  let stepNames = args;
  let steps = ctx.flow.resolveSteps(stepNames);

  if (steps.length == 0) {
    console.error(`Error: no steps provided`);
    Deno.exit(1);
  }

  for (let step of steps) {
    step.transition(StepState.disabled);
  }

  ctx.flow.clean();
  await ctx.save();

  ctx.logStatus();
}

async function cmdEnable(ctx: Context, args: string[]) : Promise<void> {
  let only = false;

  let stepNames = args;
  let steps = ctx.flow.resolveSteps(stepNames);

  if (steps.length == 0) {
    console.error(`Error: no steps provided`);
    Deno.exit(1);
  }

  for (let step of steps) {
    step.transition(step.prevState ?? StepState.none);
  }

  ctx.flow.clean();
  await ctx.save();

  ctx.logStatus();
}

async function cmdSet(ctx: Context, args: string[]) : Promise<void> {
  let only = false;

  if (args.length < 2) {
      console.error(`Error: expected state and steps. Valid steps include:`);
      console.error("");
      printStates();
      Deno.exit(1);
  }

  let state = parseStepState(args.shift()!);
  let stepNames = args;
  let steps = ctx.flow.resolveSteps(stepNames);

  for (let step of steps) {
    step.transition(state);
  }

  ctx.flow.clean();
  await ctx.save();

  ctx.logStatus();
}

async function cmdPurge(ctx: Context, args: string[]) : Promise<void> {
  if (args.length > 0) {
    console.error("Error: purge command does not take any arguments");
    Deno.exit(1);
  }
  console.log(`deleted ${await Deno.realPath(ctx.stateDir)}`);
  await Deno.remove(ctx.stateDir, { recursive: true });
}

async function main() {
  const args = Array.from(Deno.args) as string[];

  if (args.some(arg => arg == "-h" || arg == "--help")) {
    usage();
  }

  let flowFile = DEFAULT_FLOW_FILENAME;
  let debug = false;

  // Process options
  while (args.length > 0 && args[0].startsWith("-")) {
    if (args[0] == '-f' || args[0] == '--flow') {
      args.shift();
      flowFile = args.shift()!;
    }
    if (args[0] == '-e' || args[0] == '--env') {
      args.shift();
      let [key, value] = args.shift()!.split("=");
      Deno.env.set(key, value);
    }
    if (args[0] == '--debug') {
        info("Enabling debug mode");
        args.shift();
        setDebug(true);
        debug = true;
    }
  }

  if (args.length == 0) { usage(); }

  let command = args.shift();
  if (!(await exists(flowFile))) {
      console.error(`File Not found: ${flowFile}`);
      Deno.exit(1);
  }

  async function body(): Promise<void> {
    let stateDir = await ensureStateDir(flowFile);
    let state    = await loadState(stateDir);
    let flow     = await loadFlow(flowFile, state);
    let ctx      = new Context(flow, state, flowFile, stateDir);

    if (command == "run") {
      flow.clean();
      await cmdRun(ctx, args);

    } else if (command == "reset") {
      flow.clean();
      await cmdReset(ctx, args);

    } else if (command == "set") {
      flow.clean();
      await cmdSet(ctx, args);

    } else if (command == "disable") {
      flow.clean();
      await cmdDisable(ctx, args);

    } else if (command == "enable") {
      flow.clean();
      await cmdEnable(ctx, args);

    } else if (command == "purge") {
      await cmdPurge(ctx, args);

    } else if (command == "status") {
      ctx.logStatus();

    } else if (command == "inspect") {
      await cmdInspect(ctx, args);

    } else if (command == "logs") {
      await cmdLogs(ctx, args);

    } else {
      console.error("Unknown command: " + command);
    }
  }

  if (debug) {
    await body();
  } else {
    try {
      await body();
    } catch (e) {
      console.error(`Error: ${e.message}`);
      Deno.exit(1);
    }
  }
}

await main();


