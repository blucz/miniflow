import { parse } from "https://deno.land/std@0.175.0/flags/mod.ts";
import { exists, ensureDir } from "https://deno.land/std/fs/mod.ts";
import { dirname, join } from "https://deno.land/std/path/mod.ts";
import { formatDistance } from "https://cdn.skypack.dev/date-fns";
import * as toml from "https://deno.land/std@0.180.0/encoding/toml.ts";
import * as colors from "https://deno.land/std@0.179.0/fmt/colors.ts";

import { FlowToml, StateJson, Flow, Step, StepState, StepRun } from "./model.ts";
import { formatDuration, truncateOneLine, mergeStreams, logSubprocessToConsole } from "./utils.ts";

import { info, trace, error, debug } from "./log.ts";

const APP_NAME              = "miniflow";
const STATE_JSON_FILENAME   = "state.json";
const DEFAULT_FLOW_FILENAME = `${APP_NAME}.toml`;
const STATE_DIR_NAME        = `_${APP_NAME}`;
const DEFAULT_STATE         = { version: 1, steps: { } };
const MAX_RUNS_TO_RETAIN    = 10;

function usage() {
  console.error(`Usage: ${APP_NAME} <command> [args...]`);
  console.error("");
  console.error("COMMANDS");
  console.error("");
  console.error("   run                       Run the flow, attempting to make progress based on the current state.");
  console.error("   reset [--only] [step]...  Reset a step + its dependents. If no step is provided, reset all steps");
  console.error("   purge                     Clear all state associated with this flow and start from scratch");
  console.error("   status                    Show short-form flow status");
  console.error("   inspect                   Show detailed information about the flow for debugging");
  console.error("   logs [step]               Print logs from the last run of a step");
  console.error("");
  console.error("OPTIONS"); 
  console.error("");
  console.error(`    -d --flow <${DEFAULT_FLOW_FILENAME}>  Specify the flow file to use. Defaults to ${DEFAULT_FLOW_FILENAME}`);
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
  console.error("");
  console.error("    [steps.print-world]");
  console.error("    cmd = 'echo world'           # Command to run");
  console.error("    deps = ['print-hello']       # List of steps that this step depends on");
  console.error("    cwd = '.'                    # Working directory (optional, defaults to flow file directory)");
  console.error("    env = { 'ENV_VAR': 'value' } # Environment variables (optional)");
  console.error("");
  console.error("NOTES");
  console.error("");
  console.error(`    ${APP_NAME} stores state information and logs in the ${STATE_DIR_NAME} folder. Add ${STATE_DIR_NAME}`);
  console.error(`    to your .gitignore.`);
  console.error("");
  Deno.exit(1);
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

  public logFlow(): void {
    info(`---[ Flow Status ]----------------------------`);
    for (let step of this.flow.steps) {
      let colorstate = step.state.toString();
      let stateNameMaxLen = Object.values(StepState).reduce((max, state) => Math.max(max, state.length), 0);
      switch (step.state) {
        case StepState.none:          colorstate = colors.gray(step.state.padEnd(stateNameMaxLen));      break;
        case StepState.waitingForRun: colorstate = colors.yellow(step.state.padEnd(stateNameMaxLen));    break;
        case StepState.waitingForDep: colorstate = colors.yellow(step.state.padEnd(stateNameMaxLen));    break;
        case StepState.depFailed:     colorstate = colors.red(step.state.padEnd(stateNameMaxLen));       break;
        case StepState.succeeded:     colorstate = colors.green(step.state.padEnd(stateNameMaxLen));     break;
        case StepState.running:       colorstate = colors.blue(step.state.padEnd(stateNameMaxLen));      break;
        case StepState.failed:        colorstate = colors.brightRed(step.state.padEnd(stateNameMaxLen)); break;
      }
      let stepNameMaxLen = this.flow.steps.reduce((max, step) => Math.max(max, step.name.length), 0);

      let runinfo = "";
      if (step.runs.length > 0 && (step.state == StepState.succeeded || step.state == StepState.failed)) {
        let run = step.runs[0];
        if (step.state == StepState.succeeded || step.state == StepState.failed) {
          runinfo = colors.gray(`finished with exit code [${run.exitCode}] after ${formatDuration(run.durationMs!)} (${formatDistance(run.endTimestamp, new Date(), { addSuffix: true })})`);
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

async function runStep(ctx: Context, step: Step) : Promise<void> {
  let startTime = new Date();
  const filenameSafeDate = startTime.toISOString().replace(/:/g, "-").replace(/\./g, "-");
  let logfile = join(ctx.stateDir, "logs", step.name,  `run-${filenameSafeDate}.txt`);

  await ensureDir(dirname(logfile));
  step.transition(StepState.running);
  let run = new StepRun(startTime);
  run.logFile = logfile;
  step.runs.unshift(run);

  while (step.runs.length > MAX_RUNS_TO_RETAIN) {
    const runToDelete = step.runs.pop()!;
    if (runToDelete.logFile) {
      try { await Deno.remove(runToDelete.logFile); } catch { }
    }
  }

  await ctx.save();

  info(`[${step.name}] Starting step ${step.name} at ${startTime.toISOString()}`);
  trace(`[${step.name}]     cmd: ${step.cmd}`);
  trace(`[${step.name}]     cwd: ${step.cwd}`);

  for (let [env,val] of Object.entries(ctx.flow.env)) { Deno.env.set(env, val); }
  for (let [env,val] of Object.entries(step.env))     { Deno.env.set(env, val); }
  let p = Deno.run({
    cmd: [ "sh", "-c", step.cmd ],
    cwd: step.cwd,
    stdin: "null",
    stdout: "piped",
    stderr: "piped",
  });

  let logFileStream = await Deno.open(logfile, { write: true, create: true, append: true });

  try {
    let [stdout1, stdout2] = p.stdout!.readable.tee();
    let [stderr1, stderr2] = p.stderr!.readable.tee();

    let mergePromise = mergeStreams(stdout1, stderr1, logFileStream);
    let logPromise = logSubprocessToConsole(step.name, stdout2, stderr2);

    let exitCode = await p.status();
    await Promise.all([mergePromise, logPromise]);

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
    runLoop(ctx);

  } finally {
    await logFileStream.close();
    try { p.kill("SIGKILL"); } catch { }
  }
}

function runLoop(ctx: Context) {
  let waitingForRunSteps = ctx.flow.steps.filter(step => step.state == StepState.waitingForRun);

  for (let step of waitingForRunSteps) {
    ctx.pending.push(runStep(ctx, step));
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

  if (args.length > 0) {
    console.error("run doesn't take any arguments");
    Deno.exit(1);
  }

  let waitingForRunSteps = ctx.flow.steps.filter(step => step.state == StepState.waitingForRun);
  if (waitingForRunSteps.length == 0) {
    info("Nothing to do");
    ctx.logFlow();
    Deno.exit(0);
  }

  runLoop(ctx);

  while (ctx.pending.length > 0) {
    await ctx.waitAll();
  }

  ctx.logFlow();

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
      state: step.state,
      deps: step.deps.map(dep => dep.name),
      cmd: truncateOneLine(step.cmd),
      cwd: step.cwd,
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
  let steps = [];

  if (stepNames.length > 0) {
    for (let stepName of stepNames) {
      let step = ctx.flow.steps.find(x => x.name == stepName);
      if (!step) {
        console.error(`Error: step ${stepName} does not exist`);
        Deno.exit(1);
      }
      steps.push(step);
    }
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

  ctx.logFlow();
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
  }

  if (args.length == 0) { usage(); }

  let command = args.shift();
  if (!(await exists(flowFile))) {
      console.error(`File Not found: ${flowFile}`);
      Deno.exit(1);
  }

  let stateDir = await ensureStateDir(flowFile);
  let state    = await loadState(stateDir);
  let flow     = await loadFlow(flowFile, state);

  let ctx      = new Context(flow, state, flowFile, stateDir);

  flow.clean();

  if (command == "run") {
    await cmdRun(ctx, args);
  } else if (command == "reset") {
    await cmdReset(ctx, args);
  } else if (command == "purge") {
    await cmdPurge(ctx, args);
  } else if (command == "status") {
    ctx.logFlow();
  } else if (command == "inspect") {
    await cmdInspect(ctx, args);
  } else if (command == "logs") {
    await cmdLogs(ctx, args);
  } else {
    console.error("Unknown command: " + command);
  }
}

await main();


