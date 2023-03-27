import { ensureOnlyKeys} from './utils.ts'
import { info, trace, error, debug, subproc } from "./log.ts";

//////////////////// In-Mem Model Objects /////////////////////////

export enum StepState {
  none          = 'none',
  waitingForRun = 'waiting-for-run',
  waitingForDep = 'waiting-for-dependency',
  depFailed     = 'dependency-failed',
  succeeded     = 'succeeded',
  running       = 'running',
  failed        = 'failed' 
}

export class Step {
  constructor(name: string, stepToml: StepToml, state?: StepStateJson) {
    this.name = name;
    this.env  = new Map(Object.entries(stepToml.env ?? {}));
    this.cmd  = stepToml.cmd;
    this.cwd  = stepToml.cwd;
    this.desc = stepToml.desc;
    this.deps = [];

    ensureOnlyKeys(stepToml, [ "desc", "env", "cmd", "deps", "cwd" ], `in step ${name}`);

    if (state) {
      this.state = state.state;
      this.runs = (state.runs ?? []).map((run) => StepRun.fromJson(run));
    } else {
      this.state = StepState.none;
      this.runs = [];
    }

    this.isDirty = this.state == StepState.none;

  }

  public name: string;
  public env: Map<string, string>;
  public cmd: string;
  public cwd: string;
  public isDirty: boolean;
  public state: StepState;
  public runs: StepRun[];
  public desc: string | undefined;

  public deps: Step[] = [];

  public ancestors: Set<Step> = new Set();
  public descendants: Set<Step> = new Set();
  public directDescendants: Set<Step> = new Set();

  public clean() : void {
    debug(`cleaning ${this.name}`);
    if (this.isDirty) {
      debug(`  REALLY cleaning ${this.name}`);
      for (let dep of this.deps) {
          debug(`    cleaning dep ${this.name}=>${dep.name}`);
        dep.clean();
      }

      let depStates = this.deps.map((dep) => dep.state);
      debug("clean", this.name, depStates);

      if (this.state == StepState.failed) {
        debug("    nothing to do (failed)");

      } else if (this.state == StepState.running) {
        debug("    nothing to do (running)");

      } else if (this.state == StepState.succeeded) {
        debug("    nothing to do (succeeded)");

      } else if (depStates.includes(StepState.failed) ||
          depStates.includes(StepState.depFailed)) {
        debug("    case 1 (depFailed)");
        this.transition(StepState.depFailed);

      } else if (depStates.includes(StepState.running) ||
                 depStates.includes(StepState.waitingForDep) ||
                 depStates.includes(StepState.waitingForRun)) {
        debug("    case 2 (waitingForDep)");
        this.transition(StepState.waitingForDep);

      } else if (depStates.length == 0 && this.state == StepState.none) {
        debug("    case 3 (waitingForRun)");
        this.transition(StepState.waitingForRun);

      } else if (depStates.length == 0) {
        debug("    case 4 (no deps, no change)");
        this.transition(this.state);

      } else if (depStates.every(s => s == StepState.succeeded)) {
        debug("    case 5 (all deps success)");
        this.transition(StepState.waitingForRun);
      } else {
        debug("    fallthrough");
      }

      this.isDirty = false;
    }
  }

  public dirty() {
    this.isDirty = true;
  }

  public transition(state: StepState) {
    if (state == this.state) return;
    trace(`[${this.name}] ${this.state} => ${state}`);
    this.state = state;
    this.dirty();
  }
}

function ancestors(step: Step) : Set<Step> {
  function accum(acc: Set<Step>, step: Step) : Set<Step> {
    for (let d of step.deps) {
      if (!(acc.has(d))) {
        acc.add(d);
        accum(acc, d);
      }
    }
    return acc;
  }
  return accum(new Set(), step);
}

export class Flow {
  public env: Map<string, string>;
  public steps: Array<Step> 

  public finalSteps: Array<Step>
  public initialSteps: Array<Step>

  constructor(flowToml: FlowToml, stateJson: StateJson) {
    ensureOnlyKeys(flowToml, [ "env", "steps" ], `at toplevel`);

    this.env   = new Map(Object.entries(flowToml.env));
    this.steps = Object.entries(flowToml.steps).map(([name,stepToml]) => new Step(name, stepToml, stateJson.steps[name]));

    let stepsByName = new Map(this.steps.map((step) => [step.name, step]));

    for (let step of this.steps) {
      let stepToml = flowToml.steps[step.name]!;
      step.deps = [];
      for (let stepName of stepToml.deps ?? []) {
          let dep = stepsByName.get(stepName);
          if (!dep) throw new Error(`Step not found when processing dependencies: ${stepName}`);
          step.deps.push(dep);
      }
    }

    for (let step of this.steps) {
      step.ancestors   = ancestors(step);
    }

    for (let step of this.steps) {
      step.descendants = new Set(this.steps.filter((s) => s.ancestors.has(step)));
    }

    for (let step of this.steps) {
      step.directDescendants = new Set(this.steps.filter((s) => s.deps.includes(step)));
    }

    this.initialSteps = this.steps.filter((step) => step.ancestors.size == 0);
    this.finalSteps = this.steps.filter((step) => step.descendants.size == 0);
  }

  public clean(): void {
    for (let step of this.steps) {
      step.clean();
    }
  }

  public dirty(): void {
    for (let step of this.steps) {
      step.dirty();
    }
  }

  /**
   * Update the state in `stateJson` to reflect the current state of the world.
   *
   * This is done as an update as opposed to a `toJson` so that we don't disturb information 
   * about missing states. Users of the system may comment out parts of their `.toml` 
   * configuration temporarily and we shouldn't lose track of their runs when they do that.
   *
   * XXX: when steps reappear, do we need to reset it to StepState.none?
   */
  public update(stateJson: StateJson) {
    for (let step of this.steps) {
      stateJson.steps[step.name] = {
        runs:  step.runs.map((run) => run.toJson()),
        state: step.state,
      };
    }
  }
}

export class StepRun {
  constructor(startTimestamp: Date, endTimestamp?: Date, exitCode?: number, logFile?: string) {
    this.startTimestamp = startTimestamp;
    this.endTimestamp   = endTimestamp;
    this.exitCode       = exitCode;
    this.logFile        = logFile;
  }

  public startTimestamp: Date;
  public endTimestamp?: Date;
  public exitCode?: number;
  public logFile?: string;

  public get durationMs(): number | undefined {
    return this.endTimestamp ? (this.endTimestamp.getTime() - this.startTimestamp.getTime()) : undefined;
  }

  public toJson(): StepRunJson {
    return {
      startTimestamp: this.startTimestamp.toISOString(),
      endTimestamp:   this.endTimestamp?.toISOString(),
      exitCode:       this.exitCode,
      logFile:        this.logFile,
    };
  }

  public static fromJson(json: StepRunJson): StepRun {
    return new StepRun(
      new Date(json.startTimestamp),
      json.endTimestamp ? new Date(json.endTimestamp) : undefined,
      json.exitCode,
      json.logFile,
    );
  }
}

//////////////////// On-Disk Formats //////////////////////

export interface StepRunJson {
  startTimestamp: string; // ISO8660 Date
  endTimestamp?: string;   // ISO8660 Date
  exitCode?: number;
  logFile?: string;
}

export interface StepStateJson {
  state: StepState;
  runs: StepRunJson[];
}

export interface StateJson {
  version: number;
  steps: { [key: string]: StepStateJson };
}

export interface FlowToml {
  env: { [key: string]: string };
  steps: { [key: string]: StepToml };
}

export interface StepToml {
  env: { [key: string]: string };
  cmd: string;
  cwd: string;
  desc?: string;
  deps: string[];
}

