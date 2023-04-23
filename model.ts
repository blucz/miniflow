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
  failed        = 'failed',
  disabled      = 'disabled' 
}

export function parseStepState(s: string) {
  switch (s) {
      case 'none': return StepState.none;         
      case 'waiting-for-run': return StepState.waitingForRun;
      case 'waiting-for-dependency': return StepState.waitingForDep;
      case 'dependency-failed': return StepState.depFailed;    
      case 'succeeded': return StepState.succeeded;    
      case 'running': return StepState.running;      
      case 'failed': return StepState.failed;       
      case 'disabled': return StepState.disabled;     
      default: throw new Error(`invalid step state ${s}`);
  }
}

export enum LogMode {
    file = 'file',
    console = 'console',
}

export class Step {
  constructor(name: string, stepToml: StepToml, state?: StepStateJson) {
    this.name = name;
    this.env  = new Map(Object.entries(stepToml.env ?? {}));
    this.cmd  = stepToml.cmd;
    this.cwd  = stepToml.cwd;
    this.desc = stepToml.desc;
    this.deps = [];
    this.tags = stepToml.tags ?? []

    ensureOnlyKeys(stepToml, [ "desc", "env", "cmd", "deps", "cwd", "log", "tags" ], `in step ${name}`);

    if (stepToml.log == undefined) {
        this.logMode = LogMode.file;
    } else if (stepToml.log == LogMode.console) {
        this.logMode = LogMode.console;
    } else if (stepToml.log == LogMode.file) {
        this.logMode = LogMode.file;
    } else {
        throw new Error(`invalid log mode ${stepToml.log} in step ${name}`);
    }

    if (state) {
      this.state = state.state;
      this.runs = (state.runs ?? []).map((run) => StepRun.fromJson(run));
      this.prevState = state.prevState;
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
  public prevState?: StepState;
  public runs: StepRun[];
  public desc: string | undefined;
  public logMode : LogMode;
  public tags: string[]
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

      } else if (this.state == StepState.disabled) {
        debug("    nothing to do (disabled)");

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
                 depStates.includes(StepState.waitingForRun) ||
                 depStates.includes(StepState.disabled)) {
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
    this.prevState = this.state;
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

    let errors = [];

    let cyclicalDepErrors: Array<string> = [];
    for (let step of this.steps) {
      let stepToml = flowToml.steps[step.name]!;
      step.deps = [];
      if (stepToml.deps && !Array.isArray(stepToml.deps)) {
        errors.push(`step.deps must be an array in step ${step.name}`);
      } else {
        for (let stepName of stepToml.deps ?? []) {
            let dep = stepsByName.get(stepName);
            if (dep) {
              step.deps.push(dep);
            } else {
              errors.push(`Dependency not found: ${step.name} => ${stepName}`);
            }
        }
      }
    }

    function checkCycles(step: Step, seen: Set<Step> = new Set()) {
      if (seen.has(step)) {
        cyclicalDepErrors.push(`${[...seen, step].map((s) => s.name).join(" => ")}`);
        return;
      }
      seen.add(step);
      for (let dep of step.deps) {
        checkCycles(dep, seen);
      }
      seen.delete(step);
    }
    for (let step of this.steps) {
      checkCycles(step);
    }

    if (cyclicalDepErrors.length > 0) {
      errors.push(`Cyclical dependencies found: ${cyclicalDepErrors.join(", ")}`);
    }

    if (errors.length > 0) {
      throw new Error(errors.join("\n"));
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
        prevState: step.prevState,
      };
    }
  }

  public resolveSteps(stepNames: Array<string>) : Array<Step> {
    const steps = [];
    for (let stepName of stepNames) {
      let found = this.steps.filter(x => x.name == stepName || x.tags.includes(stepName));
      for (let step of found) {
        steps.push(step);
      }
      if (found.length == 0) {
        throw new Error(`Error: couldn't find any steps for ${stepName}`);
      }
    }
    return steps;
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
  prevState?: StepState;
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
  tags?: string[];
  log: string;
}

