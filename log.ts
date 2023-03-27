import * as colors from "https://deno.land/std@0.179.0/fmt/colors.ts";

let _debug = false;

export function setDebug(debug: boolean): void {
  _debug = debug;
}

function timestamp() : string {
  return colors.white(`${new Date().toLocaleString()}  `);
}

export function info(...args: unknown[]): void {
  console.log(timestamp(), colors.brightWhite(args.join(" ")));
}

export function trace(...args: unknown[]): void {
  console.log(timestamp(), colors.cyan(args.join(" ")));
}

export function error(...args: unknown[]): void {
  console.log(timestamp(), colors.brightRed(args.join(" ")));
}

export function debug(...args: unknown[]): void {
  if (_debug) {
    console.log(timestamp(), colors.green(args.join(" ")));
  }
}

export function subproc(...args: unknown[]): void {
  console.log(timestamp(), colors.yellow(args.join(" ")));
}
