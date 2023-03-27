import { info, trace, error, debug, subproc } from "./log.ts";

export function formatDuration(ms: number): string {
  let s = ms / 1000;
  let d = s / 60 / 60 / 24;
  let h = s / 60 / 60;
  let m = s / 60;
  s = s % 60;
  m = m % 60;
  h = h % 24;
  if (d > 1) { return `${d.toFixed(0)}d ${h.toFixed(0)}h ${m.toFixed(0)}m`; }
  if (h > 1) { return `${h.toFixed(0)}h ${m.toFixed(0)}m`; } 
  if (m > 1) { return `${m.toFixed(0)}m ${s.toFixed(0)}s`; }
  if (s > 1) { return `${s.toFixed(1)}s`; }
  return `${(ms/1000).toFixed(1)}s`;
}

export function truncateOneLine(s: string): string {
  let firstline = s.split('\n')[0];
  let ret = firstline.length > 60 ? firstline.substring(0, 60) : firstline;
  return ret != s ? ret + "..." : ret;
}

export function ensureOnlyKeys(obj: any, validKeys: string[], where: string) {
  let keys = new Set(Object.keys(obj));
  for (let validKey of validKeys) {
    keys.delete(validKey);
  }
  if (keys.size > 0) {
    throw new Error(`Unexpected key(s) ${where}: ${Array.from(keys).join(", ")}`);
  }
}


export async function mergeStreams(stream1: ReadableStream, stream2: ReadableStream, writer: Deno.Writer): Promise<void> {
  const iterator1 = stream1[Symbol.asyncIterator]();
  const iterator2 = stream2[Symbol.asyncIterator]();

  let result1 = await iterator1.next();
  let result2 = await iterator2.next();

  while (!result1.done || !result2.done) {
    if (!result1.done) {
      await writer.write(result1.value);
      result1 = await iterator1.next();
    }
    if (!result2.done) {
      await writer.write(result2.value);
      result2 = await iterator2.next();
    }
  }
}

export async function logSubprocessToConsole(context: string, stdout: ReadableStream, stderr: ReadableStream): Promise<void> {
  const stdoutIter = stdout[Symbol.asyncIterator]();
  const stderrIter = stderr[Symbol.asyncIterator]();

  let result1 = await stdoutIter.next();
  let result2 = await stderrIter.next();

  while (!result1.done || !result2.done) {
    if (!result1.done) {
      subproc(`[${context}] [stdout] ${new TextDecoder().decode(result1.value).trim()}`);
      result1 = await stdoutIter.next();
    }
    if (!result2.done) {
      subproc(`[${context}] [stderr] ${new TextDecoder().decode(result2.value).trim()}`);
      result2 = await stderrIter.next();
    }
  }
}

