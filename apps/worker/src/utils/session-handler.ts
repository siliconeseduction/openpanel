import type { IServiceCreateEventPayload } from '@openpanel/db';
import { sessionsQueue } from '@openpanel/queue';

export const SESSION_TIMEOUT = 1000 * 60 * 30;

const CHANGE_DELAY_THROTTLE_MS = process.env.CHANGE_DELAY_THROTTLE_MS
  ? Number.parseInt(process.env.CHANGE_DELAY_THROTTLE_MS, 10)
  : 60_000; // 1 minute

const CHANGE_DELAY_THROTTLE_MAP = new Map<string, number>();

export async function extendSessionEndJob({
  projectId,
  deviceId,
}: {
  projectId: string;
  deviceId: string;
}) {
  const last = CHANGE_DELAY_THROTTLE_MAP.get(`${projectId}:${deviceId}`) ?? 0;
  const isThrottled = Date.now() - last < CHANGE_DELAY_THROTTLE_MS;

  if (isThrottled) {
    return;
  }

  const jobId = getSessionEndJobId(projectId, deviceId);
  const job = await sessionsQueue.getJob(jobId);

  // Job may already have been promoted out of the delayed state (timeout
  // elapsed and it's running/completed, or a concurrent event beat us to
  // it) between the lookup above and here — changeDelay only works on
  // delayed jobs. Treat that the same as "no job to extend": this is a
  // best-effort bookkeeping call, not something that should fail event
  // ingestion for the underlying event.
  if (!job || !(await job.isDelayed())) {
    return;
  }

  try {
    await job.changeDelay(SESSION_TIMEOUT);
  } catch (error) {
    if (
      error instanceof Error &&
      error.message.includes('is not in the delayed state')
    ) {
      return;
    }
    throw error;
  }
  CHANGE_DELAY_THROTTLE_MAP.set(`${projectId}:${deviceId}`, Date.now());
}

const getSessionEndJobId = (projectId: string, deviceId: string) =>
  `sessionEnd:${projectId}:${deviceId}`;

export function createSessionEndJob({
  payload,
}: {
  payload: IServiceCreateEventPayload;
}) {
  return sessionsQueue.add(
    'session',
    {
      type: 'createSessionEnd',
      payload,
    },
    {
      delay: SESSION_TIMEOUT,
      jobId: getSessionEndJobId(payload.projectId, payload.deviceId),
      attempts: 3,
      backoff: {
        type: 'exponential',
        delay: 200,
      },
    }
  );
}
