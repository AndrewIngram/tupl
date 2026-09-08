/**
 * A session event buffer wakes one pending consumer per event and retains unread events.
 * Closing prevents late execution notifications while allowing already-buffered events to drain.
 */
export function createSessionEventBuffer<TEvent>() {
  const buffered: TEvent[] = [];
  const waiters: Array<(event: TEvent | null) => void> = [];
  let closed = false;

  const publish = (event: TEvent) => {
    if (closed) {
      return;
    }

    const waiter = waiters.shift();
    if (waiter) {
      waiter(event);
      return;
    }
    buffered.push(event);
  };
  const close = () => {
    if (closed) {
      return;
    }
    closed = true;
    for (const waiter of waiters.splice(0)) {
      waiter(null);
    }
  };
  const take = (): Promise<TEvent | null> => {
    if (buffered.length > 0) {
      return Promise.resolve(buffered.shift() ?? null);
    }
    if (closed) {
      return Promise.resolve(null);
    }
    return new Promise((resolve) => waiters.push(resolve));
  };

  return {
    publish,
    close,
    take,
  };
}
