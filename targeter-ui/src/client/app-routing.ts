export const targeterSnapshotNeeded = (pathname: string) =>
  pathname === '/operations' || pathname.startsWith('/operations/');
