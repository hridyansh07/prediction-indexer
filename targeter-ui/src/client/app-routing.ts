export const targeterCadenceNeeded = (pathname: string) =>
  pathname === '/operations' || pathname.startsWith('/operations/');
