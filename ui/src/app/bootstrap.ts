export const APPLICATION_NAME = "InsiderTrader" as const;

export type TradingMode = "manual" | "hybrid" | "autonomous";

export interface ApplicationIdentity {
  readonly name: typeof APPLICATION_NAME;
  readonly mode: TradingMode;
}

export function createApplicationIdentity(mode: TradingMode): ApplicationIdentity {
  return Object.freeze({ name: APPLICATION_NAME, mode });
}

